from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType, DateType, BooleanType, LongType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import requests
from requests import Response
import math
import json
import time
import itertools
from amherst_common.amherst_logger import AmherstLogger

## Helper functions start
##

def sum_columns(numeric_columns_list):  
    sum_expression = '+'.join(numeric_columns_list)
    return F.expr(sum_expression)
    
def check_valid_value(value, valid_values_list, null_allowed):
    return F.when((F.lit(null_allowed)) & (F.isnull(value)), True) \
        .otherwise( \
            F.when((F.lit(null_allowed) == False) & (F.isnull(value)), False) \
            .otherwise(value.isin(valid_values_list))
        )
        
def substring_with_expr(str_value_expr, start_index_expr, num_chars_expr):
    substring_expression = f'substr({str_value_expr}, {start_index_expr}, {num_chars_expr})'
    return F.expr(substring_expression)
    
def instr_with_expr(str_value_expr, values_list):
    contains_expressions = [f'(instr({str_value_expr}, "{value}") > 0)' for value in values_list]
    final_expression = ' or '.join(contains_expressions)
    return F.expr(final_expression)
    
def change_blank_to_null(str_value):
    return F.when(str_value != '', str_value).otherwise(None)
    
def clean_phone_number(str_value_expr):
    replace_expression = f'REPLACE(REPLACE(REPLACE(REPLACE({str_value_expr},"-",""),"(",""),")","")," ","")'
    tmp_col1 = F.expr(replace_expression)
    tmp_col2 = F.substring(F.expr(replace_expression),1,10)
    return F.when(F.substring(str_value_expr,1,3).isin(['000', '111', '999', '123']),F.lit(None))\
        .otherwise(F.when((F.length(tmp_col1) == 10) & (F.lower(tmp_col1).contains('x') == False), tmp_col1)\
        .otherwise(F.when((F.length(tmp_col1) > 10) & (F.lower(tmp_col1).contains('x') == True), tmp_col2)\
        .otherwise(F.lit(None))\
    ))
     
def regexp_like_with_expr(str_value_expr, regex):
    regex_expression = f'(regexp_replace({str_value_expr}, "{regex}", "Dummy") == "Dummy")'
    return F.expr(regex_expression) 
    
##

def validate_listings(df_listings, df_boards, df_states, df_zipcodes, df_property_sub_types):

    ## Apply prefixes to column names before join
    df_listings = df_listings.select(*[F.col(colName).alias('listings_' + colName) for colName in df_listings.columns])
    df_boards = df_boards.filter(F.col('mls').isNotNull()).select(*[F.col(colName).alias('boards_' + colName) for colName in ['mls','movedto']])
    df_states = df_states.select(*[F.col(colName).alias('states_' + colName) for colName in ['state','name']])
    df_zipcodes = df_zipcodes.select(*[F.col(colName).alias('zipcodes_' + colName) for colName in ['zipcode','state']])
    df_property_sub_types = df_property_sub_types.select(*[F.col(colName).alias('property_sub_types_' + colName) for colName in ['property_sub_type']])

    ## Join against reference data sets for business validation
    boards_join_conditions = [df_listings.listings_mls == df_boards.boards_mls]
    states_join_conditions = [(df_listings.listings_state_raw == df_states.states_state) | (df_listings.listings_state_raw == df_states.states_name)]
    zipcodes_join_conditions = [df_listings.listings_state_raw == df_zipcodes.zipcodes_state, df_listings.listings_zip_raw == df_zipcodes.zipcodes_zipcode]
    property_sub_types_join_conditions = [df_listings.listings_property_sub_type == df_property_sub_types.property_sub_types_property_sub_type]

    df_listings = df_listings.join(F.broadcast(df_boards), on = boards_join_conditions, how = 'left')\
        .join(F.broadcast(df_states), on = states_join_conditions, how = 'left')\
        .join(F.broadcast(df_zipcodes), on = zipcodes_join_conditions, how = 'left')\
        .join(F.broadcast(df_property_sub_types), on = property_sub_types_join_conditions, how = 'left')

    ## Update MLS with latest value if required (movedto is the new MLS)
    df_listings = df_listings.withColumn('_listings_mls_new',F.coalesce(F.col('boards_movedto'),F.col('listings_mls')))\
        .withColumn('_mls_change_ind', (F.col('_listings_mls_new') != F.col('listings_mls')).cast(IntegerType()))\
        .withColumn('listings_mls', F.col('_listings_mls_new'))\
        .drop('_listings_mls_new')\
        .dropDuplicates()
    
    '''
    Start validation against business rules
    ''' 
    
    ## Allowed values for certain columns
    rent_sale_allowed_values = ['Sale', 'Rental']
    listing_status_allowed_values = ['A','U','S','X']
    property_type_allowed_values = ['AP','BD','CN','CO','CP','FM','LD','MF','MH','RI','SF','TH','TS']

    ## Apply business rules
    validation_flags = {'_invalid_mls_flag': 'Invalid MLS code',\
        '_mls_null_flag': 'Missing MLS code',\
        '_mls_listing_id_null_flag':  'Missing listing number',\
        '_invalid_rent_sale_flag': 'Invalid rental/sale indicator',\
        '_invalid_listing_status_flag': 'Invalid listing status',\
        '_invalid_property_type_flag': 'Invalid property type',\
        '_invalid_property_sub_type_flag': 'Invalid property subtype',\
        '_invalid_price_flag': 'Invalid price',\
        '_invalid_state_flag': 'Invalid state',\
        '_invalid_zipcode_flag': 'Invalid zip code'\
    }

    df_listings = df_listings.withColumn('_invalid_mls_flag', (F.col('boards_mls').isNull()).cast(IntegerType()))\
        .withColumn('_mls_null_flag', (F.col('listings_mls').isNull()).cast(IntegerType()))\
        .withColumn('_mls_listing_id_null_flag', (F.col('listings_mls_listing_id').isNull()).cast(IntegerType()))\
        .withColumn('_invalid_rent_sale_flag', (check_valid_value(F.col('listings_rent_sale'),rent_sale_allowed_values,False) == False).cast(IntegerType()))\
        .withColumn('_invalid_listing_status_flag', (check_valid_value(F.col('listings_listing_status'),listing_status_allowed_values,False) == False).cast(IntegerType()))\
        .withColumn('_invalid_property_type_flag', (check_valid_value(F.col('listings_property_type'),property_type_allowed_values,False) == False).cast(IntegerType()))\
        .withColumn('_invalid_property_sub_type_flag', (F.col('property_sub_types_property_sub_type').isNull()).cast(IntegerType()))\
        .withColumn('_invalid_price_flag', F.when((F.coalesce('listings_current_price',F.lit(0)) < 1) & (F.col('listings_listing_status') == 'S') & (F.col('listings_closed_price').isNull()), F.lit(1))\
            .otherwise(F.when((F.coalesce('listings_current_price',F.lit(0)) < 1) & (F.col('listings_listing_status') != 'S'),F.lit(1))\
            .otherwise(F.lit(0))\
        ))\
        .withColumn('_invalid_state_flag', (F.col('states_state').isNull()).cast(IntegerType()))\
        .withColumn('_invalid_zipcode_flag', (F.col('zipcodes_zipcode').isNull()).cast(IntegerType()))\
        .withColumn('_reject_flags', sum_columns(validation_flags.keys()))\
        .withColumn('_reject_reasons', F.concat_ws('|',*[(F.when(F.col(flag) == 1, F.lit(validation_flags[flag]))) for flag in validation_flags.keys()]))

    ## Keep "listings_*", "states_state" (we do need it later), "_reject_flags", and "_reject_reasons" columns only
    ## Remove "listings_" prefix from the ones who have it
    columns_to_keep = [colName for colName in df_listings.columns if (colName.startswith('listings_') or colName in ['states_state','_reject_flags','_reject_reasons'])]
    df_listings = df_listings.select(*columns_to_keep)
    df_listings = df_listings.toDF(*[colName.replace('listings_','') for colName in df_listings.columns])

    ## Keep records with 0 reject flags only
    df_listings_rejected = df_listings.filter(F.col('_reject_flags') > 0)
    df_listings_good = df_listings.filter(F.col('_reject_flags') == 0)
    
    return (df_listings_good, df_listings_rejected)
    
def transform_listings(df_listings_good, df_counties, df_geo_ids, target_schema, spark):

    '''
    SIMPLE TRANSFORMATIONS START
    ''' 

    ## Trim all strings
    ## Convert invalid dates to null: fix added because dates set to 1800 in Corelogic were causing df_listings_good.rdd to crash
    ## Convert invalid timestamps to null: same as above
    string_columns = [item[0] for item in df_listings_good.dtypes if item[1] == 'string']
    date_columns = [item[0] for item in df_listings_good.dtypes if item[1] == 'date']
    timestamp_columns = [item[0] for item in df_listings_good.dtypes if item[1] == 'timestamp']
    
    df_listings_good = df_listings_good.select(*[
        F.trim(F.col(colName)).alias(colName) if colName in string_columns
        else (F.when(F.year(F.col(colName).cast(DateType())) >= 1900,  F.col(colName)).otherwise(F.lit(None).cast(DateType()))).alias(colName) if colName in date_columns
        else (F.when(F.year(F.col(colName).cast(DateType())) >= 1900,  F.col(colName)).otherwise(F.lit(None).cast(TimestampType()))).alias(colName) if colName in timestamp_columns
        else colName
        for colName in df_listings_good.columns]
        )

    ## Custom transformations start
    ## (All columns with "_" prefix will later replace the original ones) 
    df_listings_good = df_listings_good.withColumn('_unit_type', F.substring('unit_type',1,10))\
        .withColumn('_tmp_unit1', F.when((F.col('unit').isNotNull()) & (F.regexp_replace('unit', '0', '') != ''), F.substring('unit',1,10)).otherwise(F.lit(None)))\
        .withColumn('_tmp_unit2', F.expr('right(street_address_raw, length(street_address_raw)/2)'))\
        .withColumn('_tmp_unit3', F.regexp_extract('_tmp_unit2','(.*)#(.*)',2))\
        .withColumn('_unit', change_blank_to_null(F.coalesce('_tmp_unit1','_tmp_unit3')))\
        .withColumn('_tmp_subdivision1', F.upper(F.col('subdivision')))\
        .withColumn('_subdivision', F.when(F.col('_tmp_subdivision1').contains('NOT IN A SUBDIVISION'),F.lit(None))\
            .otherwise(F.when(F.col('_tmp_subdivision1').contains('NONE'),F.lit(None))\
            .otherwise(F.when(regexp_like_with_expr('_tmp_subdivision1','^(UNK)(.*)'),F.lit(None))\
            .otherwise(F.when(F.col('_tmp_subdivision1').isin(['NA','N/A','?']),F.lit(None))\
            .otherwise(F.when(F.col('subdivision').cast(IntegerType()).isNotNull(),F.lit(None))\
            .otherwise(F.when(F.length('subdivision') <= 2,F.lit(None))\
            .otherwise(F.substring('subdivision',1,100))\
        ))))))\
        .withColumn('_lot', F.when((F.col('lot').isNotNull()) & (F.regexp_replace('lot', '0', '') != ''), F.substring('lot',1,15)).otherwise(F.lit(None)))\
        .withColumn('_block', F.substring('block',1,15))\
        .withColumn('_legal_tract', F.substring('legal_tract',1,10))\
        .withColumn('_book', F.substring('book',1,25))\
        .withColumn('_section', F.when(regexp_like_with_expr('section','[0-3][0-9]'),F.col('section')).otherwise(F.lit(None)))\
        .withColumn('_tmp_township1', F.upper(F.col('township')))\
        .withColumn('_township', F.when(regexp_like_with_expr('_tmp_township1','[0-9][0-9][N,S,E,W]'),F.col('township'))\
            .otherwise(F.when(regexp_like_with_expr('_tmp_township1','[0-9][0-9].[N,S,E,W]'),F.concat(F.substring('township',1,2),F.substring('township',4,1)))\
            .otherwise(F.when(regexp_like_with_expr('_tmp_township1','T[0-9][0-9][N,S,E,W]'),F.substring('township',2,3))\
            .otherwise(F.lit(None))\
        )))\
        .withColumn('_tmp_range1', F.upper(F.col('range')))\
        .withColumn('_range', F.when(regexp_like_with_expr('_tmp_range1','[0-9][0-9][N,S,E,W]'),F.col('range'))\
            .otherwise(F.when(regexp_like_with_expr('_tmp_range1','[0-9][0-9].[N,S,E,W]'),F.concat(F.substring('range',1,2),F.substring('range',4,1)))\
            .otherwise(F.when(regexp_like_with_expr('_tmp_range1','T[0-9][0-9][N,S,E,W]'),F.substring('range',2,3))\
            .otherwise(F.lit(None))\
        )))\
        .withColumn('_apn', F.substring(F.expr("REPLACE(REPLACE(apn,'-', ''),' ','')"),1,100))\
        .withColumn('_school_district', F.when(F.col('school_district').cast(IntegerType()).isNull(),F.substring(F.col('school_district'),1,125))\
            .otherwise(F.lit(None))\
        )\
        .withColumn('_property_sub_type', F.substring(change_blank_to_null(F.upper(F.col('property_sub_type'))),1,20))\
        .withColumn('_property_description', F.substring('property_description',1,500))\
        .withColumn('_lot_size_acres', F.when((F.col('lot_size_acres').isNotNull()) & (F.col('lot_size_acres') > 0), F.col('lot_size_acres'))\
            .otherwise(F.when((F.col('lot_size_acres').isNull()) & (F.col('lot_size_sq_ft').isNotNull()) & (F.col('lot_size_sq_ft') > 0) & (F.col('lot_size_sq_ft') <= 50), F.col('lot_size_sq_ft').cast(DecimalType(16, 4)))\
            .otherwise(F.when((F.col('lot_size_acres').isNull()) & (F.col('lot_size_sq_ft').isNotNull()) & (F.col('lot_size_sq_ft') > 50), (F.round(F.col('lot_size_sq_ft'),0)/43560).cast(DecimalType(16, 4)))\
            .otherwise(F.lit(None))\
        )))\
        .withColumn('_lot_size_acres', F.when((F.col('_lot_size_acres').isNotNull()) & (F.col('_lot_size_acres') <= 1000000), F.col('_lot_size_acres').cast(DecimalType(16, 4))))\
        .withColumn('_lot_size_sq_ft', F.when((F.col('lot_size_sq_ft').isNotNull()) & (F.col('lot_size_sq_ft') > 0), F.round(F.col('lot_size_sq_ft'),0))\
            .otherwise(F.when((F.col('lot_size_sq_ft').isNull()) & (F.col('lot_size_acres').isNotNull()) & (F.col('lot_size_acres') > 0) & (F.col('lot_size_acres') < 500), (F.round(F.col('lot_size_acres')*43560,0)).cast(DecimalType(16, 4)))\
            .otherwise(F.lit(None))\
        ))\
        .withColumn('_lot_size_sq_ft', F.when((F.col('_lot_size_sq_ft').isNotNull()) & (F.col('_lot_size_sq_ft') <= 2147483647), F.col('_lot_size_sq_ft').cast(DecimalType(16, 4))))\
        .withColumn('_zoning', F.substring('zoning',1,250))\
        .withColumn('_restrictions', F.substring('restrictions',1,250))\
        .withColumn('_easements', F.substring('easements',1,250))\
        .withColumn('_tmp_water_source1', F.upper(F.col('water_source')))\
        .withColumn('_water_source', F.when(instr_with_expr('_tmp_water_source1',['CITY','COUNTY','TOWN','MUNICIPAL','PUBLIC']),F.lit('Municipal'))\
            .otherwise(F.when(regexp_like_with_expr('_tmp_water_source1','MUN.*'),F.lit('Municipal'))\
            .otherwise(F.when(instr_with_expr('_tmp_water_source1',['WATER DISTRICT','UTILITY DISTRICT','MUD','HCUD']),F.lit('Utility District'))\
            .otherwise(F.when(instr_with_expr('_tmp_water_source1',['COMM','CENTRAL']),F.lit('Community'))\
            .otherwise(F.when(instr_with_expr('_tmp_water_source1',['COOPERATIVE','CO-OP']),F.lit('Co-op'))\
            .otherwise(F.when(F.col('_tmp_water_source1').contains('WELL'),F.lit('Well'))\
            .otherwise(F.when(F.col('_tmp_water_source1').contains('SPRING'),F.lit('Spring'))\
            .otherwise(F.when(instr_with_expr('_tmp_water_source1',['CISTERN','RAINWATER']),F.lit('Cistern'))\
            .otherwise(F.when(instr_with_expr('_tmp_water_source1',['PRIVATE','PVT','WATER COMPANY']),F.lit('Private'))\
            .otherwise(F.when(F.col('_tmp_water_source1').contains('NONE'),F.lit('None'))\
            .otherwise(F.when(regexp_like_with_expr('_tmp_water_source1','NO.*'),F.lit('None'))\
            .otherwise(F.lit(None))\
        )))))))))))\
        .withColumn('_tmp_septic_sewer1', F.upper(F.col('septic_sewer')))\
        .withColumn('_septic_sewer', F.when(instr_with_expr('_tmp_septic_sewer1',['SEPTI', 'LEACH', 'FIELD', 'LAGOON', 'MOUND', 'AEROBIC', 'CESSPOOL', 'HOLDING TANK']),F.lit('Septic'))\
            .otherwise(F.when(instr_with_expr('_tmp_septic_sewer1',['SEWER', 'SWR', 'PUB', 'CITY', 'SANITARY', 'PEP']),F.lit('Sewer'))\
            .otherwise(F.lit(None))\
        ))\
        .withColumn('_sfha', F.when(F.upper(F.col('sfha')).isin(['Y','YES','IN']),F.lit('Y'))\
            .otherwise(F.when(F.upper(F.col('sfha')).isin(['N','NO','OUT']),F.lit('N'))\
            .otherwise(F.lit(None))\
        ))\
        .withColumn('_gated_community', F.when(F.upper(F.col('gated_community')).isin(['Y','N']),F.col('gated_community')))\
        .withColumn('_hoa', F.when(F.upper(F.col('hoa')).isin(['Y','YES','TRUE','T','MANDATORY']),F.lit('Y'))\
            .otherwise(F.when(F.upper(F.col('hoa')).isin(['N','NO','FALSE','F','VOLUNTARY']),F.lit('N'))\
            .otherwise(F.when(F.upper(F.col('hoa_name')).isin(['VOLUNTARY']),F.lit('N'))\
            .otherwise(F.lit(None))\
        )))\
        .withColumn('_hoa_name', F.when(F.upper(F.col('hoa_name')) == 'VOLUNTARY',F.lit(None))\
            .otherwise(F.substring('hoa_name',1,150))\
        )\
        .withColumn('_hoa_management_co', F.substring('hoa_management_co',1,250))\
        .withColumn('_hoa_management_co_phone', clean_phone_number('hoa_management_co_phone'))\
        .withColumn('_occupant_type', F.substring('occupant_type',1,25))\
        .withColumn('_ownership_type', F.substring('ownership_type',1,20))\
        .withColumn('_owner_type', F.substring('owner_type',1,25))\
        .withColumn('_owner_name', F.substring('owner_name',1,255))\
        .withColumn('_owner_phone', clean_phone_number('owner_phone'))\
        .withColumn('_year_built', F.when(F.col('year_built').between(1600,F.year(F.current_date())+1),F.col('year_built')))\
        .withColumn('_year_updated', F.when(F.col('year_updated').between(1600,F.year(F.current_date())+1),F.col('year_updated')))\
        .withColumn('_number_of_units', F.when(F.upper(F.col('property_type')) == 'SF', F.lit(1))\
            .otherwise(F.when((F.col('number_of_units').isNotNull()) & (F.col('number_of_units') > 0),F.col('number_of_units'))\
            .otherwise(F.lit(None))\
        ))\
        .withColumn('_living_area_sq_ft', F.when((F.round('living_area_sq_ft',0) > 0) & (F.round('living_area_sq_ft',0) <= 2147483647), F.round('living_area_sq_ft',0).cast(DecimalType(16, 4))))\
        .withColumn('_living_area_sq_ft_source', F.substring('living_area_sq_ft_source',1,25))\
        .withColumn('_building_style', F.substring('building_style',1,100))\
        .withColumn('_stories', F.when(F.col('stories').between(0, 99),F.col('stories')))\
        .withColumn('_beds', F.when(F.col('beds') >= 0,F.round(F.col('beds'),0)))\
        .withColumn('_full_baths', F.when(F.col('full_baths') >= 0,F.col('full_baths')))\
        .withColumn('_half_baths', F.when(F.col('half_baths') >= 0,F.col('half_baths')))\
        .withColumn('_basement', F.when(F.upper(F.col('basement')).isin(['Y','TRUE','T']),F.lit('Y'))\
            .otherwise(F.when(F.upper(F.col('basement')).isin(['N','FALSE','F']),F.lit('N'))\
            .otherwise(F.when(F.col('finished_basement_pct') > 0,F.lit('Y'))\
            .otherwise(F.lit(None))\
        )))\
        .withColumn('_finished_basement_pct', F.when(F.col('finished_basement_pct') <= 100,F.col('finished_basement_pct')))\
        .withColumn('_garage_type', F.when(F.upper(F.col('garage_type')).isin(['G','C','N']),F.col('garage_type')))\
        .withColumn('_garage_style', F.substring('garage_style',1,100))\
        .withColumn('_garage_spaces', F.when(F.round('garage_spaces',0) <= 2147483647, F.round('garage_spaces',0)))\
        .withColumn('_roof_type', F.substring('roof_type',1,255))\
        .withColumn('_exterior_material', F.substring('exterior_material',1,255))\
        .withColumn('_foundation', F.substring('foundation',1,255))\
        .withColumn('_pool', F.substring('pool',1,100))\
        .withColumn('_condition', F.substring('condition',1,250))\
        .withColumn('_property_tax_year', F.when((F.col('property_tax_year') > 2000) & (F.col('property_tax_year') <= F.year(F.current_date())+1),F.col('property_tax_year')))\
        .withColumn('_hoa_dues_frequency', F.when(F.col('hoa_dues_frequency').isin([0,1,2,4,12,52]),F.col('hoa_dues_frequency')))\
        .withColumn('_hoa_dues_description', F.substring('hoa_dues_description',1,750))\
        .withColumn('_rent_sale', F.when(F.col('rent_sale').isin(['Sale','Rental']),F.col('rent_sale'))\
            .otherwise(F.lit('Sale'))
        )\
        .withColumn('_entry_date', F.col('entry_date').cast(DateType()))\
        .withColumn('_listing_date', F.col('listing_date').cast(DateType()))\
        .withColumn('_status_date', F.col('status_date').cast(DateType()))\
        .withColumn('_current_price', F.when((F.coalesce('current_price',F.lit(0)) < 1) & (F.col('listing_status') == 'S'),F.col('closed_price').cast(DecimalType(16,4)))\
            .otherwise(F.when((F.coalesce('current_price',F.lit(0)) < 1) & (F.col('listing_status') != 'S'),F.lit(None).cast(DecimalType(16,4)))\
            .otherwise(F.col('current_price').cast(DecimalType(16,4)))\
        ))\
        .withColumn('_current_price_as_of_date', F.col('current_price_as_of_date').cast(DateType()))\
        .withColumn('_orig_price', F.when((F.col('orig_price') >= 0),F.col('orig_price')))\
        .withColumn('_orig_listing_date', F.col('orig_listing_date').cast(DateType()))\
        .withColumn('_contract_date', F.col('contract_date').cast(DateType()))\
        .withColumn('_closed_price', F.when((F.col('closed_price') >= 0),F.col('closed_price')))\
        .withColumn('_closed_date', F.when((F.col('closed_date').cast(DateType()) <= F.date_add(F.current_date(),180)), F.col('closed_date').cast(DateType())))\
        .withColumn('_days_on_market', F.when((F.col('days_on_market') >= 0),F.col('days_on_market')))\
        .withColumn('_cumulative_days_on_market', F.when((F.col('cumulative_days_on_market').isNotNull()) & (F.col('cumulative_days_on_market') >= 0) & (F.col('cumulative_days_on_market') >= F.coalesce(F.col('days_on_market'),F.lit(0))),F.col('cumulative_days_on_market'))\
            .otherwise(F.lit(None))\
        )\
        .withColumn('_sale_circumstances', F.when(F.upper(F.col('sale_circumstances')).isin(['NONE','NOT APPLICABLE']) == False,F.col('sale_circumstances')))\
        .withColumn('_listing_conditions', F.when(F.upper(F.col('listing_conditions')).isin(['NONE','NOT APPLICABLE']) == False,F.col('listing_conditions')))\
        .withColumn('_listing_url', F.substring('listing_url',1,250))\
        .withColumn('_listing_image_url', F.substring('listing_image_url',1,250))\
        .withColumn('_listing_image_url_date', F.col('listing_image_url_date').cast(DateType()))\
        .withColumn('_listing_broker_name', change_blank_to_null(F.substring('listing_broker_name',1,150)))\
        .withColumn('_listing_broker_id', F.substring('listing_broker_id',1,20))\
        .withColumn('_listing_agent_name', F.substring('listing_agent_name',1,150))\
        .withColumn('_listing_agent_id', F.substring('listing_agent_id',1,20))\
        .withColumn('_listing_agent_phone', clean_phone_number('listing_agent_phone'))\
        .withColumn('_listing_agent_email', F.when(F.col('listing_agent_email').contains('@'),F.col('listing_agent_email')))\
        .withColumn('_brokerage_name', F.substring('brokerage_name',1,100))\
        .withColumn('_brokerage_phone', clean_phone_number('brokerage_phone'))\
        .withColumn('_selling_agent_name', change_blank_to_null(F.substring('selling_agent_name',1,150)))\
        .withColumn('_selling_agent_id', F.substring('selling_agent_id',1,20))\
        .withColumn('_commissions', change_blank_to_null(F.substring('commissions',1,50)))\
        .withColumn('_buyer_agent_name', F.substring('buyer_agent_name',1,150))\
        .withColumn('_buyer_agent_id', F.substring('buyer_agent_id',1,20))\
        .withColumn('_street_address_raw', F.upper(F.substring(F.expr("REPLACE(REPLACE(REPLACE(street_address_raw,' ', '<>'), '><', ''), '<>', ' ')"),1,100)))\
        .withColumn('_city_raw', F.substring(F.expr("REPLACE(REPLACE(REPLACE(city_raw,' ', '<>'), '><', ''), '<>', ' ')"),1,100))\
        .withColumn('_state_raw', F.substring('state_raw',1,50))\
        .withColumn('_zip_raw', F.substring('zip_raw',1,20))\
        .withColumn('_source_listing_id', F.substring('source_listing_id',1,50))
        
    '''
    EXTRA COLUMNS START
    ''' 
    
    ## Add extra columns that do not require joins
    ## zip and zip_raw being the same thing is not a mistake, we do need both columns
    df_listings_good = df_listings_good.select(df_listings_good['*'],\
        F.when(F.upper(F.col('_city_raw')).isin(['UNINCORPORATED','OTHER CITY','HTTP']) == False, F.upper(F.col('_city_raw'))).alias('city'),\
        F.upper(F.col('_street_address_raw')).alias('street_address'),\
        F.col('zip_raw').alias('zip'),\
        F.col('states_state').alias('state'),\
        F.current_timestamp().alias('create_timestamp'),\
        F.current_timestamp().alias('update_timestamp'),\
        ## Note: these columns are placeholders for mls.listings only 
        F.lit(None).cast(LongType()).alias('asg_primary_id'),\
        F.lit(None).cast(BooleanType()).alias('asg_primary_id_final_flag'),\
        F.lit(None).cast(StringType()).alias('asg_primary_id_source'),\
        F.lit(None).cast(TimestampType()).alias('asg_primary_id_source_queried_timestamp'),\
        F.lit(None).cast(BooleanType()).alias('asg_primary_id_source_responded_flag'),\
        F.lit(None).cast(StringType()).alias('asg_primary_id_issue_text'),\
        F.lit(None).cast(BooleanType()).alias('asg_primary_id_mssql_fixed_flag'),\
        F.lit(None).cast(BooleanType()).alias('asg_primary_id_updated_flag'),\
        F.lit(None).cast(TimestampType()).alias('asg_primary_id_updated_timestamp'),\
        F.lit(None).cast(LongType()).alias('asg_primary_id_previous_value'),\
        F.lit(None).cast(StringType()).alias('asg_primary_id_load_status'),\
        ## Note: these columns are placeholders for mls.listings_history only
        F.lit(None).cast(StringType()).alias('listing_status_old'),\
        F.lit(None).cast(DecimalType(16,4)).alias('current_price_old'),\
        F.lit(None).cast(StringType()).alias('source_listing_id_old'),\
        F.lit(None).cast(StringType()).alias('street_address_raw_old'),\
        F.lit(None).cast(StringType()).alias('property_type_old'),\
        F.lit(None).cast(StringType()).alias('property_sub_type_old'),\
        F.lit(None).cast(BooleanType()).alias('listing_status_changed_flag'),\
        F.lit(None).cast(BooleanType()).alias('current_price_changed_flag'),\
        F.lit(None).cast(BooleanType()).alias('source_listing_id_changed_flag'),\
        F.lit(None).cast(BooleanType()).alias('street_address_raw_changed_flag'),\
        F.lit(None).cast(BooleanType()).alias('property_type_changed_flag'),\
        F.lit(None).cast(BooleanType()).alias('property_sub_type_changed_flag')\
    )    
        
    '''
    TRANSFORMATIONS THAT REQUIRE JOINS START
    ''' 

    ## Apply prefixes to column names
    df_listings_good = df_listings_good.select(*[F.col(colName).alias('listings_' + colName) for colName in df_listings_good.columns])
    df_counties1 = df_counties.select(*[F.col(colName).alias('counties1_' + colName) for colName in ['fips','state','basename']])
    df_counties2 = df_counties.select(*[F.col(colName).alias('counties2_' + colName) for colName in ['fips','state','basename']])
    df_geo_ids1 = df_geo_ids.select(*[F.col(colName).alias('geo_ids1_' + colName) for colName in ['fips','censustract','censustractgeoid','censustractname']])
    df_geo_ids2 = df_geo_ids.select(*[F.col(colName).alias('geo_ids2_' + colName) for colName in ['fips','censustract','censustractgeoid','censustractname']])
    df_geo_ids3 = df_geo_ids.select(*[F.col(colName).alias('geo_ids3_' + colName) for colName in ['fips','censustract','censustractgeoid','censustractname']])

    ## Counties join conditions
    counties1_join_conditions = [(df_listings_good.listings_fips == df_counties1.counties1_fips)]
    counties2_join_conditions = [(df_listings_good.listings_state == df_counties2.counties2_state) & (df_listings_good.listings_county_name == df_counties2.counties2_basename)]
    ## Apply joins and add calculated columns
    df_listings_good = df_listings_good.join(F.broadcast(df_counties1), on = counties1_join_conditions, how = 'left')\
        .join(F.broadcast(df_counties2), on = counties2_join_conditions, how = 'left')\
        .withColumn('_fips', F.coalesce('counties1_fips','counties2_fips'))\
        .withColumn('_county_name', F.coalesce(F.substring('listings_county_name',1,50), 'counties1_basename'))
                
    ## Geo ids join conditions
    geo_ids1_join_conditions = [(df_listings_good.listings_census_tract_geo_id == df_geo_ids1.geo_ids1_censustract) & (df_listings_good._fips == df_geo_ids1.geo_ids1_fips)]
    geo_ids2_join_conditions = [(df_listings_good.listings_census_tract_geo_id == df_geo_ids2.geo_ids2_censustractname) & (df_listings_good._fips == df_geo_ids2.geo_ids2_fips)]
    geo_ids3_join_conditions = [(df_listings_good.listings_census_tract_geo_id == df_geo_ids3.geo_ids3_censustractgeoid)]
    ## Apply joins and add calculated columns
    df_listings_good = df_listings_good.join(F.broadcast(df_geo_ids1), on = geo_ids1_join_conditions, how = 'left')\
        .join(F.broadcast(df_geo_ids2), on = geo_ids2_join_conditions, how = 'left')\
        .join(F.broadcast(df_geo_ids3), on = geo_ids3_join_conditions, how = 'left')\
        .withColumn('_census_tract_geo_id', F.coalesce('geo_ids1_censustractgeoid','geo_ids2_censustractgeoid','geo_ids3_censustractgeoid'))
    
    ## Keep "listings_*", "_fips", "_county_name", "_census_tract_geo_id" columns only
    ## Remove "listings_" prefix from the ones who have it
    ## Drop duplicates
    columns_to_keep = [colName for colName in df_listings_good.columns if (colName.startswith('listings_') or colName in ['_fips', '_county_name', '_census_tract_geo_id'])]
    df_listings_good = df_listings_good.select(*columns_to_keep)
    df_listings_good = df_listings_good.toDF(*[colName.replace('listings_','') for colName in df_listings_good.columns])\
        .dropDuplicates()
     
    '''
    COLUMN CLEANUP START
    '''    

    ## Identify columns that we need to replace with its transformed version _colName->colName
    columns_to_replace = {colName.replace('_','',1):colName for colName in df_listings_good.columns if ((colName.startswith('_')) and (colName.startswith('_tmp_') == False) and (colName.replace('_','',1) in df_listings_good.columns))}

    ## Replace those columns with its transformed version _colName
    df_listings_good = df_listings_good.select(*[
        (F.col(columns_to_replace[colName])).alias(colName) if colName in columns_to_replace.keys()
        else colName
        for colName in df_listings_good.columns if colName not in columns_to_replace.values()])
        
    ## Select the final list of columns in the order desired
    df_listings_good = df_listings_good.select(*target_schema.fieldNames())
    
    ## Apply the full target schema just to be sure that we are good
    ## df_listings_new = spark.createDataFrame(df_listings_good.rdd, schema = target_schema)

    return df_listings_good
    
def call_api(iter, expand, logger, property_id_api_endpoint, property_id_api_sleep):
    
    logger.info('One API call')
   
    headers = {'Content-Type': 'application/json', 'Accept': '*/*'}

    ## Create two copies of the iterable, we may need the 2nd one later
    iter1, iter2 = itertools.tee(iter, 2)

    ## Build a list of dictionaries on top of the iterable. Each dictionary represents a listing. 
    ## Then convert the list of dicts to a JSON string. This is what we need for the API request body.
    api_request_body = json.dumps([row.asDict() for row in iter1]) 
    
    ## Call the API here with the request body we built above
    ## Sleep added to avoid Connection reset error, see:
    ## https://stackoverflow.com/questions/52051989/requests-exceptions-connectionerror-connection-aborted-connectionreseterro
    time.sleep(property_id_api_sleep)
    response = None
    try:
        response = requests.post(url=property_id_api_endpoint, data=api_request_body, headers=headers)
    except Exception as e:   
        response = repr(e)
     
    if isinstance(response,Response) and response.status_code == 200:
    
        response_data = json.loads(response.text)
        responses = response_data['Results'] ## a list of dicts, where each dict is a listing
        ## Keep only the portion of the response that we care about ('ReferenceID','asgPropID','AddedToPmDate','IsProvisional','LastValidatedDate')
        ## Also add IssueText as None, since there weren't any issues with this particular request
        responses = [{key:response[key] if key in response else None for key in ('ReferenceID','asgPropID','AddedToPmDate','IsProvisional','LastValidatedDate','IssueText')} for response in responses]
        
        if expand:
            ## Send back one row per listing
            for response in responses:
                yield Row(**response)
        else:
            ## Send back one single row
            yield Row(Desc='<<<This is one partition>>>', ResponseStatus=response.status_code, ResponseText=responses, IssueText=None) 
                       
    else:
        logger.info(f'Issue calling PropertyMaster API')
        
        if isinstance(response,Response) and response.status_code != 200:
            ## This may be: 
                ## > status_code 400, in case of empty partition/empty body being passed to the API: we have seen this happen
                ## > any other status_code probably signals an actual error
            logger.info(f'API response text: {response.text}, response status: {response.status_code}')
            response_status = response.status_code
            issue_text = response.text
        else:
            ## Other unforeseen exceptions such as "Connection reset by peer"
            logger.info(f'API call exception: {response}')
            response_status = None
            issue_text = response 

        ## Send back result 
        if expand:
            ## Send back one row per listing, using the 2nd copy of the iterable
            for row in iter2:
                yield Row(ReferenceID=row['ReferenceID'], asgPropID=None, AddedToPmDate=None, IsProvisional=None, LastValidatedDate=None, IssueText=issue_text)            
        else:
            ## Send back one single row
            yield Row(Desc='<<<This is one partition>>>', ResponseStatus=response_status, ResponseText=None, IssueText=issue_text)     
    
def get_listing_property_ids_from_api(df_listings, property_id_api_endpoint, property_id_api_batch_size, property_id_api_sleep, logger, debug, spark):

    df_listings_with_prop_id = None

    ## Calculate set of listings for which we will attempt to find a property id
    window1 = Window.partitionBy(['StreetAddress','City','StateAbbr','Zip5','Unit'])
    
    df_listings_for_lkp = df_listings.select(\
        F.col('mls').alias('lkp_mls'),\
        F.col('mls_listing_id').alias('lkp_mls_listing_id'),\
        F.col('street_address').alias('StreetAddress'),\
        F.col('city').alias('City'),\
        F.col('state').alias('StateAbbr'),\
        F.col('zip').alias('Zip5'),\
        F.col('unit').alias('Unit'),\
        F.monotonically_increasing_id().alias('UniqueID')\
    ).withColumn('ReferenceID', F.max('UniqueID').over(window1))
    
    ## Calculate reduced dataset with distinct addresses only 
    df_listings_for_rdd = df_listings_for_lkp.select('StreetAddress','City','StateAbbr','Zip5','Unit','ReferenceID').distinct() 

    ## Calculate number of partitions needed
    estimated_count = df_listings_for_rdd.rdd.countApprox(1000, 0.95)
    num_partitions = math.ceil(estimated_count/property_id_api_batch_size)
    
    if debug:
        logger.info(f'Number of distinct addresses for property id lookup (estimate): {estimated_count}')
        logger.info(f'API batch size: {property_id_api_batch_size}')
        logger.info(f'Number of partitions needed: {num_partitions}') 
        cores = spark.conf.get('spark.executor.cores')
        max_executors = spark.conf.get('spark.dynamicAllocation.maxExecutors')
        logger.info(f'Cores: {cores} Max executors: {max_executors}')
    
    if df_listings_for_rdd.rdd.isEmpty() == False:
    
        ## Prepare the rdd required by the API
        ## Use repartition() to create partitions of roughly the same size before calling the API
        listings_for_api_rdd = df_listings_for_rdd.rdd.repartition(num_partitions)
                     
        ## Call the API once per partition, with expand = True, 
        ## which means I want my result set to be 1 record per listing
        api_result_rdd = None
        try:
            api_result_rdd = listings_for_api_rdd.mapPartitions(lambda x: call_api(x, True, logger, property_id_api_endpoint, property_id_api_sleep)) 
        except Exception as e:
            logger.error('mapPartitions() error')
            logger.error(e)         
        
        if api_result_rdd and api_result_rdd.isEmpty() == False: 
        
            ## Convert this to a regular df ('ReferenceID','asgPropID','AddedToPmDate','IsProvisional','LastValidatedDate','IssueText')
            target_schema = StructType([StructField('ReferenceID', LongType(), True),\
                StructField('asgPropID', LongType(), True),\
                StructField('AddedToPmDate', StringType(), True),\
                StructField('IsProvisional', BooleanType(), True),\
                StructField('LastValidatedDate', StringType(), True),\
                StructField('IssueText', StringType(), True)])

            ## Note: the 3rd condition of the CASE statement is not a bug.
            ## Documentation here: https://wiki.amherst.com/display/TP/MLS+Listings#MLSListings-PropertyIdlookup
            df_api_results = spark.createDataFrame(api_result_rdd, schema = target_schema)\
                .select(F.col('ReferenceID').alias('api_ReferenceID'), \
                    F.col('asgPropID').alias('api_asg_primary_id'),\
                    F.expr('\
                        CASE \
                            WHEN asgPropID IS NULL THEN CAST(NULL AS boolean) \
                            WHEN asgPropID IS NOT NULL AND IsProvisional = false AND LastValidatedDate IS NOT NULL THEN true \
                            WHEN asgPropID IS NOT NULL AND AddedToPmDate IS NULL THEN true \
                            ELSE false \
                        END').cast(BooleanType()).alias('api_asg_primary_id_final_flag'),\
                    F.col('IssueText').alias('api_IssueText')\
                )
            
            ## Join on ReferenceID, to recover the mls and mls_listing_id
            join_conditions = [(df_listings_for_lkp.ReferenceID == df_api_results.api_ReferenceID)]
            
            df_listings_with_prop_id = df_listings_for_lkp.join(df_api_results, on = join_conditions, how = 'left')\
                .select('lkp_mls','lkp_mls_listing_id',\
                    F.col('api_asg_primary_id').alias('lkp_asg_primary_id'),\
                    F.col('api_asg_primary_id_final_flag').alias('lkp_asg_primary_id_final_flag'),\
                    F.lit('API').alias('lkp_asg_primary_id_source'),\
                    F.when(F.col('api_IssueText').isNull(), F.lit(True)).otherwise(F.lit(False)).alias('lkp_asg_primary_id_source_responded_flag'),\
                    F.col('api_IssueText').alias('lkp_asg_primary_id_issue_text'),\
                    F.lit(None).cast(BooleanType()).alias('lkp_asg_primary_id_mssql_fixed_flag')\
                )
        
        else:
            logger.info(f'Empty result set received from PropertyMaster API')     
        
    return df_listings_with_prop_id

def get_listing_property_ids_from_mssql(df_listings, df_listings_mssql, df_property_master_mssql, logger, debug, spark):

    df_listings_with_prop_id = None

    ## Calculate set of listings for which we will attempt to find a property id
    df_listings_for_lkp = df_listings.select(F.col('mls').alias('lkp_mls'), F.col('mls_listing_id').alias('lkp_mls_listing_id'))

    if debug:
        estimated_count = df_listings_for_lkp.rdd.countApprox(1000, 0.95)
        logger.info(f'Number of listings for property id lookup (estimate): {estimated_count}')    
    
    if df_listings_for_lkp.rdd.isEmpty() == False:
        
        ## Join #1
        
        df_listings_mssql = df_listings_mssql.select('mls','mlslistingid','ailpropertyid')     
        
        join_conditions = [(df_listings_for_lkp.lkp_mls == df_listings_mssql.mls) & \
            (df_listings_for_lkp.lkp_mls_listing_id == df_listings_mssql.mlslistingid)]
        df_listings_with_prop_id = df_listings_for_lkp\
            .join(df_listings_mssql, on = join_conditions, how = 'left')\
            .select('lkp_mls','lkp_mls_listing_id',\
            F.when(F.col('mls').isNull(), F.lit(False)).otherwise(F.lit(True)).alias('lkp_source_responded'),\
            F.col('ailpropertyid').alias('lkp_ailpropertyid'))
        
        ## Join #2
        
        df_property_master_mssql = df_property_master_mssql.select('asgpropid','asgprimaryid','isprovisional','lastvalidated','asgtimestamp')

        join_conditions = [(df_listings_with_prop_id.lkp_ailpropertyid == df_property_master_mssql.asgpropid)]       
        df_listings_with_prop_id = df_listings_with_prop_id\
            .join(df_property_master_mssql, on = join_conditions, how = 'left')\
            .select('lkp_mls','lkp_mls_listing_id','lkp_source_responded','lkp_ailpropertyid',F.col('asgprimaryid').alias('lkp_asgprimaryid'))
        
        ## Join #3
        
        join_conditions = [(df_listings_with_prop_id.lkp_asgprimaryid == df_property_master_mssql.asgpropid)]        
        df_listings_with_prop_id = df_listings_with_prop_id\
            .join(df_property_master_mssql, on = join_conditions, how = 'left')\
            .select('lkp_mls','lkp_mls_listing_id',\
                F.col('lkp_asgprimaryid').alias('lkp_asg_primary_id'),\
                F.expr('\
                    CASE \
                        WHEN lkp_asgprimaryid IS NULL THEN CAST(NULL AS boolean) \
                        WHEN lkp_asgprimaryid IS NOT NULL AND isprovisional = 0 AND lastvalidated IS NOT NULL THEN true \
                        WHEN lkp_asgprimaryid IS NOT NULL AND asgtimestamp IS NULL THEN true \
                        ELSE false \
                    END').alias('lkp_asg_primary_id_final_flag'),\
                F.lit('MSSQL').alias('lkp_asg_primary_id_source'), \
                F.col('lkp_source_responded').alias('lkp_asg_primary_id_source_responded_flag'), \
                F.lit(None).cast(StringType()).alias('lkp_asg_primary_id_issue_text'),\
                F.expr('\
                    CASE \
                        WHEN lkp_asgprimaryid IS NULL THEN CAST(NULL AS boolean) \
                        WHEN lkp_ailpropertyid <> lkp_asgprimaryid THEN true \
                        ELSE false \
                    END').alias('lkp_asg_primary_id_mssql_fixed_flag')\
            )
     
    return df_listings_with_prop_id
    