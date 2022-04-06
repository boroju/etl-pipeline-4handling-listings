"""
mls_listings_curated.py
Author: Randall Gonzalez
Created: 2021-01-01
Desc: 
"""
## -------- SOLUTION NOTES ----------------
##
import sys
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
import time
from amherst_common.amherst_logger import AmherstLogger
from amherst_common.pyspark_utils import get_schema
from functools import reduce
import mls_listings_transform as T
from pathlib import Path
from pyspark.sql.utils import AnalysisException

def parse_args():
    """
    Argument parsing function

    :return: Namespace containing all of the command line arguments
    """
    # Setup argument parsing
    parser = argparse.ArgumentParser(description='MLS Listings curated')
    
    parser.add_argument('--from_date', type=str, required=True, help='Provide date in YYYYMMDD format')
    parser.add_argument('--to_date', type=str, required=True, help='Provide date in YYYYMMDD format')          
    parser.add_argument('--input_dir_listings', type=str, required=True, help='Specify comma separated path(s) to the input file(s)')
    parser.add_argument('--input_mls_list', type=str, required=False, help='Specify comma separated list of MLS codes to filter the input data on') 
    parser.add_argument('--input_dir_boards', type=str, required=True, help='Specify path to the boards data set')
    parser.add_argument('--input_dir_states', type=str, required=True, help='Specify path to the states data set')
    parser.add_argument('--input_dir_zipcodes', type=str, required=True, help='Specify path to the zip codes data set')
    parser.add_argument('--input_dir_property_sub_types', type=str, required=True, help='Specify path to the property sub types data set')
    parser.add_argument('--input_dir_counties', type=str, required=True, help='Specify path to the counties data set')
    parser.add_argument('--input_dir_geo_ids', type=str, required=True, help='Specify path to the geos data set')
    parser.add_argument('--listings_hist_output_dir', type=str, required=True, help='Specify path for the listing history output ORC file(s)') 
    parser.add_argument('--num_output_files', type=int, required=False, default=100, help='Number of files to be used when repartioning the output') 
    parser.add_argument('--vacuum_hrs', type=int, required=False, help='Number of hours for Delta table retention')     
    parser.add_argument('-s', '--target_schema_file', type=str, required=True, help='JSON schema file for desired curated data')
    parser.add_argument('-p', '--shuffle_partitions', type=int, required=False, default=2, help='Provide number of shuffle partitions to be used')
    parser.add_argument('-g', '--log_dir', type=str, required=True, help='Specify path to the log directory')
    parser.add_argument('-d', '--debug', action='store_true', required=False, help='Specify log level as DEBUG')
    parsed_args = parser.parse_args()
    
    return parsed_args

## Helper functions start
##
## Helper functions end 
    
## ==============================
##  MAIN starts here
## ==============================

def main(): 
        
    ## Read input data and auxiliary datasets    
    logger.info(f'Reading input data and auxiliary datasets')
    logger.info(f'Reading listings data where load_date between {_from_date} and {_to_date}')
    try:
        df_listings_lst = []
        for d in _input_dir_listings:
            df = spark.read.orc(d).filter(f'load_date between "{_from_date}" and "{_to_date}"')
            if _input_mls_list:
                df = df.filter(F.col('mls').isin(_input_mls_list))            
            df_listings_lst.append(df)
        df_listings = reduce(DataFrame.unionAll, df_listings_lst) 
        ## Reference data sets for business validation
        df_boards = spark.read.orc(_input_dir_boards)
        df_states = spark.read.orc(_input_dir_states)
        df_zipcodes = spark.read.orc(_input_dir_zipcodes)
        df_property_sub_types = spark.read.orc(_input_dir_property_sub_types)
        ## Reference data sets for extra columns
        df_counties = spark.read.orc(_input_dir_counties)
        df_geo_ids = spark.read.orc(_input_dir_geo_ids)
    except Exception as e:
        logger.error(f'Error reading input data')
        logger.error(str(e))
        logger.error(f'Exiting program')
        sys.exit(1)   

    ## Read the final target schema that we want to enforce
    logger.info(f'Opening schema file [{_target_schema_file}]')
    try:
        target_schema = get_schema(_target_schema_file)
    except Exception as e:
        logger.error(f'Error opening schema file [{_target_schema_file}]')
        logger.error(str(e))
        logger.error(f'Exiting program')
        sys.exit(1)

    '''
    VALIDATE AND TRANSFORM INPUT LISTINGS
    '''

    ## Validate against business rules
    ## Keep records with 0 reject flags only
    (df_listings_good, df_listings_rejected) = T.validate_listings(df_listings, df_boards, df_states, df_zipcodes, df_property_sub_types)

    ## Apply transformations on good listings
    df_listing_hist_new = T.transform_listings(df_listings_good, df_counties, df_geo_ids, target_schema, spark)
     
    '''
    PREPARE LISTINGS HISTORY CURATED DATASET
    ''' 

    ## Current listings in curated, may not exist if this is first run
    ## Add column "_from" to curated too, before doing the union
    df_listing_hist_curated = None
    try: 
        df_listing_hist_curated = spark.read.orc(_listings_hist_output_dir)\
            .select(*target_schema.fieldNames())\
            .withColumn('_from', F.lit('curated'))    
    except Exception as e:
        logger.info(f'Listing history curated does not exist in specified location, using empty dataset instead')
    
    ## Union "new" and "curated" listings, if curated exists
    if df_listing_hist_curated:
        df_listing_hist_union = df_listing_hist_new.withColumn('_from',F.lit('new')).union(df_listing_hist_curated)
    else:
        df_listing_hist_union = df_listing_hist_new.withColumn('_from',F.lit('new'))
        
    ## Apply window to find out most recent record. If "new" and "curated" records have same orderBy values, we keep the "new" record
    ## Also recalculate "create_timestamp" (we want the min value available) and asg_prop_id (we want the max value available, if any)
    ## Note that each aggregate function uses a different window, but all are partitioned the same:
    
    ## For F.row_number
    window1 = Window.partitionBy(['mls', 'mls_listing_id', 'source_as_of_date']).orderBy(F.desc('listing_date'),F.asc('entry_date'),F.desc('load_date'),F.desc('_from'))
    ## For F.min
    window2 = Window.partitionBy(['mls', 'mls_listing_id', 'source_as_of_date'])    
    
    df_listing_hist_union = df_listing_hist_union.select(df_listing_hist_union['*'],\
        F.row_number().over(window1).alias('_rownum'),\
        F.min('create_timestamp').over(window2).alias('_create_timestamp')\
    )    
    
    ## For F.lead
    window1 = Window.partitionBy(['mls', 'mls_listing_id']).orderBy(F.desc('source_as_of_date'))

    ## Separate good records and calculate "_old" (previous) values
    ## We only add the record to history if at least one of six columns changed between current record and previous one
    df_listing_hist_final = df_listing_hist_union.filter(F.col('_rownum') == 1)\
        .withColumn('create_timestamp', F.col('_create_timestamp'))\
        .withColumn('listing_status_old', F.lead('listing_status',1,None).over(window1))\
        .withColumn('current_price_old', F.lead('current_price',1,None).over(window1))\
        .withColumn('source_listing_id_old', F.lead('source_listing_id',1,None).over(window1))\
        .withColumn('street_address_raw_old', F.lead('street_address_raw',1,None).over(window1))\
        .withColumn('property_type_old', F.lead('property_type',1,None).over(window1))\
        .withColumn('property_sub_type_old', F.lead('property_sub_type',1,None).over(window1))\
        .withColumn('listing_status_changed_flag', F.expr('not(listing_status <=> listing_status_old)'))\
        .withColumn('current_price_changed_flag', F.expr('not(round(current_price,0) <=> round(current_price_old,0))'))\
        .withColumn('source_listing_id_changed_flag', F.expr('not(source_listing_id <=> source_listing_id_old)'))\
        .withColumn('street_address_raw_changed_flag', F.expr('not(street_address_raw <=> street_address_raw_old)'))\
        .withColumn('property_type_changed_flag', F.expr('not(property_type <=> property_type_old)'))\
        .withColumn('property_sub_type_changed_flag', F.expr('not(property_sub_type <=> property_sub_type_old)'))\
        .withColumn('_driving_columns_changed_flag', F.expr('listing_status_changed_flag OR current_price_changed_flag OR source_listing_id_changed_flag OR street_address_raw_changed_flag OR property_type_changed_flag OR property_sub_type_changed_flag'))\
        .withColumn('_insert_update_flag', F.col('_driving_columns_changed_flag'))\
        .drop('_from','_rownum','_create_timestamp','_driving_columns_changed_flag')
        ## Do not drop '_insert_update_flag' because we need it for the merge logic
        
    '''
    SAVE DATA
    '''
    
    logger.info(f'Saving data')
    
    # Handle case where curated Delta table data may not exist yet, such as on the first run
    logger.info(f'Try reading curated Delta table data to see if it exists')
    try:
        spark.read.format("delta").load(listings_hist_delta_dir)
    except AnalysisException as e:
        logger.error(f'There is currently no curated Delta table data to read. {str(e)}')
        logger.info(f'Creating empty Delta table at location: [{listings_hist_delta_dir}]')
        df_listing_hist_final.limit(0).drop('_insert_update_flag').write.format("delta").mode("overwrite").option("userMetadata", "Create empty listings hist delta table").save(listings_hist_delta_dir)
    
    # Read curated Delta table to create destination for Merge
    df_curated = spark.read.format("delta").load(listings_hist_delta_dir)
    df_curated.createOrReplaceTempView('destination')
    # Set source for Merge
    df_listing_hist_final.createOrReplaceTempView('source')    
    
    # List of cols to use for the merge join condition
    merge_on_cols = ['mls', 'mls_listing_id', 'source_as_of_date']
    sql_merge_on = ' AND '.join([f'destination.{col} = source.{col}' for col in merge_on_cols])
    
    # Create the match condition to only update matched rows if at least one of these columns has changed
    match_cols = ['listing_status_old','current_price_old','source_listing_id_old','street_address_raw_old','property_type_old','property_sub_type_old',\
        'listing_status','current_price','source_listing_id','street_address_raw','property_type','property_sub_type',\
        'listing_status_changed_flag','current_price_changed_flag','source_listing_id_changed_flag','street_address_raw_changed_flag','property_type_changed_flag','property_sub_type_changed_flag'\
    ]
    sql_match_condition = 'NOT(' + ' AND '.join([f'destination.{col} <=> source.{col}' for col in match_cols]) + ')'

    sql = f'''
            merge into destination using source
            on {sql_merge_on}
            when matched and (source._insert_update_flag = true and ({sql_match_condition})) then
                update set *  
            when matched and source._insert_update_flag = false then 
                delete                
            when not matched and source._insert_update_flag = true then
                insert *
            '''
            
    if _debug:
        logger.debug(f'SQL merge statement:\n{sql}')

    # Run Delta table merge
    logger.info('Merge new data into curated')
    spark.sql(sql)

    # Read current data from Delta table and overwrite orc version for Hive external table
    logger.info(f'Overwrite final data at location: [{_listings_hist_output_dir}] using Delta table from location: [{listings_hist_delta_dir}]')

    spark.read.format("delta") \
        .load(listings_hist_delta_dir) \
        .repartition(_num_output_files) \
        .write.orc(_listings_hist_output_dir, mode='overwrite', compression='zlib')
    
    # Cleanup older Delta table versions beyond n hours
    if _vacuum_hrs:
        logger.info(f'Vacuum Delta table data older than {_vacuum_hrs} hours.')
        DeltaTable.forPath(spark, listings_hist_delta_dir).vacuum(_vacuum_hrs)

    logger.info(f'Finished processing')

## ==============================
##  MAIN ends here
## ==============================

if __name__ == '__main__':

    start_time = time.time()

    # Process command line arguments and set global vars
    args = parse_args()

    _from_date = args.from_date
    _to_date = args.to_date  
    _input_dir_listings = args.input_dir_listings
    if _input_dir_listings:
        _input_dir_listings = [s.strip() for s in _input_dir_listings.split(',')]        
    _input_mls_list = args.input_mls_list
    if _input_mls_list:
        _input_mls_list = [s.strip() for s in _input_mls_list.split(',')]    
    _input_dir_boards = args.input_dir_boards
    _input_dir_states = args.input_dir_states
    _input_dir_zipcodes = args.input_dir_zipcodes
    _input_dir_property_sub_types = args.input_dir_property_sub_types
    _input_dir_counties = args.input_dir_counties
    _input_dir_geo_ids = args.input_dir_geo_ids
    _listings_hist_output_dir = args.listings_hist_output_dir
    _num_output_files = args.num_output_files
    _vacuum_hrs = args.vacuum_hrs
    _target_schema_file = args.target_schema_file
    _shuffle_partitions = args.shuffle_partitions
    _log_dir = args.log_dir
    _debug = args.debug
    
    # Set the Delta table path based on output_dir with suffix "_delta"
    p = Path(_listings_hist_output_dir)
    listings_hist_delta_dir = Path(p.parent, p.name + '_delta').as_posix()    
 
    # Setup logging
    logger = AmherstLogger(log_directory=_log_dir, log_file_app='MLS Listings History Curated', vendor_name='MLS', vendor_product='listings')

    for key, value in args.__dict__.items():
        logger.info(f'{key}: {value}')
    
    # Set up spark session
    spark = SparkSession.builder \
        .appName('MLS Listings History Curated') \
        .config('spark.sql.shuffle.partitions', _shuffle_partitions) \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .enableHiveSupport() \
        .getOrCreate()

    # Import Delta Lake APIs after Spark session has started
    from delta.tables import *

    # Do stuff
    main()

    execution_time = (time.time() - start_time)
    logger.info(f'Done. Execution time in seconds: {str(execution_time)}')

    # End program with SUCCESS
    sys.exit(0)