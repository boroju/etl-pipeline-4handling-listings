## -------- SOLUTION NOTES ----------------
##
import sys
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, TimestampType, LongType
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
    parser.add_argument('--listings_output_dir', type=str, required=True, help='Specify path for the output ORC file(s). This path should be different than the one specified in listings_curated_dir parameter')
    parser.add_argument('--num_output_files', type=int, required=False, default=100, help='Number of files to be used when repartioning the output')     
    parser.add_argument('--vacuum_hrs', type=int, required=False, help='Number of hours for Delta table retention')         
    parser.add_argument('-s', '--target_schema_file', type=str, required=True, help='JSON schema file for desired curated data')
    parser.add_argument('-p', '--shuffle_partitions', type=int, required=False, default=2, help='Provide number of shuffle partitions to be used')
    parser.add_argument('-r', '--reject_data_dir', type=str, required=True, help='Specify path to the directory that will store rejected records')
    parser.add_argument('-g', '--log_dir', type=str, required=True, help='Specify path to the log directory')
    parser.add_argument('--property_id_source', type=str, required=True, choices=['API','MSSQL','NONE'], help='Specify trusted source of property ids')
    parser.add_argument('--property_id_api_endpoint', type=str, required=False, help='Specify URL of the PropertyMaster API endpoint')
    parser.add_argument('--property_id_modes', type=str, required=False, nargs='+', choices=['New','Null','Provisional','Final'], default=['New','Null'], help='Specify mode(s) to be used when looking up property ids')
    parser.add_argument('--property_id_limit', type=int, required=False, help='Specify maximum number of listing property ids to lookup - all if not provided')
    parser.add_argument('--property_id_api_batch_size', type=int, required=False, default=500, help='Specify batch size to be used when requesting property ids from the PropertyMaster API')    
    parser.add_argument('--property_id_api_sleep', type=float, required=False, default=0.01, help='Specify sleep in seconds before executing a post request to the PropertyMaster API')    
    parser.add_argument('--input_dir_listings_mssql', type=str, required=False, help='Specify path to the MSSQL Listing_dt data set')
    parser.add_argument('--input_dir_property_master_mssql', type=str, required=False, help='Specify path to the MSSQL PropertyMaster_dt data set')
    parser.add_argument('-d', '--debug', action='store_true', required=False, help='Specify log level as DEBUG')
    parser.add_argument('--log_rejected_records', action='store_true', required=False, help='Specify whether rejected records should be logged')
    parsed_args = parser.parse_args()
    
    return parsed_args

## Helper functions start  
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
        if (_property_id_source == 'MSSQL'):
            df_listings_mssql = spark.read.orc(_input_dir_listings_mssql)
            df_property_master_mssql = spark.read.orc(_input_dir_property_master_mssql)
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
    df_listings_new = T.transform_listings(df_listings_good, df_counties, df_geo_ids, target_schema, spark)
     
    '''
    PREPARE LISTINGS CURATED DATASET
    ''' 

    ## Current listings in curated, may not exist if this is first run
    ## Add column "_from" to curated too, before doing the union
    df_listings_curated = None
    try: 
        df_listings_curated = spark.read.format("delta").load(listings_delta_dir)\
            .withColumn('_from', F.lit('curated'))
        ## Here we may need reordering of columns!
    except Exception as e:
        logger.info(f'Listings curated does not exist in specified location, using empty dataset instead')
        logger.info(str(e))
    
    ## Union "new" and "curated" listings, if curated exists
    if df_listings_curated:
        df_listings_union = df_listings_new.withColumn('_from',F.lit('new')).union(df_listings_curated)
    else:
        df_listings_union = df_listings_new.withColumn('_from',F.lit('new'))
        
    ## Apply window to find out most recent record. If "new" and "curated" records have same orderBy values, we keep the "new" record
    ## Also recalculate "create_timestamp" (we want the min value available) and asg_prop_id (we want the max value available, if any)
    ## Note that each aggregate function uses a different window, but all are partitioned the same:
    
    ## For F.row_number
    window1 = Window.partitionBy(['mls', 'mls_listing_id']).orderBy(F.desc('source_as_of_date'),F.desc('listing_date'),F.asc('entry_date'),F.desc('load_date'),F.desc('_from'))
    ## For F.min and F.max
    window2 = Window.partitionBy(['mls', 'mls_listing_id'])
    
    df_listings_union = df_listings_union.select(df_listings_union['*'],\
        F.row_number().over(window1).alias('_rownum'),\
        F.min('create_timestamp').over(window2).alias('_create_timestamp'),\
        F.max('asg_primary_id').over(window2).alias('_asg_primary_id'),\
        F.max('asg_primary_id_final_flag').over(window2).alias('_asg_primary_id_final_flag'),\
        F.max('asg_primary_id_source').over(window2).alias('_asg_primary_id_source'),\
        F.max('asg_primary_id_source_queried_timestamp').over(window2).alias('_asg_primary_id_source_queried_timestamp'),\
        F.max('asg_primary_id_source_responded_flag').over(window2).alias('_asg_primary_id_source_responded_flag'),\
        F.max('asg_primary_id_issue_text').over(window2).alias('_asg_primary_id_issue_text'),\
        F.max('asg_primary_id_mssql_fixed_flag').over(window2).alias('_asg_primary_id_mssql_fixed_flag'),\
        F.max('asg_primary_id_updated_flag').over(window2).alias('_asg_primary_id_updated_flag'),\
        F.max('asg_primary_id_updated_timestamp').over(window2).alias('_asg_primary_id_updated_timestamp'),\
        F.max('asg_primary_id_previous_value').over(window2).alias('_asg_primary_id_previous_value'),\
        F.when(F.max('asg_primary_id_load_status').over(window2).isNull(),F.lit('Null')).otherwise(F.max('asg_primary_id_load_status').over(window2)).alias('_asg_primary_id_load_status')
    )
 
    ## Build listings_final
    ## Note here we only keep the records that are changing
    df_listings_final = df_listings_union.filter((F.col('_from') == 'new') & (F.col('_rownum') == 1))\
        .withColumn('create_timestamp',F.col('_create_timestamp'))\
        .withColumn('asg_primary_id',F.col('_asg_primary_id'))\
        .withColumn('asg_primary_id_final_flag',F.col('_asg_primary_id_final_flag'))\
        .withColumn('asg_primary_id_source',F.col('_asg_primary_id_source'))\
        .withColumn('asg_primary_id_source_queried_timestamp',F.col('_asg_primary_id_source_queried_timestamp'))\
        .withColumn('asg_primary_id_source_responded_flag',F.col('_asg_primary_id_source_responded_flag'))\
        .withColumn('asg_primary_id_issue_text',F.col('_asg_primary_id_issue_text'))\
        .withColumn('asg_primary_id_mssql_fixed_flag',F.col('_asg_primary_id_mssql_fixed_flag'))\
        .withColumn('asg_primary_id_updated_flag',F.col('_asg_primary_id_updated_flag'))\
        .withColumn('asg_primary_id_updated_timestamp',F.col('_asg_primary_id_updated_timestamp'))\
        .withColumn('asg_primary_id_previous_value',F.col('_asg_primary_id_previous_value'))\
        .withColumn('asg_primary_id_load_status',F.col('_asg_primary_id_load_status'))\
        .drop('_rownum','_create_timestamp','_asg_primary_id','_asg_primary_id_final_flag',\
        '_asg_primary_id_source','_asg_primary_id_source_queried_timestamp',\
        '_asg_primary_id_source_responded_flag','_asg_primary_id_issue_text','_asg_primary_id_mssql_fixed_flag',\
        '_asg_primary_id_updated_flag','_asg_primary_id_updated_timestamp','_asg_primary_id_previous_value','_asg_primary_id_load_status')
 
    ## Calculate outdated records
    if _log_rejected_records:
        df_listings_outdated = df_listings_union.filter((F.col('_from') == 'new') & (F.col('_rownum') > 1))\
            .drop('_from','_rownum','_create_timestamp','_asg_primary_id','_asg_primary_id_final_flag',\
            '_asg_primary_id_source','_asg_primary_id_source_queried_timestamp',\
            '_asg_primary_id_source_responded_flag','_asg_primary_id_issue_text','_asg_primary_id_mssql_fixed_flag',\
            '_asg_primary_id_updated_flag','_asg_primary_id_updated_timestamp','_asg_primary_id_previous_value','_asg_primary_id_load_status')\
            .withColumn('_reject_reasons',F.lit('Outdated record'))
      
    '''
    PROPERTYID LOOKUP
    '''

    if _property_id_source in (['API','MSSQL']):
    
        logger.info(f'Property id lookup start')
    
        ## Prepare the where clause to filter the listings dataset on
        filters = []
        if 'New' in _property_id_modes:
            filters.append("(_from = 'new' AND asg_primary_id_load_status = 'Null' AND asg_primary_id_source_queried_timestamp IS NULL)")
        if 'Null' in _property_id_modes:
            filters.append("(_from = 'new' AND asg_primary_id_load_status = 'Null' AND asg_primary_id_source_queried_timestamp IS NOT NULL)")	
        if 'Provisional' in _property_id_modes:
            filters.append("(_from = 'new' AND asg_primary_id_load_status = 'Provisional')")
        if 'Final' in _property_id_modes:
            filters.append("(_from = 'new' AND asg_primary_id_load_status = 'Final')")
        
        where_clause = ''.join(["(", " OR ".join(filters), ")"])
        
        if _debug:
            logger.info(f'WHERE clause for property id lookup: {where_clause}')        
        
        df_listings_to_lookup = df_listings_final.filter(where_clause)
        
        if _property_id_limit:
        
            ## If a limit is being applied, give higher priority to records we have not tried to lookup before
            ## Priority order:
            ##      source: null, MSSQL, API ---> F.desc_nulls_first
            ##      load_status_num: 0-New, 1-Null, 2-Provisional, 3-Final --> F.asc_nulls_first
            ##      source_responded_flag: null, false, true --> F.asc_nulls_first         
            df_listings_to_lookup = df_listings_to_lookup\
                .withColumn('_asg_primary_id_load_status_num',F.expr("CASE WHEN asg_primary_id_load_status = 'Null' AND asg_primary_id_source_queried_timestamp IS NULL THEN 0 WHEN asg_primary_id_load_status = 'Null' AND asg_primary_id_source_queried_timestamp IS NOT NULL THEN 1 WHEN asg_primary_id_load_status = 'Provisional' THEN 2 WHEN asg_primary_id_load_status = 'Final' THEN 3 END"))\
                .orderBy(F.desc_nulls_first('asg_primary_id_source'), F.asc_nulls_first('_asg_primary_id_load_status_num'), F.asc_nulls_first('asg_primary_id_source_responded_flag'), F.asc('mls'), F.asc('mls_listing_id'))\
                .limit(_property_id_limit).drop('_asg_primary_id_load_status_num')
            
        ## Get property ids for that listings subset
        if _property_id_source == 'API':
            df_listings_with_prop_id = T.get_listing_property_ids_from_api(df_listings_to_lookup, _property_id_api_endpoint, _property_id_api_batch_size, _property_id_api_sleep, logger, _debug, spark)
        elif _property_id_source == 'MSSQL':
            df_listings_with_prop_id = T.get_listing_property_ids_from_mssql(df_listings_to_lookup, df_listings_mssql, df_property_master_mssql, logger, _debug, spark)
      
        if df_listings_with_prop_id is not None:
      
            ## Inner join main dataset with lookup results on (mls, mls listing id)
            join_conditions = [(df_listings_to_lookup.mls == df_listings_with_prop_id.lkp_mls) & \
                (df_listings_to_lookup.mls_listing_id == df_listings_with_prop_id.lkp_mls_listing_id)]

            ## Here the order is important, asg_primary_id column *must* go last
            df_listings_to_lookup = df_listings_to_lookup.join(df_listings_with_prop_id, on = join_conditions, how = 'inner')\
                .withColumn('asg_primary_id_final_flag', F.col('lkp_asg_primary_id_final_flag'))\
                .withColumn('asg_primary_id_source', F.col('lkp_asg_primary_id_source'))\
                .withColumn('asg_primary_id_source_queried_timestamp', F.current_timestamp())\
                .withColumn('asg_primary_id_source_responded_flag', F.col('lkp_asg_primary_id_source_responded_flag'))\
                .withColumn('asg_primary_id_issue_text', F.col('lkp_asg_primary_id_issue_text'))\
                .withColumn('asg_primary_id_mssql_fixed_flag', F.col('lkp_asg_primary_id_mssql_fixed_flag'))\
                .withColumn('asg_primary_id_updated_flag', F.expr('CASE WHEN coalesce(asg_primary_id,-1) <> coalesce(lkp_asg_primary_id,-1) THEN true ELSE false END'))\
                .withColumn('asg_primary_id_updated_timestamp', F.expr('CASE WHEN coalesce(asg_primary_id,-1) <> coalesce(lkp_asg_primary_id,-1) THEN current_timestamp() ELSE null END').cast(TimestampType()))\
                .withColumn('asg_primary_id_previous_value', F.expr('CASE WHEN coalesce(asg_primary_id,-1) <> coalesce(lkp_asg_primary_id,-1) THEN asg_primary_id ELSE null END').cast(LongType()))\
                .withColumn('asg_primary_id_load_status', F.expr("CASE WHEN lkp_asg_primary_id_final_flag IS NULL THEN 'Null' WHEN lkp_asg_primary_id_final_flag = true THEN 'Final' WHEN lkp_asg_primary_id_final_flag = false THEN 'Provisional' END"))\
                .withColumn('asg_primary_id', F.col('lkp_asg_primary_id'))\
                .withColumn('_from', F.lit('propertyid_new'))\
                .drop('lkp_mls','lkp_mls_listing_id','lkp_asg_primary_id','lkp_asg_primary_id_final_flag','lkp_asg_primary_id_source','lkp_asg_primary_id_source_responded_flag','lkp_asg_primary_id_issue_text','lkp_asg_primary_id_mssql_fixed_flag')                 
        
            ## At this point we can union df_listings_to_lookup with the main data set df_listings_final, apply window, order by _from desc 
            df_listings_union = df_listings_final.union(df_listings_to_lookup)
            
            ## Apply window to find out final records. 
            ## Priority order: 
            ##      _from: propertyid_new, new, curated --> F.desc
            window1 = Window.partitionBy(['mls', 'mls_listing_id']).orderBy(F.desc('_from'))
            
            df_listings_union = df_listings_union.select(df_listings_union['*'],\
                F.row_number().over(window1).alias('_rownum')\
            )
         
            ## Build listings_final
            df_listings_final = df_listings_union.filter(F.col('_rownum') == 1)\
                .drop('_rownum')        

        else:
            logger.info('Property id lookup returned null dataframe')
        
    elif _property_id_source == 'NONE':
    
        ## We do nothing to get property ids
        logger.info('Bypassing property id lookup step')
        
    df_listings_final = df_listings_final.drop('_from')                   

    '''
    SAVE DATA
    '''
    
    logger.info(f'Saving data')

    ## Create files for rejected data
    if _log_rejected_records:
        logger.info(f'Writing rows that failed business rules validation to HDFS: {_reject_data_dir}')
        try:
            df_listings_rejected = df_listings_rejected.select(F.to_json(F.struct(*[colName for colName in df_listings_rejected.columns])).alias('_rejected_data'))  
            df_listings_rejected.select('_rejected_data').write.mode(saveMode='overwrite').text(_reject_data_dir, compression='none')
        except Exception as e:
            logger.error(f'Could not write rejected data to dir')
            logger.error(str(e))
            sys.exit(1)    
        logger.info(f'The data has been written to HDFS: {_reject_data_dir}') 
    
    ## Create files for outdated data, which we also reject
    ## This is done in append mode because they go to the same dir as the other rejected records
    ## Moving forward this step is optional because it is not always desirable to keep track of outdated records (especially during a backfill)
    if _log_rejected_records:
        logger.info(f'Writing df_listings_outdated to HDFS: {_reject_data_dir}')
        try:
            df_listings_outdated = df_listings_outdated.select(F.to_json(F.struct(*[colName for colName in df_listings_outdated.columns])).alias('_rejected_data'))
            df_listings_outdated.select('_rejected_data').write.mode(saveMode='append').text(_reject_data_dir, compression='none') 
        except Exception as e:
            logger.error(f'Could not write outdated data to dir')
            logger.error(str(e))
            sys.exit(1)  
        logger.info(f'The data has been written to HDFS: {_reject_data_dir}') 
    
    ## -----
    ## Merge listings data
    ## -----
    
    logger.info(f'Try reading curated Delta table data to see if it exists')
    try:
        spark.read.format("delta").load(listings_delta_dir)
    except AnalysisException as e:
        logger.error(f'There is currently no curated Delta table data to read. {str(e)}')
        logger.info(f'Creating empty Delta table at location: [{listings_delta_dir}]')
        df_listings_final.limit(0).write.format("delta").mode("overwrite").option("userMetadata", "Create empty listings delta table").save(listings_delta_dir)
    
    # Read curated Delta table to create destination for Merge
    df_curated = spark.read.format("delta").load(listings_delta_dir)
    df_curated.createOrReplaceTempView('destination')
    # Set source for Merge
    df_listings_final.createOrReplaceTempView('source')    
    
    # List of cols to use for the merge join condition
    merge_on_cols = ['mls', 'mls_listing_id']
    sql_merge_on = ' AND '.join([f'destination.{col} = source.{col}' for col in merge_on_cols])
    
    # No match condition needed because we already determined that these data is new
    
    sql = f'''
            merge into destination using source
            on {sql_merge_on}
            when matched then
                update set *                
            when not matched then
                insert *
            '''
            
    if _debug:
        logger.debug(f'SQL merge statement:\n{sql}')

    # Run Delta table merge
    logger.info('Merge new data into curated')
    spark.sql(sql)

    # Read current data from Delta table and overwrite orc version for Hive external table
    # Remove extra columns that are used for asg_primary_id logic but we don't want in the final table
    logger.info(f'Overwrite final data at location: [{_listings_output_dir}] using Delta table from location: [{listings_delta_dir}]')

    spark.read.format("delta") \
        .load(listings_delta_dir) \
        .drop('asg_primary_id_source','asg_primary_id_source_queried_timestamp','asg_primary_id_source_responded_flag','asg_primary_id_issue_text','asg_primary_id_mssql_fixed_flag','asg_primary_id_updated_flag','asg_primary_id_updated_timestamp','asg_primary_id_previous_value','asg_primary_id_load_status')\
        .repartition(_num_output_files) \
        .write.orc(_listings_output_dir, mode='overwrite', compression='zlib')
    
    # Cleanup older Delta table versions beyond n hours
    if _vacuum_hrs:
        logger.info(f'Vacuum Delta table data older than {_vacuum_hrs} hours.')
        DeltaTable.forPath(spark, listings_delta_dir).vacuum(_vacuum_hrs)    
    
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
    _listings_output_dir = args.listings_output_dir
    _num_output_files = args.num_output_files
    _vacuum_hrs = args.vacuum_hrs
    _target_schema_file = args.target_schema_file
    _shuffle_partitions = args.shuffle_partitions
    _reject_data_dir = args.reject_data_dir
    _log_dir = args.log_dir
    _property_id_source = args.property_id_source
    _property_id_api_endpoint = args.property_id_api_endpoint
    _property_id_modes = args.property_id_modes
    _property_id_limit = args.property_id_limit
    _property_id_api_batch_size = args.property_id_api_batch_size
    _property_id_api_sleep = args.property_id_api_sleep
    _input_dir_listings_mssql = args.input_dir_listings_mssql
    _input_dir_property_master_mssql = args.input_dir_property_master_mssql
    _debug = args.debug
    _log_rejected_records = args.log_rejected_records
        
    # Set the Delta table path based on output_dir with suffix "_delta"
    p = Path(_listings_output_dir)
    listings_delta_dir = Path(p.parent, p.name + '_delta').as_posix()           
        
    # Setup logging
    logger = AmherstLogger(log_directory=_log_dir, log_file_app='MLS Listings Curated', vendor_name='MLS', vendor_product='listings')

    for key, value in args.__dict__.items():
        logger.info(f'{key}: {value}')
          
    # Set up spark session
    spark = SparkSession.builder \
        .appName('MLS Listings Curated') \
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
