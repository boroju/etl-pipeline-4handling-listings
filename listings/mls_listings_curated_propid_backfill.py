"""
mls_listings_curated_propid_backfill.py
Author: Randall Gonzalez
Created: 2021-08-25
Desc: 
"""
## -------- SOLUTION NOTES ----------------
## >> Script to backfill asg_primary_id column of mls.listings, either first time or on an ongoing basis.

import sys
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, TimestampType, BooleanType, LongType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
import time 
from amherst_common.amherst_logger import AmherstLogger
from amherst_common.pyspark_utils import get_schema
import mls_listings_transform as T
import os
from pyspark.sql.utils import AnalysisException

def parse_args():
    """
    Argument parsing function

    :return: Namespace containing all of the command line arguments
    """
    # Setup argument parsing
    parser = argparse.ArgumentParser(description='MLS Listings Curated PropID Backfill')
    
    parser.add_argument('--listings_delta_dir', type=str, required=True, help='Specify path to the listings delta table file(s)')
    parser.add_argument('--input_mls_list', type=str, required=False, help='Specify comma separated list of MLS codes to filter the input data on') 
    parser.add_argument('--backfill_start_date', type=str, required=False, help='Provide date in YYYYMMDD format')
    parser.add_argument('--backfill_end_date', type=str, required=False, help='Provide date in YYYYMMDD format')
    parser.add_argument('--backfill_limit', type=int, required=False, help='Specify maximum number of listing property ids to lookup - all if not provided') 
    parser.add_argument('--property_id_source', type=str, required=True, choices=['API','MSSQL'], help='Specify trusted source of property ids')
    parser.add_argument('--property_id_api_endpoint', type=str, required=False, help='Specify URL of the PropertyMaster API endpoint')
    parser.add_argument('--property_id_modes', type=str, required=True, nargs='+', choices=['Null','Provisional','Final'], help='Specify mode(s) to be used when looking up property ids')
    parser.add_argument('--property_id_api_batch_size', type=int, required=False, default=500, help='Specify batch size to be used when requesting property ids from the PropertyMaster API')    
    parser.add_argument('--property_id_api_sleep', type=float, required=False, default=0.01, help='Specify sleep in seconds before executing a post request to the PropertyMaster API')    
    parser.add_argument('--input_dir_listings_mssql', type=str, required=False, help='Specify path to the MSSQL Listing_dt data set')
    parser.add_argument('--input_dir_property_master_mssql', type=str, required=False, help='Specify path to the MSSQL PropertyMaster_dt data set')
    parser.add_argument('--listings_orc_dir', type=str, required=True, help='Specify path to the listings ORC file(s). This path should be different than the one specified in listings_delta_dir parameter')
    parser.add_argument('--overwrite_orc_location', action='store_true', required=False, help='Specify whether output should be written to orc directory')    
    parser.add_argument('--num_output_orc_files', type=int, required=False, default=100, help='Number of files to be used when repartioning the orc output')     
    parser.add_argument('--vacuum_hrs', type=int, required=False, help='Number of hours for Delta table retention')     
    parser.add_argument('-s', '--target_schema_file', type=str, required=True, help='JSON schema file for desired curated data')
    parser.add_argument('-p', '--shuffle_partitions', type=int, required=False, default=2, help='Provide number of shuffle partitions to be used')
    parser.add_argument('-g', '--log_dir', type=str, required=True, help='Specify path to the log directory')
    parser.add_argument('-d', '--debug', action='store_true', required=False, help='Specify log level as DEBUG')
    parsed_args = parser.parse_args()
    
    return parsed_args

## Helper functions start
   
## Helper functions end 
    
## ==============================
##  MAIN starts here
## ==============================

def main():

    ## Read the final target schema that we want to enforce
    logger.info(f'Opening schema file [{_target_schema_file}]')
    try:
        target_schema = get_schema(_target_schema_file)
    except Exception as e:
        logger.error(f'Error opening schema file [{_target_schema_file}]')
        logger.error(str(e))
        logger.error(f'Exiting program')
        sys.exit(1) 
        
    ## If the Delta table does not exist, create it using orc data as baseline
    ## This will be executed only on the first run
    logger.info(f'Try reading curated Delta table data to see if it exists')
    try:
        spark.read.format("delta").load(_listings_delta_dir)
    except AnalysisException as e:
        logger.info(f'There is currently no curated Delta table data to read. {str(e)}')
        logger.info(f'Creating Delta table at location: [{_listings_delta_dir}]')
        logger.info(f'...using orcs at location: [{_listings_orc_dir}]')
        df_listings_orc = spark.read.orc(_listings_orc_dir)
        ## Add placeholders for the property id columns if they don't exist already
        if not 'asg_primary_id' in df_listings_orc.columns:   
            logger.info(f'...with placeholders for the new property id columns')
            df_listings_orc = df_listings_orc\
                .withColumn('asg_primary_id', F.lit(None).cast(LongType()))\
                .withColumn('asg_primary_id_final_flag', F.lit(None).cast(BooleanType()))\
                .withColumn('asg_primary_id_source', F.lit(None).cast(StringType()))\
                .withColumn('asg_primary_id_source_queried_timestamp', F.lit(None).cast(TimestampType()))\
                .withColumn('asg_primary_id_source_responded_flag', F.lit(None).cast(BooleanType()))\
                .withColumn('asg_primary_id_issue_text', F.lit(None).cast(StringType()))\
                .withColumn('asg_primary_id_mssql_fixed_flag', F.lit(None).cast(BooleanType()))\
                .withColumn('asg_primary_id_updated_flag', F.lit(None).cast(BooleanType()))\
                .withColumn('asg_primary_id_updated_timestamp', F.lit(None).cast(TimestampType()))\
                .withColumn('asg_primary_id_previous_value', F.lit(None).cast(LongType()))\
                .withColumn('asg_primary_id_load_status', F.lit('Null').cast(StringType()))
        ## Create the Delta table
        df_listings_orc.select(*target_schema.fieldNames())\
            .write.format("delta").mode("overwrite").option("userMetadata", "Create listings delta table").save(_listings_delta_dir)    
    
    ## Read input data and auxiliary datasets    
    logger.info(f'Reading input data and auxiliary datasets')
    try:
        ## We know this exists because we just created it if it did not
        df_listings_to_backfill = spark.read.format("delta").load(_listings_delta_dir)   
        ## Reference data sets
        if (_property_id_source == 'MSSQL'):
            df_listings_mssql = spark.read.orc(_input_dir_listings_mssql)
            df_property_master_mssql = spark.read.orc(_input_dir_property_master_mssql)
    except Exception as e:
        logger.error(f'Error reading input data')
        logger.error(str(e))
        logger.error(f'Exiting program')
        sys.exit(1)       
    
    ## Filter working data set
    if _input_mls_list:
        df_listings_to_backfill = df_listings_to_backfill.filter(F.col('mls').isin(_input_mls_list))
            
    if _backfill_start_date and _backfill_end_date:
        df_listings_to_backfill = df_listings_to_backfill.filter(f'load_date between "{_backfill_start_date}" and "{_backfill_end_date}"')
              
    if _property_id_modes:
        df_listings_to_backfill = df_listings_to_backfill.filter(F.col('asg_primary_id_load_status').isin(_property_id_modes))

    if _backfill_limit:
        ## If a limit is being applied, give higher priority to records we have not tried to lookup before
        ## Priority order:
        ##      source: null, MSSQL, API ---> F.desc_nulls_first
        ##      load_status_num: 1-Null, 2-Provisional, 3-Final --> F.asc_nulls_first
        ##      source_responded_flag: null, false, true --> F.asc_nulls_first 
        df_listings_to_backfill = df_listings_to_backfill\
            .withColumn('_asg_primary_id_load_status_num',F.expr("CASE WHEN asg_primary_id_load_status = 'Null' THEN 1 WHEN asg_primary_id_load_status = 'Provisional' THEN 2 WHEN asg_primary_id_load_status = 'Final' THEN 3 END"))\
            .orderBy(F.desc_nulls_first('asg_primary_id_source'), F.asc_nulls_first('_asg_primary_id_load_status_num'), F.asc_nulls_first('asg_primary_id_source_responded_flag'), F.asc('mls'), F.asc('mls_listing_id'))\
            .limit(_backfill_limit).drop('_asg_primary_id_load_status_num')

    '''
    PROPERTYID LOOKUP
    '''

    logger.info(f'Property id lookup start')
    
    ## Get property ids for that listings subset (*potentially limited by limit parameter*)
    if _property_id_source == 'API':
        df_listings_with_prop_id = T.get_listing_property_ids_from_api(df_listings_to_backfill, _property_id_api_endpoint, _property_id_api_batch_size, _property_id_api_sleep, logger, _debug, spark)
    elif _property_id_source == 'MSSQL':
        df_listings_with_prop_id = T.get_listing_property_ids_from_mssql(df_listings_to_backfill, df_listings_mssql, df_property_master_mssql, logger, _debug, spark)

    if df_listings_with_prop_id is not None:
      
        ## Inner join the main dataset with the lookup results on (mls, mls listing id)
        join_conditions = [(df_listings_to_backfill.mls == df_listings_with_prop_id.lkp_mls) & \
            (df_listings_to_backfill.mls_listing_id == df_listings_with_prop_id.lkp_mls_listing_id)]

        ## Here the order is important, asg_primary_id column *must* go last
        df_listings_to_backfill = df_listings_to_backfill.join(df_listings_with_prop_id, on = join_conditions, how = 'inner')\
            .withColumn('asg_primary_id_final_flag', F.col('lkp_asg_primary_id_final_flag'))\
            .withColumn('asg_primary_id_source', F.col('lkp_asg_primary_id_source'))\
            .withColumn('asg_primary_id_source_queried_timestamp', F.current_timestamp())\
            .withColumn('asg_primary_id_source_responded_flag', F.col('lkp_asg_primary_id_source_responded_flag'))\
            .withColumn('asg_primary_id_issue_text', F.col('lkp_asg_primary_id_issue_text'))\
            .withColumn('asg_primary_id_mssql_fixed_flag', F.col('lkp_asg_primary_id_mssql_fixed_flag'))\
            .withColumn('asg_primary_id_updated_flag', F.expr('not(asg_primary_id <=> lkp_asg_primary_id)'))\
            .withColumn('asg_primary_id_updated_timestamp', F.expr('CASE WHEN asg_primary_id_updated_flag = true THEN current_timestamp() ELSE null END').cast(TimestampType()))\
            .withColumn('asg_primary_id_previous_value', F.expr('CASE WHEN asg_primary_id_updated_flag = true THEN asg_primary_id ELSE null END').cast(LongType()))\
            .withColumn('asg_primary_id_load_status', F.expr("CASE WHEN lkp_asg_primary_id_final_flag IS NULL THEN 'Null' WHEN lkp_asg_primary_id_final_flag = true THEN 'Final' WHEN lkp_asg_primary_id_final_flag = false THEN 'Provisional' END"))\
            .withColumn('asg_primary_id', F.col('lkp_asg_primary_id'))\
            .drop('lkp_mls','lkp_mls_listing_id','lkp_asg_primary_id','lkp_asg_primary_id_final_flag','lkp_asg_primary_id_source','lkp_asg_primary_id_source_responded_flag','lkp_asg_primary_id_issue_text','lkp_asg_primary_id_mssql_fixed_flag')                 
        
    else:
        logger.error('Property id lookup returned null dataframe')
        logger.error(f'Exiting program')
        sys.exit(1)            

    '''
    FINAL DATASET
    '''
      
    ## Select the final list of columns in the order desired
    df_listings_to_backfill = df_listings_to_backfill.select(*target_schema.fieldNames())
    
    '''
    SAVE DATA
    '''
    
    logger.info(f'Saving data')
    
    # Read curated Delta table to create destination for Merge
    df_curated = spark.read.format("delta").load(_listings_delta_dir)
    df_curated.createOrReplaceTempView('destination')
    # Set source for Merge
    df_listings_to_backfill.createOrReplaceTempView('source')       
    
    # List of cols to use for the merge join condition
    merge_on_cols = ['mls', 'mls_listing_id']
    sql_merge_on = ' AND '.join([f'destination.{col} = source.{col}' for col in merge_on_cols])    
    
    # Create the match condition to only update matched rows if at least one of these columns has changed
    match_cols = ['asg_primary_id','asg_primary_id_final_flag','asg_primary_id_source',\
        'asg_primary_id_source_queried_timestamp','asg_primary_id_source_responded_flag',\
        'asg_primary_id_issue_text','asg_primary_id_mssql_fixed_flag','asg_primary_id_updated_flag','asg_primary_id_updated_timestamp',\
        'asg_primary_id_previous_value','asg_primary_id_load_status'\
    ]
    sql_match_condition = 'NOT(' + ' AND '.join([f'destination.{col} <=> source.{col}' for col in match_cols]) + ')'
    
    sql = f'''
            merge into destination using source
            on {sql_merge_on}
            when matched and ({sql_match_condition}) then
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
    if _overwrite_orc_location:
        logger.info(f'Overwrite orc data at location: [{_listings_orc_dir}] using Delta table from location: [{_listings_delta_dir}]')
        spark.read.format("delta") \
            .load(_listings_delta_dir) \
            .drop('asg_primary_id_source','asg_primary_id_source_queried_timestamp','asg_primary_id_source_responded_flag','asg_primary_id_issue_text','asg_primary_id_mssql_fixed_flag','asg_primary_id_updated_flag','asg_primary_id_updated_timestamp','asg_primary_id_previous_value','asg_primary_id_load_status') \
            .repartition(_num_output_orc_files) \
            .write.orc(_listings_orc_dir, mode='overwrite', compression='zlib')

    # Cleanup older Delta table versions beyond n hours
    if _vacuum_hrs:
        logger.info(f'Vacuum Delta table data older than {_vacuum_hrs} hours.')
        DeltaTable.forPath(spark, _listings_delta_dir).vacuum(_vacuum_hrs)

    logger.info(f'Finished processing')

## ==============================
##  MAIN ends here
## ==============================

if __name__ == '__main__':

    start_time = time.time()

    # Process command line arguments and set global vars
    args = parse_args()
 
    _listings_delta_dir = args.listings_delta_dir
    _input_mls_list = args.input_mls_list
    if _input_mls_list:
        _input_mls_list = [s.strip() for s in _input_mls_list.split(',')] 
    _backfill_start_date = args.backfill_start_date
    _backfill_end_date = args.backfill_end_date
    _backfill_limit = args.backfill_limit
    _property_id_source = args.property_id_source
    _property_id_api_endpoint = args.property_id_api_endpoint
    _property_id_modes = args.property_id_modes
    _property_id_api_batch_size = args.property_id_api_batch_size
    _property_id_api_sleep = args.property_id_api_sleep
    _input_dir_listings_mssql = args.input_dir_listings_mssql
    _input_dir_property_master_mssql = args.input_dir_property_master_mssql
    _listings_orc_dir = args.listings_orc_dir
    _overwrite_orc_location = args.overwrite_orc_location
    _num_output_orc_files = args.num_output_orc_files
    _vacuum_hrs = args.vacuum_hrs
    _target_schema_file = args.target_schema_file
    _shuffle_partitions = args.shuffle_partitions
    _log_dir = args.log_dir
    _debug = args.debug
        
    # Setup logging
    logger = AmherstLogger(log_directory=_log_dir, log_file_app='MLS Listings Curated PropID Backfill', vendor_name='MLS', vendor_product='listings')

    for key, value in args.__dict__.items():
        logger.info(f'{key}: {value}')
    
    ## Validate directories
    if (os.path.realpath(_listings_orc_dir) == os.path.realpath(_listings_delta_dir)):
        logger.error(f'--listings_orc_dir and --listings_delta_dir cannot be the same directory')
        logger.error(f'Exiting program')
        sys.exit(1)    
    
    # Set up spark session
    spark = SparkSession.builder \
        .appName('MLS Listings Curated PropID Backfill') \
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