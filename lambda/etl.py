import boto3
import pandas as pd
import pandasql as psql
import s3fs
import logging

logger = logging.getLogger()
object_types = ["client","portfolios","accounts","transactions"]

def lambda_handler(event, context):
    bucket_name = event['Records']['s3']['bucket']['name'];
    csv_object  = event['Records']['s3']['object']['key'];
    objects_for_processing = validate_object_group (bucket_name,csv_object)
    if objects_for_processing:
        logger.Info("Processing CSV")
        processing_csv(bucket_name,objects_for_processing)
    else :
        logger.Info("Already Processed")

def validate_object_group (bucket_name,csv_object):
    ### Validate is this object already Processed with tags
    valid_date = csv_object.split("_")[-1]
    valid_objects = object_builder(valid_date)
    for s3_object in valid_objects:
        try:
            if get_tags(bucket_name,s3_object).['TagSet']['Value'] == "processed":
                return []
        ### Block to catch if object doesnt exsists
        except (ValueError, TypeError):
            logger.error("Error Validating object")
            return []
    return valid_objects           

def object_builder(valid_time):
    return [objects + "_" + valid_time + ".csv" for objects in object_types]

def get_tags(bucket_name,valid_object):
    s3 = boto3.client('s3')
    object_tag = s3.get_object_tagging(Bucket=bucket_name,Key=valid_object)
    return object_tag

def processing_csv(bucket_name,objects_for_processing):
    client_message = {}
    portfolio_message = {}
    error_message = {}
    for csv_type in objects_for_processing:
        try:
            globals()['df_%s' %csv_type.split("_".[0]]  = pd.read_csv('s3://%s/%s'%(bucket_name,csv_type))
        except (ValueError, TypeError):
            logger.error("Error creating dataframe object")
        for client_reference ,row in df_client:
            accountnumber = psql.sqldf("select accout_number from df_portfolios where  client_reference=%s"%(client_reference))
            taxes_paid = psql.sqldf("select accout_number from df_accounts where  accout_number=%s"%(accountnumber))
            tax_free_allowance = psql.sqldf("select accout_number from df_clients where  accout_number=%s"%(accountnumber))
