import boto3 
import botocore
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError, ParamValidationError, WaiterError
import loguru
from loguru import logger
import os
import io
from io import BytesIO, StringIO
import pandas as pd
import numpy as np
import datetime
from datetime import datetime 
from datetime import timedelta

AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']


# ---------------------------------------
# FUNCTIONS
# ---------------------------------------

# FUNCTION TO EXECUTE ATHENA QUERY AND RETURN RESULTS
# ----------

def run_athena_query(query:str, database: str, region:str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            logger.info(f'Query is in {state} state..')
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')
            

        # OBTAIN DATA

        # --------------



        query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                MaxResults= 1000)
        


        # Extract qury result column names into a list  

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]



        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]



        # Convert data rows into a list of lists
        query_results_data = [[r['VarCharValue'] for r in row['Data']] for row in data_rows]



        # Paginate Results if necessary
        while 'NextToken' in query_results:
                query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                NextToken=query_results['NextToken'],
                                                MaxResults= 1000)



                # Extract quuery result data rows
                data_rows = query_results['ResultSet']['Rows'][1:]


                # Convert data rows into a list of lists
                query_results_data.extend([[r['VarCharValue'] for r in row['Data']] for row in data_rows])



        results_df = pd.DataFrame(query_results_data, columns = col_names)
        
        return results_df


    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax
            
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist
            
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions
            
        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions


# Check S3 Path for Existing Data
# -----------

def check_path_for_objects(bucket: str, s3_prefix:str):

  logger.info(f'Checking for existing data in {bucket}/{s3_prefix}')

  # Create s3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # List objects in s3_prefix
  result = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

  # Instantiate objects_exist
  objects_exist=False

  # Set objects_exist to true if objects are in prefix
  if 'Contents' in result:
      objects_exist=True

      logger.info('Data already exists!')

  return objects_exist

# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):


  logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

  # Create an S3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # Use list_objects_v2 to list all objects within the specified prefix
  objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

  # Extract the list of object keys
  keys_to_delete = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]

  # Check if there are objects to delete
  if keys_to_delete:
      # Delete the objects using 'delete_objects'
      response = s3_client.delete_objects(
          Bucket=bucket,
          Delete={'Objects': [{'Key': key} for key in keys_to_delete]}
      )
      logger.info(f"Deleted {len(keys_to_delete)} objects")
  else:
      logger.info("No objects to delete")




# --------------
# Function to run Athena query , not return results
# --------------


def run_athena_query_no_results(query:str, database: str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name='us-east-1',
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            logger.info(f'Query is in {state} state..')
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')

                    
    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax
            
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist
            
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions
            
        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions





# ========================================================================
# Execute Code
# ========================================================================

DATABASE = 'prymal'
REGION = 'us-east-1'

# Construct query to pull data by product
# ----

QUERY = f"""SELECT *
            FROM shopify_line_items
        """

# Query datalake to get quantiy sold per sku for the last 120 days
# ----

result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)
# result_df.columns = ['partition_date','order_date','sku','sku_name','qty_sold']

logger.info(result_df.head(3))
logger.info(result_df.info())
logger.info(f"Count of NULL RECORDS: {len(result_df.loc[result_df['order_date'].isna()])}")
# Format datatypes & new columns
result_df['order_date'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%m-%d')
result_df['week'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%W')
result_df['product_rev'] = result_df['quantity'].astype(float) * result_df['price'].astype(float)

logger.info(f"MIN DATE: {result_df['order_date'].min()}")
logger.info(f"MAX DATE: {result_df['order_date'].max()}")


# Create dataframe copy
shopify_line_item_df = result_df.copy()

# --------------------
# calculate first order date & age at time of purchase
# --------------------

first_order_df = shopify_line_item_df.groupby('email',as_index=False).agg({
                                    'order_date':'min'

})

first_order_df.columns = ['email','first_order_date']

shopify_line_item_df = shopify_line_item_df.merge(first_order_df, how='left',on='email')
shopify_line_item_df['age'] = (pd.to_datetime(shopify_line_item_df['order_date'])- pd.to_datetime(shopify_line_item_df['first_order_date'])).dt.days
shopify_line_item_df['cuttoff_60_days'] = pd.to_datetime(pd.to_datetime(shopify_line_item_df['first_order_date']) + timedelta(60)).dt.strftime('%Y-%m-%d')
shopify_line_item_df['first_order_month'] = pd.to_datetime(shopify_line_item_df['first_order_date']).dt.strftime('%Y-%m')

# --------------------
# Calculate summary statistics for all customers
# --------------------

df = shopify_line_item_df.copy()

cust_log = df.groupby('email',as_index=False).agg(
                                first_order_date=('order_date', 'min'),
                                latest_order_date=('order_date', 'max'),
                                lifetime_spend=('product_rev', 'sum'),
                                lifetime_order_cnt=('order_id', 'nunique')

                     

)

#Calulate lifetime AOV
cust_log['lifetime_aov'] = cust_log['lifetime_spend'] / cust_log['lifetime_order_cnt']

print(cust_log.columns)

cust_log.columns = ['email','first_order_date','latest_order_date','lifetime_spend','lifetime_order_cnt','lifetime_aov']

# --------------------
# Subset to only look at customers > X days old as of today, then for all orders up to their X day cutoff, calculate 

# --total spend
# --avg order count 
# --average order value
# --------------------

# Cutoff age (in days) of customers to assess lifetime value
cutoffs = [30,60,90,120,150,180,365]

for age_cutoff in cutoffs:

    # Subset df to only show customers > 60 days old & only show their purchases up to the 60 day cutoff mark

    df = shopify_line_item_df.loc[(shopify_line_item_df['age'] <= age_cutoff)&(pd.to_datetime(shopify_line_item_df['first_order_date']) + timedelta(age_cutoff) <= pd.to_datetime('today'))]

    # Calculate summary stats for subset of data
    cust_log_subset = df.groupby('email',as_index=False).agg({
                                    'product_rev':'sum',
                                    'order_id':'nunique'

    })

    cust_log_subset['order_id'] = cust_log_subset['order_id'].astype(int)

    aov_col = f'aov_{age_cutoff}_days'
    spend_col = f'spend_{age_cutoff}_days'
    order_cnt_col = f'freq_{age_cutoff}_days'

    cust_log_subset[aov_col] = cust_log_subset['product_rev'] / cust_log_subset['order_id']

    cust_log_subset.columns = ['email',spend_col,order_cnt_col,aov_col]

    cust_log = cust_log.merge(cust_log_subset,how='left',on='email')

# Drop null email records
cust_log = cust_log.loc[cust_log['email']!='',['email', 'first_order_date', 'latest_order_date', 'lifetime_spend',
       'lifetime_order_cnt', 'lifetime_aov', 'spend_30_days', 'freq_30_days',
       'aov_30_days', 'spend_60_days', 'freq_60_days', 'aov_60_days',
       'spend_90_days', 'freq_90_days', 'aov_90_days',
       'spend_120_days', 'freq_60_days', 'aov_120_days',
       'spend_150_days', 'freq_60_days', 'aov_150_days',
       'spend_180_days', 'freq_60_days', 'aov_180_days',
       'spend_365_days', 'freq_60_days', 'aov_365_days']]

# --------------------
# WRITE TO S3
# --------------------

yesterday_date = pd.to_datetime(pd.to_datetime('today') - timedelta(1)).strftime('%Y-%m-%d')


# Create s3 client
s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

# Set bucket
BUCKET = os.environ['S3_PRYMAL_ANALYTICS']

# Log number of rows
logger.info(f'{len(cust_log)} rows in cust_log')
 
# Configure S3 Prefix
S3_PREFIX_PATH = f"shopify/customer_log/partition_date={yesterday_date}/shopify_customer_log_{yesterday_date}.csv"

# Check if data already exists for this partition
data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

# If data already exists, delete it .. 
if data_already_exists == True:
   
   # Delete data 
   delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)


logger.info(f'Writing to {S3_PREFIX_PATH}')


with io.StringIO() as csv_buffer:
    cust_log.to_csv(csv_buffer, index=False)

    response = s3_client.put_object(
        Bucket=BUCKET, 
        Key=S3_PREFIX_PATH, 
        Body=csv_buffer.getvalue()
    )

    status = response['ResponseMetadata']['HTTPStatusCode']

    if status == 200:
        logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
    else:
        logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")


# --------------------
# RUN 'ATHENA ALTER TABLE' TO UPDATE TABLE 
# --------------------


QUERY = f"""

ALTER TABLE shopify_customer_log ADD
  PARTITION (partition_date = '{yesterday_date}')
  
"""

run_athena_query_no_results(query=QUERY, database='prymal-analytics')
