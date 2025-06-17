import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
from datetime import datetime
import time
import logging
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# MWAA Compatible Logging
logger = logging.getLogger(__name__)

# MWAA Environment Variables and Connections
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

def get_mwaa_config():
    """Strictly load configuration from MWAA Airflow Variables"""
    try:
        config = {
            'S3_RAW_BUCKETS': {
                'songs': Variable.get("s3_raw_bucket_songs"),
                'users': Variable.get("s3_raw_bucket_users"),
                'streams': Variable.get("s3_raw_bucket_streams")
            },
            'S3_CLEANED_BUCKET': Variable.get("s3_cleaned_bucket"),
            'PROCESSED_FILES_BUCKET': Variable.get("processed_files_bucket"),
            'REDSHIFT_ROLE': Variable.get("redshift_iam_role"),
            'REDSHIFT_WORKGROUP': Variable.get("redshift_workgroup"),
            'REDSHIFT_DATABASE': Variable.get("redshift_database"),
            'AWS_REGION': Variable.get("aws_region")
        }
        logger.info("Successfully loaded MWAA config from Airflow Variables")
        return config
    except Exception as e:
        logger.error(f"Error loading MWAA config from Variables: {e}")
        raise



# Load MWAA configuration once at the top
MWAA_CONFIG = get_mwaa_config()

S3_RAW_BUCKETS = MWAA_CONFIG['S3_RAW_BUCKETS']
S3_CLEANED_BUCKET = MWAA_CONFIG['S3_CLEANED_BUCKET']
PROCESSED_FILES_BUCKET = MWAA_CONFIG['PROCESSED_FILES_BUCKET']
REDSHIFT_ROLE = MWAA_CONFIG['REDSHIFT_ROLE']
REDSHIFT_WORKGROUP = MWAA_CONFIG['REDSHIFT_WORKGROUP']
REDSHIFT_DATABASE = MWAA_CONFIG['REDSHIFT_DATABASE']
AWS_REGION = MWAA_CONFIG['AWS_REGION']


# Static configurations (unchanged)
PROCESSED_FILES_KEY = 'processed_files/processed_files.json'

prefix_map = {
    'songs': 'songs/',
    'users': 'users/',
    'streams': 'streams/'
}

REDSHIFT_TABLES = {
    'songs': 'songs_table',
    'users': 'users_table',
    'streams': 'streams_table'
}

# Define primary keys for upsert logic
PRIMARY_KEYS = {
    'songs_table': ['id'],
    'users_table': ['user_id'],
    'streams_table': ['user_id', 'track_id', 'listen_time']
}

# Define columns that should be updated (exclude primary keys)
UPDATE_COLUMNS = {
    'songs_table': [
        'track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms',
        'explicit', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
        'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
        'time_signature', 'track_genre'
    ],
    'users_table': [
        'user_name', 'user_age', 'user_country', 'created_at', 'user_tenure_days'
    ],
    'streams_table': []  # Streams are typically insert-only, but you can add columns if needed
}

BASE_REQUIRED_COLUMNS = {
    'songs': [
        'id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms',
        'explicit', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
        'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
        'time_signature', 'track_genre'
    ],
    'users': [
        'user_id', 'user_name', 'user_age', 'user_country', 'created_at'
    ],
    'streams': [
        'user_id', 'track_id', 'listen_time'
    ]
}

FINAL_COLUMNS = {
    'songs': BASE_REQUIRED_COLUMNS['songs'],
    'users': BASE_REQUIRED_COLUMNS['users'] + ['user_tenure_days'],
    'streams': BASE_REQUIRED_COLUMNS['streams'] + ['listen_hour', 'listen_day_of_week', 'listen_month']
}

CREATE_TABLE_QUERIES = {
    'songs_table': """
        CREATE TABLE IF NOT EXISTS public.songs_table (
            id BIGINT PRIMARY KEY,
            track_id VARCHAR(256),
            artists VARCHAR(1024),
            album_name VARCHAR(1024),
            track_name VARCHAR(1024),
            popularity INT,
            duration_ms INT,
            explicit BOOLEAN,
            danceability FLOAT,
            energy FLOAT,
            key INT,
            loudness FLOAT,
            mode INT,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INT,
            track_genre VARCHAR(256)
        );
    """,
    'users_table': """
        CREATE TABLE IF NOT EXISTS public.users_table (
            user_id BIGINT PRIMARY KEY,
            user_name VARCHAR(256),
            user_age INT,
            user_country VARCHAR(256),
            created_at TIMESTAMP,
            user_tenure_days INT
        );
    """,
    'streams_table': """
        CREATE TABLE IF NOT EXISTS public.streams_table (
            user_id BIGINT,
            track_id VARCHAR(256),
            listen_time TIMESTAMP,
            listen_hour INT,
            listen_day_of_week INT,
            listen_month INT,
            PRIMARY KEY (user_id, track_id, listen_time)
        );
    """
}

# MWAA Compatible AWS Clients
def get_aws_clients():
    """Initialize AWS clients with MWAA IAM role credentials"""
    try:
        # MWAA automatically provides AWS credentials via IAM role
        # No need to explicitly specify credentials
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        redshift_data = boto3.client('redshift-data', region_name=AWS_REGION)
        
        logger.info(f"Successfully initialized AWS clients for region: {AWS_REGION}")
        return s3_client, redshift_data
    except Exception as e:
        logger.error(f"Failed to initialize AWS clients: {e}")
        raise

# Initialize clients globally (will use MWAA's IAM role)
s3_client, redshift_data = get_aws_clients()


def load_processed_files():
    """Load processed files metadata with MWAA error handling"""
    try:
        obj = s3_client.get_object(Bucket=PROCESSED_FILES_BUCKET, Key=PROCESSED_FILES_KEY)
        data = obj['Body'].read().decode('utf-8')
        processed_files = set(json.loads(data))
        logger.info(f"Loaded {len(processed_files)} processed files metadata from S3")
        return processed_files
    except s3_client.exceptions.NoSuchKey:
        logger.info("No processed files metadata found. Starting fresh.")
        return set()
    except Exception as e:
        logger.error(f"Error loading processed files metadata: {e}")
        # In MWAA, we should raise the exception to fail the task properly
        raise


def save_processed_files(processed_files):
    """Save processed files metadata with MWAA error handling"""
    try:
        data = json.dumps(list(processed_files), indent=2)
        s3_client.put_object(
            Bucket=PROCESSED_FILES_BUCKET, 
            Key=PROCESSED_FILES_KEY, 
            Body=data,
            ContentType='application/json'
        )
        logger.info(f"Successfully saved {len(processed_files)} processed files metadata to S3")
    except Exception as e:
        logger.error(f"Failed to save processed files metadata: {e}")
        raise


def get_new_files_to_process(dataset_name, source_bucket):
    """Get new files to process with enhanced MWAA logging"""
    prefix = prefix_map.get(dataset_name)
    if not prefix:
        raise ValueError(f"No prefix configured for dataset: {dataset_name}")

    logger.info(f"Checking for new {dataset_name} files in s3://{source_bucket}/{prefix}")
    
    try:
        processed_files = load_processed_files()
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=source_bucket, Prefix=prefix)

        new_files = []
        total_files = 0
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    total_files += 1
                    if key.endswith('.csv') and key not in processed_files:
                        new_files.append(key)
                        logger.info(f"Found new file to process: {key}")

        logger.info(f"Found {len(new_files)} new {dataset_name} files to process out of {total_files} total files")
        return new_files
        
    except Exception as e:
        logger.error(f"Error scanning for new files in {dataset_name}: {e}")
        raise


def ingest_data(dataset_name, source_bucket, ti=None, **kwargs):
    """MWAA compatible data ingestion with proper context handling"""
    # MWAA context handling
    context = kwargs
    execution_date = context.get('logical_date') or context.get('execution_date') or context.get('ds')
    
    if execution_date is None:
        logger.error("No execution date found in Airflow context")
        raise ValueError("execution_date not found in Airflow context")

    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
    
    # Use the configured bucket for this dataset
    actual_source_bucket = S3_RAW_BUCKETS.get(dataset_name, source_bucket)
    
    logger.info(f"Starting MWAA ingestion of '{dataset_name}' from s3://{actual_source_bucket} for {execution_date}")

    try:
        new_files = get_new_files_to_process(dataset_name, actual_source_bucket)
        if not new_files:
            logger.info(f"No new files to process for {dataset_name}")
            if ti:
                ti.xcom_push(key=f"{dataset_name}_raw_key", value=None)
            return None

        logger.info(f"Processing {len(new_files)} new files for {dataset_name}")
        dfs = []
        
        for i, key in enumerate(new_files, 1):
            logger.info(f"Reading file {i}/{len(new_files)}: s3://{actual_source_bucket}/{key}")
            try:
                obj = s3_client.get_object(Bucket=actual_source_bucket, Key=key)
                df = pd.read_csv(io.BytesIO(obj['Body'].read()))
                logger.info(f"File {key} loaded: {len(df)} rows, {len(df.columns)} columns")
                dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to read file {key}: {e}")
                raise

        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined dataframe: {len(combined_df)} rows, {len(combined_df.columns)} columns")

        # Save combined dataframe as pickle to S3 with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_key = f"temp/{dataset_name}_{execution_date.strftime('%Y%m%d')}_{timestamp}.pkl"
        
        buf = io.BytesIO()
        combined_df.to_pickle(buf)
        buf.seek(0)
        
        s3_client.put_object(
            Bucket=S3_CLEANED_BUCKET, 
            Key=temp_key, 
            Body=buf.getvalue(),
            ContentType='application/octet-stream'
        )
        logger.info(f"Saved combined data to s3://{S3_CLEANED_BUCKET}/{temp_key}")

        # Update processed files metadata
        processed_files = load_processed_files()
        processed_files.update(new_files)
        save_processed_files(processed_files)

        if ti:
            ti.xcom_push(key=f"{dataset_name}_raw_key", value=temp_key)

        logger.info(f"Successfully ingested {dataset_name} data: {len(combined_df)} total records")
        return temp_key
        
    except Exception as e:
        logger.error(f"Failed to ingest {dataset_name} data: {e}")
        raise


def clean_dataframe_for_parquet(df):
    """Clean dataframe for Parquet compatibility with enhanced logging"""
    original_cols = len(df.columns)
    index_cols_to_remove = [col for col in df.columns if col.startswith('__index_level_') or col == 'index']
    
    if index_cols_to_remove:
        logger.info(f"Removing {len(index_cols_to_remove)} index columns: {index_cols_to_remove}")
        df = df.drop(columns=index_cols_to_remove)
    
    df_cleaned = df.reset_index(drop=True)
    logger.info(f"DataFrame cleaning: {original_cols} -> {len(df_cleaned.columns)} columns")
    
    return df_cleaned


def create_parquet_compatible_schema(df, dataset_name):
    """Create PyArrow schema with non-nullable types for Redshift compatibility"""
    schema_fields = []
    
    logger.info(f"Creating Parquet schema for {dataset_name} with {len(df.columns)} columns")
    
    for col in df.columns:
        dtype = df[col].dtype
        
        if col == 'user_age' and dataset_name == 'users':
            # Force user_age to be non-nullable int32
            schema_fields.append(pa.field(col, pa.int32(), nullable=False))
        elif col in ['user_id', 'id'] and 'int' in str(dtype):
            schema_fields.append(pa.field(col, pa.int64(), nullable=False))
        elif col in ['popularity', 'duration_ms', 'key', 'mode', 'time_signature', 
                     'listen_hour', 'listen_day_of_week', 'listen_month', 'user_tenure_days'] and 'int' in str(dtype):
            schema_fields.append(pa.field(col, pa.int32(), nullable=False))
        elif col == 'explicit' and dtype == 'bool':
            schema_fields.append(pa.field(col, pa.bool_(), nullable=False))
        elif 'float' in str(dtype):
            schema_fields.append(pa.field(col, pa.float64(), nullable=False))
        elif col in ['created_at', 'listen_time'] and 'datetime' in str(dtype):
            schema_fields.append(pa.field(col, pa.timestamp('us'), nullable=False))
        else:
            # String columns
            schema_fields.append(pa.field(col, pa.string(), nullable=True))
    
    schema = pa.schema(schema_fields)
    logger.info(f"Created Parquet schema with {len(schema_fields)} fields")
    return schema


def transform_data(dataset_name, ti=None, **kwargs):
    """MWAA compatible data transformation with comprehensive error handling"""
    context = kwargs
    execution_date = context.get('logical_date') or context.get('execution_date') or context.get('ds')
    
    if execution_date is None:
        raise ValueError("execution_date not found in Airflow context")
    if isinstance(execution_date, str):
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

    logger.info(f"Starting MWAA transformation of {dataset_name} for {execution_date}")

    raw_key = None
    if ti:
        raw_key = ti.xcom_pull(task_ids=f"ingest_{dataset_name}", key=f"{dataset_name}_raw_key")

    if not raw_key:
        logger.warning(f"No raw_key found for {dataset_name}, skipping transformation")
        return None

    try:
        logger.info(f"Loading raw data from s3://{S3_CLEANED_BUCKET}/{raw_key}")
        obj = s3_client.get_object(Bucket=S3_CLEANED_BUCKET, Key=raw_key)
        df = pd.read_pickle(io.BytesIO(obj['Body'].read()))
        
        logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns for transformation")

        # Drop extra index columns and reset index
        df = clean_dataframe_for_parquet(df)

        # Validate required columns
        missing_cols = [col for col in BASE_REQUIRED_COLUMNS[dataset_name] if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in {dataset_name} data: {missing_cols}")

        original_rows = len(df)

        if dataset_name == 'songs':
            logger.info("Transforming songs data...")
            df = df.dropna(subset=BASE_REQUIRED_COLUMNS['songs'])
            logger.info(f"After dropping NaN values: {len(df)} rows (removed {original_rows - len(df)})")
            
            # Remove duplicates based on primary key before processing
            df = df.drop_duplicates(subset=['id'], keep='last')
            logger.info(f"After removing duplicates: {len(df)} rows")

            # Ensure popularity is non-nullable int32
            df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce').fillna(0).astype('int32')

            df = df.astype({
                'id': 'int64',
                'duration_ms': 'int32',
                'explicit': 'bool',
                'danceability': 'float64',
                'energy': 'float64',
                'key': 'int32',
                'loudness': 'float64',
                'mode': 'int32',
                'speechiness': 'float64',
                'acousticness': 'float64',
                'instrumentalness': 'float64',
                'liveness': 'float64',
                'valence': 'float64',
                'tempo': 'float64',
                'time_signature': 'int32'
            })

            # Select only the exact columns that match the table schema
            df = df[FINAL_COLUMNS['songs']]
            logger.info(f"Songs final schema: {len(df.columns)} columns")

        elif dataset_name == 'users':
            logger.info("Transforming users data...")
            df = df.dropna(subset=BASE_REQUIRED_COLUMNS['users'])
            logger.info(f"After dropping NaN values: {len(df)} rows (removed {original_rows - len(df)})")
            
            # Remove duplicates based on primary key before processing
            df = df.drop_duplicates(subset=['user_id'], keep='last')
            logger.info(f"After removing duplicates: {len(df)} rows")

            # Force user_age to non-nullable int32 (fill NaNs with 0)
            df['user_age'] = pd.to_numeric(df['user_age'], errors='coerce').fillna(0).astype('int32')

            # Handle timestamps with validation and truncation
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            valid_start = pd.Timestamp('1400-01-01')
            valid_end = pd.Timestamp('9999-12-31 23:59:59.999999')
            mask = (df['created_at'] >= valid_start) & (df['created_at'] <= valid_end)
            df = df[mask].copy()
            logger.info(f"After timestamp validation: {len(df)} rows")
            
            df['created_at'] = df['created_at'].dt.floor('us')
            df['user_tenure_days'] = (pd.Timestamp.now() - df['created_at']).dt.days.astype('int32')

            df = df.astype({
                'user_id': 'int64',
                'user_country': 'string',
                'user_name': 'string'
            })

            # Select only the exact columns that match the table schema
            df = df[FINAL_COLUMNS['users']]
            logger.info(f"Users final schema: {len(df.columns)} columns")

        elif dataset_name == 'streams':
            logger.info("Transforming streams data...")
            df = df.dropna(subset=BASE_REQUIRED_COLUMNS['streams'])
            logger.info(f"After dropping NaN values: {len(df)} rows (removed {original_rows - len(df)})")
            
            # Remove duplicates based on composite primary key before processing
            df = df.drop_duplicates(subset=['user_id', 'track_id', 'listen_time'], keep='last')
            logger.info(f"After removing duplicates: {len(df)} rows")

            df = df.astype({
                'user_id': 'int64'
            })

            df['listen_time'] = pd.to_datetime(df['listen_time'], errors='coerce')
            df['listen_hour'] = df['listen_time'].dt.hour.astype('int32')
            df['listen_day_of_week'] = df['listen_time'].dt.dayofweek.astype('int32')
            df['listen_month'] = df['listen_time'].dt.month.astype('int32')

            # Select only the exact columns that match the table schema
            df = df[FINAL_COLUMNS['streams']]
            logger.info(f"Streams final schema: {len(df.columns)} columns")

        else:
            raise ValueError(f"Unknown dataset: {dataset_name}")

        # Create PyArrow table with explicit schema for Redshift compatibility
        schema = create_parquet_compatible_schema(df, dataset_name)
        
        # Fill any remaining NaN values before creating the table
        for col in df.columns:
            if df[col].dtype in ['int32', 'int64']:
                df[col] = df[col].fillna(0)
            elif df[col].dtype in ['float64']:
                df[col] = df[col].fillna(0.0)
            elif df[col].dtype == 'bool':
                df[col] = df[col].fillna(False)
        
        table = pa.Table.from_pandas(df, schema=schema)
        
        # Save to Parquet in memory
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        # Include timestamp for uniqueness
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{dataset_name}/{dataset_name}_cleaned_{execution_date.strftime('%Y%m%d')}_{timestamp}.parquet"
        
        s3_client.put_object(
            Bucket=S3_CLEANED_BUCKET, 
            Key=s3_key, 
            Body=buf.getvalue(),
            ContentType='application/octet-stream'
        )

        if ti:
            ti.xcom_push(key=f"{dataset_name}_s3_key", value=s3_key)

        logger.info(f"Successfully transformed {dataset_name}: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Saved to s3://{S3_CLEANED_BUCKET}/{s3_key}")

        return s3_key
        
    except Exception as e:
        logger.error(f"Failed to transform {dataset_name} data: {e}")
        raise


def generate_atomic_upsert_sql(table_name: str, s3_path: str) -> str:
    """
    Generate a single atomic upsert SQL statement optimized for MWAA/Redshift.
    This approach avoids the temporary table session issues by using CTEs.
    """
    primary_keys = PRIMARY_KEYS[table_name]
    update_cols = UPDATE_COLUMNS[table_name]
    
    # Get all columns for the dataset
    dataset_name = table_name.replace('_table', '')
    all_columns = FINAL_COLUMNS[dataset_name]
    
    # Create a unique staging table name to avoid conflicts in MWAA
    timestamp = int(time.time())
    staging_table = f"mwaa_staging_{table_name}_{timestamp}"
    
    if update_cols:  # If there are columns to update (songs, users)
        # Join condition for primary keys
        join_conditions = " AND ".join([f"{table_name}.{pk} = {staging_table}.{pk}" for pk in primary_keys])
        
        # Update SET clause
        update_set = ", ".join([f"{col} = {staging_table}.{col}" for col in update_cols])
        
        # Column lists for INSERT
        column_list = ", ".join(all_columns)
        staging_column_list = ", ".join([f"{staging_table}.{col}" for col in all_columns])
        
        atomic_sql = f"""
        BEGIN TRANSACTION;
        
        -- Create staging table and load data in one operation
        CREATE TABLE {staging_table} (LIKE {table_name});
        
        COPY {staging_table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_ROLE}'
        FORMAT AS PARQUET
        COMPUPDATE OFF
        STATUPDATE OFF;
        
        -- Update existing records
        UPDATE {table_name} 
        SET {update_set}
        FROM {staging_table}
        WHERE {join_conditions};
        
        -- Insert new records
        INSERT INTO {table_name} ({column_list})
        SELECT {staging_column_list}
        FROM {staging_table}
        WHERE NOT EXISTS (
            SELECT 1 FROM {table_name} 
            WHERE {" AND ".join([f"{table_name}.{pk} = {staging_table}.{pk}" for pk in primary_keys])}
        );
        
        -- Clean up staging table
        DROP TABLE {staging_table};
        
        COMMIT TRANSACTION;
        """
    else:
        # For tables like streams where we only insert if not exists
        join_conditions = " AND ".join([f"{table_name}.{pk} = {staging_table}.{pk}" for pk in primary_keys])
        column_list = ", ".join(all_columns)
        staging_column_list = ", ".join([f"{staging_table}.{col}" for col in all_columns])
        
        atomic_sql = f"""
        BEGIN TRANSACTION;
        
        -- Create staging table and load data in one operation
        CREATE TABLE {staging_table} (LIKE {table_name});
        
        COPY {staging_table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_ROLE}'
        FORMAT AS PARQUET
        COMPUPDATE OFF
        STATUPDATE OFF;
        
        -- Insert new records only
        INSERT INTO {table_name} ({column_list})
        SELECT {staging_column_list}
        FROM {staging_table}
        WHERE NOT EXISTS (
            SELECT 1 FROM {table_name} 
            WHERE {" AND ".join([f"{table_name}.{pk} = {staging_table}.{pk}" for pk in primary_keys])}
        );
        
        -- Clean up staging table
        DROP TABLE {staging_table};
        
        COMMIT TRANSACTION;
        """
    
    return atomic_sql


def create_table_if_not_exists(table_name: str, create_sql: str):
    """Create table if it doesn't exist with MWAA compatible error handling"""
    try:
        logger.info(f"Ensuring table {table_name} exists...")
        response = redshift_data.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DATABASE,
            Sql=create_sql,
            WithEvent=True
        )
        statement_id = response['Id']
        logger.info(f"CREATE TABLE statement submitted for {table_name}, ID: {statement_id}")

        # Wait for CREATE TABLE to finish before proceeding
        wait_for_copy_completion(statement_id, description=f"CREATE TABLE {table_name}")

        logger.info(f"Table {table_name} is ready")
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        raise

def wait_for_copy_completion(statement_id, timeout=600, description=None):
    """Wait for statement completion with better error handling"""
    elapsed = 0
    interval = 10
    
    if description:  # Log the description if provided
        logger.info(f"Waiting for statement: {description}")
    
    while elapsed < timeout:
        try:
            desc = redshift_data.describe_statement(Id=statement_id)
            status = desc['Status']
            
            if status == 'FINISHED':
                logger.info(f"Statement {statement_id} finished successfully.")
                if 'ResultRows' in desc:
                    logger.info(f"Statement affected {desc['ResultRows']} rows.")
                return desc
            elif status == 'FAILED':
                error = desc.get('Error', 'Unknown error')
                logger.error(f"Statement {statement_id} failed: {error}")
                raise Exception(f"Statement failed: {error}")
            elif status in ['SUBMITTED', 'PICKED', 'STARTED']:
                logger.info(f"Statement {statement_id} status: {status}, waiting...")
            else:
                logger.warning(f"Unexpected status for statement {statement_id}: {status}")
                
        except Exception as e:
            if "Statement not found" in str(e):
                logger.error(f"Statement {statement_id} not found - it may have been cleaned up")
                raise
            else:
                logger.error(f"Error checking statement status: {e}")
                raise
        
        time.sleep(interval)
        elapsed += interval
    
    raise TimeoutError(f"Timed out waiting for statement {statement_id} to finish after {timeout} seconds.")




def execute_sql_statement(sql: str, description: str) -> str:
    """Execute a single SQL statement and return statement ID"""
    try:
        response = redshift_data.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP,
            Database=REDSHIFT_DATABASE,
            Sql=sql,
            WithEvent=True
        )
        statement_id = response['Id']
        logger.info(f"{description}: Statement ID {statement_id}")
        return statement_id
    except Exception as e:
        logger.error(f"Failed to execute {description}: {e}")
        raise


def load_data_to_redshift(dataset_name, ti=None, **kwargs):
    """Load data to Redshift using atomic upsert approach"""
    s3_key = None
    if ti:
        s3_key = ti.xcom_pull(task_ids=f"transform_{dataset_name}", key=f"{dataset_name}_s3_key")

    if not s3_key:
        logger.warning(f"No cleaned S3 key found for {dataset_name}, skipping load to Redshift.")
        return None

    table = REDSHIFT_TABLES[dataset_name]
    create_sql = CREATE_TABLE_QUERIES[table]

    # Create table if not exists and wait for completion
    create_table_if_not_exists(table, create_sql)

    full_s3_path = f"s3://{S3_CLEANED_BUCKET}/{s3_key}"
    
    logger.info(f"Starting atomic upsert process for {dataset_name} from {full_s3_path}")
    
    try:
        # Generate and execute atomic upsert SQL
        atomic_sql = generate_atomic_upsert_sql(table, full_s3_path)
        
        logger.info(f"Executing atomic upsert for {dataset_name}")
        statement_id = execute_sql_statement(atomic_sql, f"Atomic upsert for {dataset_name}")
        
        # Wait for completion with extended timeout for large operations
        result = wait_for_copy_completion(statement_id, timeout=900)  # 15 minutes timeout
        
        logger.info(f"Successfully completed atomic upsert for {dataset_name} data to Redshift table {table}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to execute atomic upsert for {dataset_name}: {e}")
        raise
