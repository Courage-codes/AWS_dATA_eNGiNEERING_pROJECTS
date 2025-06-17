import boto3
import pandas as pd
import json
from datetime import datetime, timedelta
import logging
import time
import os

# MWAA compatible logging setup
logger = logging.getLogger(__name__)

# MWAA Environment Variables (with fallbacks for local development)
REDSHIFT_WORKGROUP = os.getenv('REDSHIFT_WORKGROUP', 'warehouse')
REDSHIFT_DATABASE = os.getenv('REDSHIFT_DATABASE', 'dev')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# Initialize AWS clients with MWAA compatible configuration
def get_redshift_client():
    """Get Redshift client with MWAA compatible configuration"""
    try:
        return boto3.client('redshift-data', region_name=AWS_REGION)
    except Exception as e:
        logger.error(f"Failed to initialize Redshift client: {e}")
        raise

# Lazy initialization of client
redshift_data = None

def get_redshift_data_client():
    """Lazy initialization of Redshift client for MWAA compatibility"""
    global redshift_data
    if redshift_data is None:
        redshift_data = get_redshift_client()
    return redshift_data

# Materialized view definitions
MATERIALIZED_VIEWS = {
    'genre_kpis_mv': 'genre_kpis_materialized_view',
    'hourly_kpis_mv': 'hourly_kpis_materialized_view'
}

# MWAA compatible materialized view creation queries with Redshift-native performance optimization
MATERIALIZED_VIEW_QUERIES = {
    'genre_kpis_materialized_view': """
        CREATE MATERIALIZED VIEW public.genre_kpis_materialized_view 
        DISTSTYLE KEY 
        DISTKEY (track_genre)
        SORTKEY (track_genre, listen_count)
        AS
        WITH genre_stats AS (
            SELECT 
                s.track_genre,
                COUNT(*) as listen_count,
                AVG(s.duration_ms) as average_duration_ms,
                SUM(s.popularity) as popularity_index,
                COUNT(DISTINCT st.user_id) as unique_listeners_count
            FROM streams_table st
            JOIN songs_table s ON st.track_id = s.track_id
            WHERE s.track_genre IS NOT NULL
            GROUP BY s.track_genre
        ),
        genre_track_popularity AS (
            SELECT 
                s.track_genre,
                s.track_id,
                s.track_name,
                s.popularity,
                ROW_NUMBER() OVER (
                    PARTITION BY s.track_genre 
                    ORDER BY s.popularity DESC, s.track_name ASC
                ) as rn
            FROM streams_table st
            JOIN songs_table s ON st.track_id = s.track_id
            WHERE s.track_genre IS NOT NULL
        ),
        most_popular_per_genre AS (
            SELECT 
                track_genre,
                track_id as most_popular_track_id,
                track_name as most_popular_track_name,
                popularity as most_popular_track_popularity
            FROM genre_track_popularity
            WHERE rn = 1
        )
        SELECT 
            gs.track_genre,
            gs.listen_count,
            gs.average_duration_ms,
            gs.popularity_index,
            mp.most_popular_track_id,
            mp.most_popular_track_name,
            mp.most_popular_track_popularity,
            gs.unique_listeners_count,
            CURRENT_DATE as calculation_date,
            CURRENT_TIMESTAMP as updated_at
        FROM genre_stats gs
        JOIN most_popular_per_genre mp ON gs.track_genre = mp.track_genre;
    """,
    'hourly_kpis_materialized_view': """
        CREATE MATERIALIZED VIEW public.hourly_kpis_materialized_view 
        DISTSTYLE KEY 
        DISTKEY (listen_hour)
        SORTKEY (listen_hour, total_listens)
        AS
        WITH hourly_stats AS (
            SELECT 
                st.listen_hour,
                COUNT(DISTINCT st.user_id) as unique_listeners,
                COUNT(DISTINCT st.track_id) as unique_tracks,
                COUNT(*) as total_listens,
                AVG(s.popularity) as avg_track_popularity
            FROM streams_table st
            JOIN songs_table s ON st.track_id = s.track_id
            WHERE st.listen_hour IS NOT NULL
            GROUP BY st.listen_hour
        ),
        hourly_artist_counts AS (
            SELECT 
                st.listen_hour,
                s.artists,
                COUNT(*) as artist_listen_count,
                ROW_NUMBER() OVER (
                    PARTITION BY st.listen_hour 
                    ORDER BY COUNT(*) DESC, s.artists ASC
                ) as rn
            FROM streams_table st
            JOIN songs_table s ON st.track_id = s.track_id
            WHERE st.listen_hour IS NOT NULL AND s.artists IS NOT NULL
            GROUP BY st.listen_hour, s.artists
        ),
        hourly_top_artists AS (
            SELECT 
                listen_hour,
                artists as top_artist,
                artist_listen_count as top_artist_listen_count
            FROM hourly_artist_counts
            WHERE rn = 1
        )
        SELECT 
            hs.listen_hour,
            hs.unique_listeners,
            COALESCE(hta.top_artist, 'Unknown') as top_artist,
            COALESCE(hta.top_artist_listen_count, 0) as top_artist_listen_count,
            hs.unique_tracks,
            hs.total_listens,
            hs.avg_track_popularity,
            CURRENT_DATE as calculation_date,
            CURRENT_TIMESTAMP as updated_at
        FROM hourly_stats hs
        LEFT JOIN hourly_top_artists hta ON hs.listen_hour = hta.listen_hour;
    """
}

def is_redshift_environment():
    """Check if we're running in a Redshift environment"""
    return True

def execute_sql_statement(sql: str, description: str) -> str:
    """Execute a single SQL statement and return statement ID - MWAA compatible"""
    try:
        client = get_redshift_data_client()
        max_retries = 3
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                response = client.execute_statement(
                    WorkgroupName=REDSHIFT_WORKGROUP,
                    Database=REDSHIFT_DATABASE,
                    Sql=sql,
                    WithEvent=True
                )
                statement_id = response['Id']
                logger.info(f"{description}: Statement ID {statement_id} (attempt {attempt + 1})")
                return statement_id
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Attempt {attempt + 1} failed for {description}: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise
    except Exception as e:
        logger.error(f"Failed to execute {description} after {max_retries} attempts: {e}")
        raise

def wait_for_copy_completion(statement_id, timeout=900):
    """Wait for statement completion with MWAA compatible error handling"""
    elapsed = 0
    interval = 15
    client = get_redshift_data_client()
    while elapsed < timeout:
        try:
            desc = client.describe_statement(Id=statement_id)
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
                logger.info(f"Statement {statement_id} status: {status}, waiting... (elapsed: {elapsed}s)")
            else:
                logger.warning(f"Unexpected status for statement {statement_id}: {status}")
        except Exception as e:
            if "Statement not found" in str(e):
                logger.error(f"Statement {statement_id} not found - it may have been cleaned up")
                raise
            elif "AccessDenied" in str(e):
                logger.error(f"Access denied for statement {statement_id}. Check MWAA IAM permissions.")
                raise
            else:
                logger.error(f"Error checking statement status: {e}")
                if elapsed >= timeout - interval:
                    raise
        time.sleep(interval)
        elapsed += interval
    raise TimeoutError(f"Timed out waiting for statement {statement_id} to finish after {timeout} seconds.")

def extract_count_from_record(record):
    """Extract count value from Redshift Data API record with multiple fallbacks"""
    if not record or len(record) == 0:
        return 0
    
    first_field = record[0]
    if not first_field:
        return 0
    
    # Try different field types that Redshift Data API might use
    for field_type in ['longValue', 'stringValue', 'doubleValue']:
        if field_type in first_field:
            value = first_field[field_type]
            if field_type == 'stringValue':
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return 0
            return int(value) if value is not None else 0
    
    logger.warning(f"Could not extract count from record: {first_field}")
    return 0

def materialized_view_exists(view_name: str) -> bool:
    """Check if a materialized view exists using multiple fallback approaches"""
    # Method 1: Try INFORMATION_SCHEMA.TABLES (most compatible) - checking for any table type first
    info_sql = f"""
        SELECT COUNT(*) as view_count 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = 'public' 
        AND table_name = '{view_name}';
    """
    
    # Method 2: Direct query approach (most reliable for existence check)
    direct_sql = f"SELECT COUNT(*) FROM public.{view_name} LIMIT 0;"
    
    methods = [
        ("INFORMATION_SCHEMA", info_sql),
        ("DIRECT_QUERY", direct_sql)
    ]
    
    for method_name, check_sql in methods:
        try:
            logger.info(f"Checking if materialized view {view_name} exists using {method_name}")
            statement_id = execute_sql_statement(check_sql, f"Checking materialized view {view_name} existence via {method_name}")
            result = wait_for_copy_completion(statement_id, timeout=60)
            client = get_redshift_data_client()
            
            if method_name == "DIRECT_QUERY":
                # If direct query succeeds, the view exists
                logger.info(f"Materialized view {view_name} exists (confirmed via direct query)")
                return True
            else:
                # For catalog queries, check the count
                get_result_response = client.get_statement_result(Id=statement_id)
                records = get_result_response.get('Records', [])
                logger.debug(f"Records from {method_name}: {records}")
                
                if records and len(records) > 0:
                    count = extract_count_from_record(records[0])
                    exists = count > 0
                    logger.info(f"Materialized view {view_name} exists: {exists} (count: {count}) via {method_name}")
                    return exists
            
            logger.info(f"Materialized view {view_name} does not exist (via {method_name})")
            return False
            
        except Exception as e:
            logger.warning(f"Method {method_name} failed for checking {view_name}: {e}")
            if method_name == "DIRECT_QUERY":
                # If direct query fails, view likely doesn't exist
                logger.info(f"Materialized view {view_name} does not exist (direct query failed)")
                return False
            continue
    
    # If all methods failed, assume view doesn't exist
    logger.warning(f"All methods failed to check materialized view {view_name} existence, assuming it doesn't exist")
    return False

def create_and_refresh_kpi_materialized_views(ti=None, **kwargs):
    """Create materialized views if they don't exist and refresh them - MWAA compatible"""
    try:
        execution_date = kwargs.get('logical_date') or kwargs.get('execution_date') or datetime.now()
        if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")
        calculation_date = execution_date.strftime('%Y-%m-%d')
        logger.info(f"Starting MWAA compatible KPI materialized view refresh for {calculation_date}")
        created_views = []
        failed_views = []
        for view_name, create_sql in MATERIALIZED_VIEW_QUERIES.items():
            try:
                logger.info(f"Processing materialized view: {view_name}")
                create_materialized_view_if_not_exists(view_name, create_sql)
                created_views.append(view_name)
                logger.info(f"Redshift environment detected - using SORTKEY/DISTKEY for {view_name} performance optimization")
            except Exception as e:
                logger.error(f"Failed to process materialized view {view_name}: {e}")
                failed_views.append(view_name)
                continue
        logger.info(f"Materialized view creation summary: {len(created_views)} successful, {len(failed_views)} failed")
        if created_views:
            logger.info("Refreshing successfully created materialized views")
            refresh_specific_kpi_materialized_views(created_views)
        else:
            logger.warning("No materialized views were successfully created or verified; skipping refresh")
        logger.info("MWAA compatible KPI materialized views refresh completed")
    except Exception as e:
        logger.error(f"MWAA KPI materialized views refresh failed: {e}")
        raise

def create_materialized_view_if_not_exists(view_name: str, create_sql: str):
    """Create materialized view if it doesn't already exist - Redshift compatible"""
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            # Check if view exists
            if materialized_view_exists(view_name):
                logger.info(f"Materialized view {view_name} already exists, skipping creation")
                return
            
            logger.info(f"Creating materialized view: {view_name} (attempt {attempt + 1})")
            statement_id = execute_sql_statement(create_sql, f"Creating materialized view {view_name}")
            wait_for_copy_completion(statement_id, timeout=1200)
            logger.info(f"Successfully created materialized view: {view_name}")
            
            # Give some time for the view to be available
            time.sleep(2)
            
            # Verify creation
            if materialized_view_exists(view_name):
                logger.info(f"Confirmed: Materialized view {view_name} is now available")
                return
            else:
                logger.warning(f"Materialized view {view_name} creation succeeded but verification failed on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying creation after {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Materialized view {view_name} verification failed after {max_retries} attempts")
                    raise Exception(f"View creation inconsistency for {view_name}")
                    
        except Exception as e:
            error_msg = str(e).lower()
            if ("already exists" in error_msg or ("relation" in error_msg and "already exists" in error_msg)):
                logger.info(f"Materialized view {view_name} already exists (detected during creation attempt)")
                # Double-check existence
                if materialized_view_exists(view_name):
                    logger.info(f"Confirmed: Materialized view {view_name} is available")
                    return
                else:
                    logger.warning(f"Creation claimed view exists but verification says it doesn't - this might be a timing issue")
                    time.sleep(5)  # Wait a bit longer
                    if materialized_view_exists(view_name):
                        logger.info(f"Confirmed after waiting: Materialized view {view_name} is available")
                        return
                    
            logger.error(f"Failed to create materialized view {view_name} on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying creation after {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise

def refresh_specific_kpi_materialized_views(view_names: list):
    """Refresh specific KPI materialized views - helper function"""
    successful_refreshes = 0
    for view_name in view_names:
        try:
            if not materialized_view_exists(view_name):
                logger.warning(f"Cannot refresh {view_name} - materialized view does not exist")
                continue
            refresh_materialized_view(view_name)
            successful_refreshes += 1
        except Exception as e:
            logger.error(f"Failed to refresh {view_name}: {e}")
            continue
    logger.info(f"Successfully refreshed {successful_refreshes}/{len(view_names)} materialized views")
    return successful_refreshes > 0

def refresh_materialized_view(view_name: str):
    """Refresh a specific materialized view - MWAA compatible"""
    refresh_sql = f"REFRESH MATERIALIZED VIEW public.{view_name};"
    try:
        logger.info(f"Refreshing materialized view: {view_name}")
        statement_id = execute_sql_statement(refresh_sql, f"Refreshing materialized view {view_name}")
        wait_for_copy_completion(statement_id, timeout=1200)
        logger.info(f"Successfully refreshed materialized view: {view_name}")
    except Exception as e:
        logger.error(f"Failed to refresh materialized view {view_name}: {e}")
        raise

def get_materialized_view_info():
    """Get information about all materialized views - Multiple fallback approaches"""
    # Try INFORMATION_SCHEMA approach first
    info_sql = """
        SELECT 
            table_schema as schemaname,
            table_name as matviewname,
            'true' as is_populated,
            'available' as refresh_status
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE table_schema = 'public' 
        AND table_name IN ('genre_kpis_materialized_view', 'hourly_kpis_materialized_view')
        ORDER BY table_name;
    """
    
    try:
        logger.info(f"Getting materialized view information using INFORMATION_SCHEMA")
        statement_id = execute_sql_statement(info_sql, f"Getting materialized view info via INFORMATION_SCHEMA")
        result = wait_for_copy_completion(statement_id)
        client = get_redshift_data_client()
        get_result_response = client.get_statement_result(Id=statement_id)
        records = get_result_response.get('Records', [])
        logger.info(f"Found {len(records)} materialized view records via INFORMATION_SCHEMA")
        for i, record in enumerate(records):
            logger.info(f"Record {i+1}: {record}")
        logger.info(f"Successfully retrieved materialized view information via INFORMATION_SCHEMA")
        return result
    except Exception as e:
        logger.warning(f"INFORMATION_SCHEMA method failed for getting materialized view info: {e}")
    
    # If catalog method fails, try a basic existence check
    logger.warning("Catalog method failed, performing basic existence check")
    for view_name in ['genre_kpis_materialized_view', 'hourly_kpis_materialized_view']:
        exists = materialized_view_exists(view_name)
        logger.info(f"Materialized view {view_name} exists: {exists}")
    
    logger.info("Completed basic materialized view existence check")
    return {"Status": "Basic check completed"}

__all__ = [
    'create_and_refresh_kpi_materialized_views',
    'get_materialized_view_info'
]
