from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# Add the dags folder to Python path for imports
sys.path.insert(0, '/usr/local/airflow/dags')

# Import ETL modules (these should be in the same dags folder)
from ETL_s3_rEDShift import (
    ingest_data, 
    transform_data, 
    load_data_to_redshift
)
# Import KPI modules for materialized views
from kPIsandViews import (
    create_and_refresh_kpi_materialized_views,
    get_materialized_view_info
)

# MWAA specific default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2025, 6, 14),
    'catchup': False
}

# Main ETL Pipeline DAG
dag = DAG(
    dag_id='music_etl_pipeline_mwaa',
    default_args=default_args,
    description='Complete ETL pipeline for music streaming data with KPI materialized views - MWAA Compatible',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    max_active_tasks=8,
    tags=['etl', 'music', 'redshift', 'kpis', 'materialized-views', 'mwaa'],
    doc_md="""
    ## Music Streaming ETL Pipeline (MWAA Compatible)
    
    Complete ETL pipeline that processes music streaming data from multiple sources:
    - Ingests data from RDS exports and S3 streaming logs
    - Transforms and cleans data for analytics
    - Loads data into Redshift data warehouse
    - Creates and refreshes KPI materialized views for real-time analytics
    
    **Data Sources:**
    - Songs: RDS export with metadata and audio features
    - Users: RDS export with demographics and profiles
    - Streams: S3 streaming activity logs
    
    **Output:**
    - Redshift tables: songs_table, users_table, streams_table
    - KPI materialized views for business intelligence
    """
)

# =============================================================================
# TASK DEFINITIONS
# =============================================================================

# Pipeline start marker
start = EmptyOperator(
    task_id='start_pipeline',
    dag=dag
)

# =============================================================================
# DATA INGESTION TASKS
# Parallel ingestion from multiple data sources
# =============================================================================

ingest_songs = PythonOperator(
    task_id='ingest_songs',
    python_callable=ingest_data,
    op_kwargs={'dataset_name': 'songs', 'source_bucket': 'rds.data'},
    dag=dag,
    doc_md="Ingest song metadata and audio features from RDS export"
)

ingest_users = PythonOperator(
    task_id='ingest_users',
    python_callable=ingest_data,
    op_kwargs={'dataset_name': 'users', 'source_bucket': 'rds.data'},
    dag=dag,
    doc_md="Ingest user demographics and profile data from RDS export"
)

ingest_streams = PythonOperator(
    task_id='ingest_streams',
    python_callable=ingest_data,
    op_kwargs={'dataset_name': 'streams', 'source_bucket': 's3.streams'},
    dag=dag,
    doc_md="Ingest streaming activity data from S3"
)

# =============================================================================
# DATA TRANSFORMATION TASKS
# Clean and prepare data for Redshift loading
# =============================================================================

transform_songs = PythonOperator(
    task_id='transform_songs',
    python_callable=transform_data,
    op_kwargs={'dataset_name': 'songs'},
    dag=dag,
    doc_md="Clean and transform song data: normalize audio features, validate metadata, handle duplicates"
)

transform_users = PythonOperator(
    task_id='transform_users',
    python_callable=transform_data,
    op_kwargs={'dataset_name': 'users'},
    dag=dag,
    doc_md="Clean and transform user data: standardize demographics, validate profiles, anonymize PII"
)

transform_streams = PythonOperator(
    task_id='transform_streams',
    python_callable=transform_data,
    op_kwargs={'dataset_name': 'streams'},
    dag=dag,
    doc_md="Clean and transform streaming data: parse timestamps, validate user/song IDs, calculate metrics"
)

# =============================================================================
# DATA LOADING TASKS
# Load transformed data into Redshift tables
# =============================================================================

load_songs = PythonOperator(
    task_id='load_songs_to_redshift',
    python_callable=load_data_to_redshift,
    op_kwargs={'dataset_name': 'songs'},
    dag=dag,
    doc_md="Load transformed song data into Redshift songs_table with DISTKEY optimization"
)

load_users = PythonOperator(
    task_id='load_users_to_redshift',
    python_callable=load_data_to_redshift,
    op_kwargs={'dataset_name': 'users'},
    dag=dag,
    doc_md="Load transformed user data into Redshift users_table with DISTKEY optimization"
)

load_streams = PythonOperator(
    task_id='load_streams_to_redshift',
    python_callable=load_data_to_redshift,
    op_kwargs={'dataset_name': 'streams'},
    dag=dag,
    doc_md="Load transformed streaming data into Redshift streams_table with DISTKEY/SORTKEY optimization"
)

# =============================================================================
# KPI MATERIALIZED VIEWS TASKS
# Create business intelligence views for analytics
# =============================================================================

create_refresh_kpi_views = PythonOperator(
    task_id='create_and_refresh_kpi_materialized_views',
    python_callable=create_and_refresh_kpi_materialized_views,
    dag=dag,
    doc_md="""
    **Create and refresh KPI materialized views with Redshift-native optimization:**
    
    **Business Intelligence Views Created:**
    - Genre performance metrics (listen counts, popularity trends)
    - User engagement analytics (listening patterns, retention)
    - Song popularity rankings (play counts, user interactions)
    - Revenue and subscription metrics
    
    **Redshift Optimizations:**
    - DISTKEY configuration for parallel processing
    - SORTKEY optimization for query performance
    - Automatic refresh scheduling for data freshness
    """
)

# Validation task to ensure KPI views were created successfully
validate_kpi_views = PythonOperator(
    task_id='validate_kpi_materialized_views',
    python_callable=get_materialized_view_info,
    dag=dag,
    doc_md="""
    **Validate KPI materialized views creation and optimization:**
    
    **Validation Checks:**
    - Verify view existence using SVV_MATERIALIZED_VIEWS
    - Confirm DISTKEY and SORTKEY configurations are applied
    - Check data population and row counts
    - Validate refresh status and timestamps
    - Log view metadata for monitoring and troubleshooting
    
    **Ensures:**
    - All views are ready for business intelligence queries
    - Optimal performance configuration is in place
    - Data integrity and completeness
    """
)

# Pipeline completion marker
end = EmptyOperator(
    task_id='end_pipeline',
    dag=dag
)

# =============================================================================
# TASK DEPENDENCIES - PIPELINE ORCHESTRATION
# =============================================================================

# Start with parallel data ingestion from all sources
start >> [ingest_songs, ingest_users, ingest_streams]

# Transform each dataset after ingestion (parallel processing)
ingest_songs >> transform_songs >> load_songs
ingest_users >> transform_users >> load_users  
ingest_streams >> transform_streams >> load_streams

# CRITICAL: KPI views require ALL base tables to be loaded
# Wait for all data loading to complete before creating analytics views
[load_songs, load_users, load_streams] >> create_refresh_kpi_views >> validate_kpi_views >> end

# =============================================================================
# DAG EXPORT FOR MWAA
# =============================================================================

# Export the main ETL pipeline DAG for MWAA deployment
globals()['music_etl_pipeline_mwaa'] = dag
