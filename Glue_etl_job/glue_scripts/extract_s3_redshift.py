import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1750253681672 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora.data/apartment_attributes/run-1750251729289-part-block-0-r-00000-snappy.parquet"]}, transformation_ctx="AmazonS3_node1750253681672")

# Script generated for node Amazon S3
AmazonS3_node1750253717441 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora.data/bookings/run-1750251876640-part-block-0-r-00000-snappy.parquet"]}, transformation_ctx="AmazonS3_node1750253717441")

# Script generated for node Amazon S3
AmazonS3_node1750253731145 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora.data/apartments/run-1750251801282-part-block-0-r-00000-snappy.parquet"]}, transformation_ctx="AmazonS3_node1750253731145")

# Script generated for node Amazon S3
AmazonS3_node1750253743274 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://aurora.data/user_viewing/run-1750251951836-part-block-0-r-00000-snappy.parquet"]}, transformation_ctx="AmazonS3_node1750253743274")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750253706120 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750253681672, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DOUBLE PRECISION, has_photo VARCHAR, pets_allowed VARCHAR, price_display VARCHAR, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750253706120")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750253722143 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750253717441, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date VARCHAR, checkin_date VARCHAR, checkout_date VARCHAR, total_price DOUBLE PRECISION, currency VARCHAR, booking_status VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750253722143")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750253735499 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750253731145, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DOUBLE PRECISION, currency VARCHAR, listing_created_on VARCHAR, is_active VARCHAR, last_modified_timestamp VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750253735499")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750253750221 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1750253743274, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.user_viewing", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS raw_layer.user_viewing (user_id INTEGER, apartment_id INTEGER, viewed_at VARCHAR, is_wishlisted VARCHAR, call_to_action VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750253750221")

job.commit()