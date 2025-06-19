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

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268510356 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      DATE_TRUNC('week', booking_date) AS week,     AVG(DATEDIFF(day, checkin_date, checkout_date)) AS avg_booking_duration_days FROM curated_layer.bookings WHERE booking_status = 'confirmed'   AND checkout_date > checkin_date   AND checkin_date >= booking_date GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750268510356")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268458492 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT     EXTRACT(year FROM booking_date) as booking_year,     EXTRACT(month FROM booking_date) as booking_month,     SUM(DATEDIFF(day, checkin_date, checkout_date)) as total_booked_nights FROM     curated_layer.bookings WHERE     booking_status = 'confirmed' AND checkout_date > checkin_date GROUP BY     EXTRACT(year FROM booking_date),     EXTRACT(month FROM booking_date) ORDER BY     booking_year, booking_month", "redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750268458492")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268486674 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      DATE_TRUNC('week', booking_date) AS week,     apartment_id,     SUM(total_price) AS total_revenue FROM curated_layer.bookings WHERE booking_status = 'confirmed' GROUP BY 1, apartment_id ORDER BY week, total_revenue DESC", "redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750268486674")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268424585 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      DATE_TRUNC('week', listing_created_on) AS week,     AVG(price) AS average_listing_price FROM curated_layer.apartments WHERE is_active = true GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750268424585")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268502818 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      DATE_TRUNC('week', booking_date) AS week,     user_id,     COUNT(*) AS total_bookings FROM curated_layer.bookings GROUP BY 1, user_id ORDER BY 1, total_bookings DESC", "redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750268502818")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268474730 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "SELECT      DATE_TRUNC('week', b.booking_date) AS week,     a.cityname,     COUNT(*) AS bookings_count FROM curated_layer.bookings b JOIN curated_layer.apartment_attributes a ON b.apartment_id = a.id WHERE b.booking_status = 'confirmed' GROUP BY 1, 2 ORDER BY 1, bookings_count DESC", "redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750268474730")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268530619 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": "WITH user_bookings AS (     SELECT          user_id,         booking_date     FROM curated_layer.bookings     WHERE booking_status = 'confirmed' ), user_booking_windows AS (     SELECT          ub1.user_id,         ub1.booking_date,         COUNT(ub2.booking_date) AS bookings_in_last_30_days     FROM user_bookings ub1     JOIN user_bookings ub2       ON ub1.user_id = ub2.user_id      AND ub2.booking_date BETWEEN ub1.booking_date - INTERVAL '30 day' AND ub1.booking_date     GROUP BY ub1.user_id, ub1.booking_date ), weekly_repeat_customers AS (     SELECT          DATE_TRUNC('week', booking_date) AS week,         user_id,         MAX(bookings_in_last_30_days) AS max_bookings_in_30d     FROM user_booking_windows     GROUP BY 1, user_id ) SELECT      week,     COUNT(DISTINCT CASE WHEN max_bookings_in_30d > 1 THEN user_id END) * 100.0 / NULLIF(COUNT(DISTINCT user_id), 0) AS repeat_customer_rate_percentage FROM weekly_repeat_customers GROUP BY 1 ORDER BY 1", "redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750268530619")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268514388 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750268510356, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation_layer.avg_booking_duration_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation_layer.avg_booking_duration_weekly (week TIMESTAMP, avg_booking_duration_days BIGINT);"}, transformation_ctx="AmazonRedshift_node1750268514388")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268468629 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750268458492, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation_layer.booked_nights_by_booking_month", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation_layer.booked_nights_by_booking_month (booking_year INTEGER, booking_month INTEGER, total_booked_nights BIGINT);"}, transformation_ctx="AmazonRedshift_node1750268468629")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268493998 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750268486674, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation_layer.top_performing_listings_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation_layer.top_performing_listings_weekly (week TIMESTAMP, apartment_id INTEGER, total_revenue DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750268493998")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268443194 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750268424585, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation_layer.avg_listing_price_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation_layer.avg_listing_price_weekly (week TIMESTAMP, average_listing_price DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750268443194")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268506337 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750268502818, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation_layer.total_bookings_per_user_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation_layer.total_bookings_per_user_weekly (week TIMESTAMP, user_id INTEGER, total_bookings BIGINT);"}, transformation_ctx="AmazonRedshift_node1750268506337")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268479153 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750268474730, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation_layer.most_popular_locations_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation_layer.most_popular_locations_weekly (week TIMESTAMP, cityname VARCHAR, bookings_count BIGINT);"}, transformation_ctx="AmazonRedshift_node1750268479153")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750268534399 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1750268530619, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "presentation_layer.repeat_customer_rate_weekly", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS presentation_layer.repeat_customer_rate_weekly (week TIMESTAMP, repeat_customer_rate_percentage DECIMAL);"}, transformation_ctx="AmazonRedshift_node1750268534399")

job.commit()