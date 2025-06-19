import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node Custom Transform
def MyTransform_user(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import lower, trim, to_date, col
    from pyspark.sql.types import BooleanType

    df_user_viewing = dfc.select(list(dfc.keys())[0])
    df_user_viewing = df_user_viewing.toDF()

    df_user_viewing = df_user_viewing.withColumn('viewed_at', to_date(df_user_viewing['viewed_at'], 'dd/MM/yyyy'))
    df_user_viewing = df_user_viewing.withColumn('is_wishlisted', col('is_wishlisted').cast('boolean'))
    df_user_viewing = df_user_viewing.withColumn('call_to_action', lower(trim(df_user_viewing['call_to_action'])))
    dyf_user_viewing_transformed = DynamicFrame.fromDF(df_user_viewing, glueContext, "dyf_user_viewing_transformed_transformed")

    return DynamicFrameCollection({"user_viewing_transformed": dyf_user_viewing_transformed}, glueContext)
# Script generated for node Transform_attributes
def MyTransform_attributes(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import lower, trim,to_date, col, regexp_replace
    from pyspark.sql.types import BooleanType, DoubleType


    df_apartment_attributes = dfc.select(list(dfc.keys())[0])

    df_apartment_attributes = df_apartment_attributes.toDF()

    df_apartment_attributes = df_apartment_attributes.withColumn(
    'has_photo', col('has_photo').cast(BooleanType())
    )
    df_apartment_attributes = df_apartment_attributes.withColumn(
        'pets_allowed', col('pets_allowed').cast(BooleanType())
    )
    df_apartment_attributes = df_apartment_attributes.withColumn('category', lower(trim(df_apartment_attributes['category'])))
    df_apartment_attributes = df_apartment_attributes.withColumn('amenities', lower(trim(df_apartment_attributes['amenities'])))
    df_apartment_attributes = df_apartment_attributes.withColumn('price_type', lower(trim(df_apartment_attributes['price_type'])))
    df_apartment_attributes = df_apartment_attributes.withColumn('cityname', lower(trim(df_apartment_attributes['cityname'])))
    df_apartment_attributes = df_apartment_attributes.withColumn('state', lower(trim(df_apartment_attributes['state'])))
    df_apartment_attributes = df_apartment_attributes.withColumnRenamed("price_display", "price_display_usd")
    df_apartment_attributes = df_apartment_attributes.withColumn(
    'price_display_usd',
    regexp_replace(col('price_display_usd'), '[^0-9.]', '').cast(DoubleType())
    )
    df_apartment_attributes_cleaned = df_apartment_attributes.dropDuplicates(['id'])


    dyf_apartment_attributes_transformed = DynamicFrame.fromDF(df_apartment_attributes, glueContext, "dyf_apartment_attributes_transformed")

    return DynamicFrameCollection({"apartment_attributes_transformed": dyf_apartment_attributes_transformed}, glueContext)
# Script generated for node Custom Transform_bookings
def MyTransform_bookings(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrameCollection, DynamicFrame
    from pyspark.sql.functions import to_date, lower, trim,when,col
    dyf_bookings = dfc.select(list(dfc.keys())[0])
    df_bookings = dyf_bookings.toDF()

    df_bookings = df_bookings.withColumn('booking_date', to_date(df_bookings['booking_date'], 'dd/MM/yyyy'))
    df_bookings = df_bookings.withColumn('checkin_date', to_date(df_bookings['checkin_date'], 'dd/MM/yyyy'))
    df_bookings = df_bookings.withColumn('checkout_date', to_date(df_bookings['checkout_date'], 'dd/MM/yyyy'))
    df_bookings = df_bookings.withColumn('currency', lower(trim(df_bookings['currency'])))
    df_bookings = df_bookings.withColumn('booking_status', lower(trim(df_bookings['booking_status'])))
    df_bookings = df_bookings.dropDuplicates(['booking_id'])

    exchange_rates = {
        "eur": 1.08,
        "inr": 0.012,
        "usd": 1.0
    }


    df_bookings = df_bookings.withColumn(
        "total_price_usd",
        when(col("currency") == "eur", col("total_price") * exchange_rates["eur"])
        .when(col("currency") == "inr", col("total_price") * exchange_rates["inr"])
        .otherwise(col("total_price") * exchange_rates["usd"]) # Assuming 'usd' is the base case
    )

    dyf_bookings_transformed = DynamicFrame.fromDF(df_bookings, glueContext, "dyf_bookings_transformed")

    return DynamicFrameCollection({"bookings_transformed": dyf_bookings_transformed}, glueContext)
# Script generated for node Custom Transform
def MyTransform_apartments(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import to_date, col, lower, trim, when
    from pyspark.sql.types import BooleanType

    df_apartments = dfc.select(list(dfc.keys())[0])
    df_apartments = df_apartments.toDF()

    # Convert string dates to DateType
    df_apartments = df_apartments.withColumn('listing_created_on', to_date(col('listing_created_on'), 'dd/MM/yyyy'))
    df_apartments = df_apartments.withColumn('last_modified_timestamp', to_date(col('last_modified_timestamp'), 'dd/MM/yyyy'))

    # Normalize string columns
    df_apartments = df_apartments.withColumn('source', lower(trim(col('source'))))
    df_apartments = df_apartments.withColumn('currency', lower(trim(col('currency'))))

    # Convert 'is_active' string ("true"/"false") to boolean
    df_apartments = df_apartments.withColumn(
        'is_active',
        when(lower(trim(col('is_active'))) == 'true', True)
        .when(lower(trim(col('is_active'))) == 'false', False)
        .otherwise(None)
    )

    exchange_rates = {
        "eur": 1.08,
        "inr": 0.012,
        "usd": 1.0
    }

    # Create the 'price_usd' column by converting prices based on currency
    df_apartments = df_apartments.withColumn(
        "price_usd",
        when(col("currency") == "eur", col("price") * exchange_rates["eur"])
        .when(col("currency") == "inr", col("price") * exchange_rates["inr"])
        .otherwise(col("price") * exchange_rates["usd"]) # Assuming 'usd' is the base case
    )


    df_apartments = df_apartments.dropDuplicates(['id'])

    dyf_apartments_transformed = DynamicFrame.fromDF(df_apartments, glueContext, "dyf_apartments_transformed")

    return DynamicFrameCollection({"apartments_transformed": dyf_apartments_transformed}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon Redshift
AmazonRedshift_node1750179055217 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.user_viewing", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750179055217")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750182234929 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartments", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750182234929")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750173585361 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.bookings", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750173585361")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750181048550 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "raw_layer.apartment_attributes", "connectionName": "Redshift connection"}, transformation_ctx="AmazonRedshift_node1750181048550")

# Script generated for node Custom Transform
CustomTransform_node1750179067268 = MyTransform_user(glueContext, DynamicFrameCollection({"AmazonRedshift_node1750179055217": AmazonRedshift_node1750179055217}, glueContext))

# Script generated for node Custom Transform
CustomTransform_node1750182338422 = MyTransform_apartments(glueContext, DynamicFrameCollection({"AmazonRedshift_node1750182234929": AmazonRedshift_node1750182234929}, glueContext))

# Script generated for node Custom Transform_bookings
CustomTransform_bookings_node1750174265841 = MyTransform_bookings(glueContext, DynamicFrameCollection({"AmazonRedshift_node1750173585361": AmazonRedshift_node1750173585361}, glueContext))

# Script generated for node Transform_attributes
Transform_attributes_node1750181058300 = MyTransform_attributes(glueContext, DynamicFrameCollection({"AmazonRedshift_node1750181048550": AmazonRedshift_node1750181048550}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1750179073216 = SelectFromCollection.apply(dfc=CustomTransform_node1750179067268, key=list(CustomTransform_node1750179067268.keys())[0], transformation_ctx="SelectFromCollection_node1750179073216")

# Script generated for node Select From Collection
SelectFromCollection_node1750182374555 = SelectFromCollection.apply(dfc=CustomTransform_node1750182338422, key=list(CustomTransform_node1750182338422.keys())[0], transformation_ctx="SelectFromCollection_node1750182374555")

# Script generated for node Collection_bookings
Collection_bookings_node1750176485245 = SelectFromCollection.apply(dfc=CustomTransform_bookings_node1750174265841, key=list(CustomTransform_bookings_node1750174265841.keys())[0], transformation_ctx="Collection_bookings_node1750176485245")

# Script generated for node Collection
Collection_node1750181065531 = SelectFromCollection.apply(dfc=Transform_attributes_node1750181058300, key=list(Transform_attributes_node1750181058300.keys())[0], transformation_ctx="Collection_node1750181065531")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750179079086 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750179073216, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated_layer.user_viewing", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated_layer.user_viewing (user_id INTEGER, apartment_id INTEGER, viewed_at DATE, is_wishlisted BOOLEAN, call_to_action VARCHAR);"}, transformation_ctx="AmazonRedshift_node1750179079086")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750182385848 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1750182374555, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated_layer.apartments", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated_layer.apartments (id INTEGER, title VARCHAR, source VARCHAR, price DOUBLE PRECISION, currency VARCHAR, listing_created_on DATE, is_active BOOLEAN, last_modified_timestamp DATE, price_usd DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750182385848")

# Script generated for node Redshift_bookings
Redshift_bookings_node1750176594724 = glueContext.write_dynamic_frame.from_options(frame=Collection_bookings_node1750176485245, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated_layer.bookings", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated_layer.bookings (booking_id INTEGER, user_id INTEGER, apartment_id INTEGER, booking_date DATE, checkin_date DATE, checkout_date DATE, total_price DOUBLE PRECISION, currency VARCHAR, booking_status VARCHAR, total_price_usd DOUBLE PRECISION);"}, transformation_ctx="Redshift_bookings_node1750176594724")

# Script generated for node Amazon Redshift
AmazonRedshift_node1750181080515 = glueContext.write_dynamic_frame.from_options(frame=Collection_node1750181065531, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-814724283777-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "curated_layer.apartment_attributes", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS curated_layer.apartment_attributes (id INTEGER, category VARCHAR, body VARCHAR, amenities VARCHAR, bathrooms INTEGER, bedrooms INTEGER, fee DOUBLE PRECISION, has_photo BOOLEAN, pets_allowed BOOLEAN, price_display_usd DOUBLE PRECISION, price_type VARCHAR, square_feet INTEGER, address VARCHAR, cityname VARCHAR, state VARCHAR, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);"}, transformation_ctx="AmazonRedshift_node1750181080515")

job.commit()