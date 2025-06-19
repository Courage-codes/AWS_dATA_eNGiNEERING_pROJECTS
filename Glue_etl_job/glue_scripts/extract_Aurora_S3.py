import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Relational DB
RelationalDB_node1750164942401 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartment_attributes",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750164942401"
)

# Script generated for node Relational DB
RelationalDB_node1750163861469 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartments",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750163861469"
)

# Script generated for node Relational DB
RelationalDB_node1750164226087 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "bookings",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750164226087"
)

# Script generated for node Relational DB
RelationalDB_node1750164975557 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "user_viewing",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750164975557"
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750164942401, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750163840689", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750164942401.count() >= 1):
   RelationalDB_node1750164942401 = RelationalDB_node1750164942401.coalesce(1)
AmazonS3_node1750164960699 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750164942401, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aurora.data/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750164960699")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750163861469, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750163840689", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750163861469.count() >= 1):
   RelationalDB_node1750163861469 = RelationalDB_node1750163861469.coalesce(1)
AmazonS3_node1750163866845 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750163861469, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aurora.data/apartments/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750163866845")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750164226087, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750163840689", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750164226087.count() >= 1):
   RelationalDB_node1750164226087 = RelationalDB_node1750164226087.coalesce(1)
AmazonS3_node1750164233076 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750164226087, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aurora.data/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750164233076")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750164975557, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750163840689", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (RelationalDB_node1750164975557.count() >= 1):
   RelationalDB_node1750164975557 = RelationalDB_node1750164975557.coalesce(1)
AmazonS3_node1750164980755 = glueContext.write_dynamic_frame.from_options(frame=RelationalDB_node1750164975557, connection_type="s3", format="glueparquet", connection_options={"path": "s3://aurora.data/user_viewing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750164980755")

job.commit()