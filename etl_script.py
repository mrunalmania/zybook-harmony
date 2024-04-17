import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1712594602306 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://zybooks-stream/content.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1712594602306")

# Script generated for node Amazon S3 Completion
AmazonS3Completion_node1708717130990 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://zybooks-stream/prod/completion2024/"], "recurse": True}, transformation_ctx="AmazonS3Completion_node1708717130990")

# Script generated for node Amazon S3 Timespent
AmazonS3Timespent_node1708717102317 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://zybooks-stream/prod/time-spent2024/"], "recurse": True}, transformation_ctx="AmazonS3Timespent_node1708717102317")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1712594676485 = ApplyMapping.apply(frame=AmazonS3_node1712594602306, mappings=[("zybook_code", "string", "right_zybook_code", "string"), ("zybook_id", "string", "right_zybook_id", "string"), ("canonical_section_id", "string", "right_canonical_section_id", "string"), ("chapter_number", "string", "right_chapter_number", "string"), ("section_number", "string", "right_section_number", "string"), ("section_title", "string", "right_section_title", "string"), ("content_resource_id", "string", "right_content_resource_id", "string"), ("caption", "string", "right_caption", "string"), ("content_type", "string", "right_content_type", "string"), ("resource_type", "string", "right_resource_type", "string"), ("ordering", "string", "right_ordering", "string"), ("points_possible", "string", "right_points_possible", "string")], transformation_ctx="RenamedkeysforJoin_node1712594676485")

# Script generated for node Change Schema
ChangeSchema_node1708717151950 = ApplyMapping.apply(frame=AmazonS3Completion_node1708717130990, mappings=[("activity_time", "string", "r_activity_time", "string"), ("crid", "int", "r_crid", "int"), ("csid", "int", "r_csid", "int"), ("part", "int", "r_part", "int"), ("resource_type", "int", "r_resource_type", "int"), ("score", "int", "r_score", "int"), ("user_id", "int", "r_user_id", "int"), ("zybook_id", "int", "r_zybook_id", "int")], transformation_ctx="ChangeSchema_node1708717151950")

# Script generated for node Filter Timespent
FilterTimespent_node1708717258311 = Filter.apply(frame=AmazonS3Timespent_node1708717102317, f=lambda row: (row["resource_type"] == 1), transformation_ctx="FilterTimespent_node1708717258311")

# Script generated for node Filter Completion
FilterCompletion_node1708717354925 = Filter.apply(frame=ChangeSchema_node1708717151950, f=lambda row: (row["r_resource_type"] == 1), transformation_ctx="FilterCompletion_node1708717354925")

# Script generated for node Join
Join_node1708717417704 = Join.apply(frame1=FilterTimespent_node1708717258311, frame2=FilterCompletion_node1708717354925, keys1=["zybook_id", "user_id", "csid", "crid", "part"], keys2=["r_zybook_id", "r_user_id", "r_csid", "r_crid", "r_part"], transformation_ctx="Join_node1708717417704")

# Script generated for node Aggregate_sum_sec
Aggregate_sum_sec_node1708717500175 = sparkAggregate(glueContext, parentFrame = Join_node1708717417704, groups = ["user_id", "csid", "crid", "part", "zybook_id"], aggs = [["seconds", "sum"], ["r_score", "max"]], transformation_ctx = "Aggregate_sum_sec_node1708717500175")

# Script generated for node Final-Join
FinalJoin_node1712594628809 = Join.apply(frame1=Aggregate_sum_sec_node1708717500175, frame2=RenamedkeysforJoin_node1712594676485, keys1=["crid", "zybook_id"], keys2=["right_content_resource_id", "right_zybook_id"], transformation_ctx="FinalJoin_node1712594628809")

# Script generated for node Amazon S3
AmazonS3_node1712594731893 = glueContext.write_dynamic_frame.from_options(frame=FinalJoin_node1712594628809, connection_type="s3", format="csv", connection_options={"path": "s3://athena-output-bucket-zybook-project", "partitionKeys": ["zybook_id"]}, transformation_ctx="AmazonS3_node1712594731893")

job.commit()
