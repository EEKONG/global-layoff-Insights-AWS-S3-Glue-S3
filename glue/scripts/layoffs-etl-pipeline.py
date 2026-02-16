import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1770793331804 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://industry-layoffs-data-raw-cleaned-v1/Industry_layoffs_2022_to_2023.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1770793331804")

# Script generated for node total-layoffs-by-company
totallayoffsbycompany_node1770793490790 = sparkAggregate(glueContext, parentFrame = AmazonS3_node1770793331804, groups = ["company"], aggs = [["total_laid_off", "sum"]], transformation_ctx = "totallayoffsbycompany_node1770793490790")

# Script generated for node total-layoffs-by-country
totallayoffsbycountry_node1770793371429 = sparkAggregate(glueContext, parentFrame = AmazonS3_node1770793331804, groups = ["country"], aggs = [["total_laid_off", "sum"]], transformation_ctx = "totallayoffsbycountry_node1770793371429")

# Script generated for node total-layoffs-by-industry
totallayoffsbyindustry_node1770793423793 = sparkAggregate(glueContext, parentFrame = AmazonS3_node1770793331804, groups = ["industry"], aggs = [["total_laid_off", "sum"]], transformation_ctx = "totallayoffsbyindustry_node1770793423793")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=totallayoffsbycompany_node1770793490790, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770793294377", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (totallayoffsbycompany_node1770793490790.count() >= 1):
   totallayoffsbycompany_node1770793490790 = totallayoffsbycompany_node1770793490790.coalesce(1)
AmazonS3_node1770793549323 = glueContext.write_dynamic_frame.from_options(frame=totallayoffsbycompany_node1770793490790, connection_type="s3", format="csv", connection_options={"path": "s3://industry-layoffs-insights-buckets-v1/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1770793549323")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=totallayoffsbycountry_node1770793371429, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770793294377", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (totallayoffsbycountry_node1770793371429.count() >= 1):
   totallayoffsbycountry_node1770793371429 = totallayoffsbycountry_node1770793371429.coalesce(1)
AmazonS3_node1770793608089 = glueContext.write_dynamic_frame.from_options(frame=totallayoffsbycountry_node1770793371429, connection_type="s3", format="csv", connection_options={"path": "s3://industry-layoffs-insights-buckets-v1/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1770793608089")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=totallayoffsbyindustry_node1770793423793, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1770793294377", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (totallayoffsbyindustry_node1770793423793.count() >= 1):
   totallayoffsbyindustry_node1770793423793 = totallayoffsbyindustry_node1770793423793.coalesce(1)
AmazonS3_node1770793580387 = glueContext.write_dynamic_frame.from_options(frame=totallayoffsbyindustry_node1770793423793, connection_type="s3", format="csv", connection_options={"path": "s3://industry-layoffs-insights-buckets-v1/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1770793580387")

job.commit()