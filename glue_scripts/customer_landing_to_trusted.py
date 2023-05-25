import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Custmer landing
Custmerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udaglue-practice/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Custmerlanding_node1",
)

# Script generated for node Filter
Filter_node1684925908965 = Filter.apply(
    frame=Custmerlanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1684925908965",
)

# Script generated for node Customer trusted
Customertrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1684925908965,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udaglue-practice/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Customertrusted_node3",
)

job.commit()
