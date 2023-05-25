import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step_trainer Trusted
Step_trainerTrusted_node1685006842456 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udaglue-practice/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainerTrusted_node1685006842456",
)

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udaglue-practice/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_trusted_node1",
)

# Script generated for node Map users
Mapusers_node1685007102863 = Join.apply(
    frame1=Accelerometer_trusted_node1,
    frame2=Step_trainerTrusted_node1685006842456,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Mapusers_node1685007102863",
)

# Script generated for node Drop Fields
DropFields_node1685007187093 = DropFields.apply(
    frame=Mapusers_node1685007102863,
    paths=["timeStamp"],
    transformation_ctx="DropFields_node1685007187093",
)

# Script generated for node Step_trainer ML
Step_trainerML_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685007187093,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udaglue-practice/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Step_trainerML_node3",
)

job.commit()
