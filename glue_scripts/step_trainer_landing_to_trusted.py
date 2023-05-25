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

# Script generated for node Step_trainer Landing
Step_trainerLanding_node1685006842456 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udaglue-practice/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainerLanding_node1685006842456",
)

# Script generated for node Curated customer
Curatedcustomer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udaglue-practice/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Curatedcustomer_node1",
)

# Script generated for node Map users to avoid conflict
Mapuserstoavoidconflict_node1685012123505 = ApplyMapping.apply(
    frame=Step_trainerLanding_node1685006842456,
    mappings=[
        ("sensorReadingTime", "bigint", "`(st) sensorReadingTime`", "long"),
        ("serialNumber", "string", "`(st) serialNumber`", "string"),
        ("distanceFromObject", "int", "`(st) distanceFromObject`", "int"),
    ],
    transformation_ctx="Mapuserstoavoidconflict_node1685012123505",
)

# Script generated for node Map users
Mapusers_node1685007102863 = Join.apply(
    frame1=Curatedcustomer_node1,
    frame2=Mapuserstoavoidconflict_node1685012123505,
    keys1=["serialNumber"],
    keys2=["`(st) serialNumber`"],
    transformation_ctx="Mapusers_node1685007102863",
)

# Script generated for node Drop Fields
DropFields_node1685007187093 = DropFields.apply(
    frame=Mapusers_node1685007102863,
    paths=[
        "serialNumber",
        "z",
        "timeStamp",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "y",
        "x",
        "user",
        "customerName",
        "lastUpdateDate",
        "email",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1685007187093",
)

# Script generated for node Trusted Step Trainer
TrustedStepTrainer_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685007187093,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udaglue-practice/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedStepTrainer_node3",
)

job.commit()
