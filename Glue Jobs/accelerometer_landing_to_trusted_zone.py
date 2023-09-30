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

# Script generated for node Customer Trusted
CustomerTrusted_node1696087556192 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1696087556192",
)

# Script generated for node Accelorometer Landing
AccelorometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelorometerLanding_node1",
)

# Script generated for node Join
Join_node1696087642558 = Join.apply(
    frame1=AccelorometerLanding_node1,
    frame2=CustomerTrusted_node1696087556192,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1696087642558",
)

# Script generated for node Drop Fields
DropFields_node1696100013025 = DropFields.apply(
    frame=Join_node1696087642558,
    paths=[
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
        "customerName",
        "shareWithResearchAsOfDate",
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
    ],
    transformation_ctx="DropFields_node1696100013025",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696100013025,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://erkin-stedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node2",
)

job.commit()
