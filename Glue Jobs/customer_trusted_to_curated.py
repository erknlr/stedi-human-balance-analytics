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
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1696102600446 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1696102600446",
)

# Script generated for node Join Customer Trusted to Accelerometer Landing
JoinCustomerTrustedtoAccelerometerLanding_node1696102648770 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerLanding_node1696102600446,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomerTrustedtoAccelerometerLanding_node1696102648770",
)

# Script generated for node Drop Fields
DropFields_node1696102710155 = DropFields.apply(
    frame=JoinCustomerTrustedtoAccelerometerLanding_node1696102648770,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1696102710155",
)

# Script generated for node Customer Curated
CustomerCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696102710155,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://erkin-stedi/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node2",
)

job.commit()
