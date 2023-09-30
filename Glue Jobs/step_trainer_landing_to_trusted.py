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

# Script generated for node Trainer Landing
TrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="TrainerLanding_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1696104216257 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1696104216257",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696104430344 = ApplyMapping.apply(
    frame=CustomerCurated_node1696104216257,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "long", "registrationDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "long", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696104430344",
)

# Script generated for node Join Trainer Landing to Customer Curated
JoinTrainerLandingtoCustomerCurated_node1696104241448 = Join.apply(
    frame1=RenamedkeysforJoin_node1696104430344,
    frame2=TrainerLanding_node1,
    keys1=["right_serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinTrainerLandingtoCustomerCurated_node1696104241448",
)

# Script generated for node Drop Fields
DropFields_node1696104538798 = DropFields.apply(
    frame=JoinTrainerLandingtoCustomerCurated_node1696104241448,
    paths=[
        "right_serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "phone",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1696104538798",
)

# Script generated for node Trainer Trusted
TrainerTrusted_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696104538798,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://erkin-stedi/step_trainer/trusted/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="TrainerTrusted_node2",
)

job.commit()
