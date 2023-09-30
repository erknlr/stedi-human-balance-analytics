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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1696106817334 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1696106817334",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Customer Curated
CustomerCurated_node1696106814616 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://erkin-stedi/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1696106814616",
)

# Script generated for node Join Customer Curated to Acceloremeter Trusted
JoinCustomerCuratedtoAcceloremeterTrusted_node1696106977360 = Join.apply(
    frame1=CustomerCurated_node1696106814616,
    frame2=AccelerometerTrusted_node1696106817334,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomerCuratedtoAcceloremeterTrusted_node1696106977360",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696107688269 = ApplyMapping.apply(
    frame=JoinCustomerCuratedtoAcceloremeterTrusted_node1696106977360,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "bigint"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "bigint"),
        ("registrationDate", "bigint", "registrationDate", "bigint"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "bigint"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "bigint"),
        ("z", "double", "z", "double"),
        ("timeStamp", "bigint", "timeStamp", "long"),
        ("user", "string", "user", "string"),
        ("y", "double", "y", "double"),
        ("x", "double", "x", "double"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696107688269",
)

# Script generated for node Join Step Trainer Trusted to Customer/Acceloremeter
JoinStepTrainerTrustedtoCustomerAcceloremeter_node1696107613225 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=RenamedkeysforJoin_node1696107688269,
    keys1=["serialNumber", "sensorReadingTime"],
    keys2=["right_serialNumber", "timeStamp"],
    transformation_ctx="JoinStepTrainerTrustedtoCustomerAcceloremeter_node1696107613225",
)

# Script generated for node Drop Fields
DropFields_node1696107977144 = DropFields.apply(
    frame=JoinStepTrainerTrustedtoCustomerAcceloremeter_node1696107613225,
    paths=[
        "right_serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "timeStamp",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "phone",
        "email",
        "registrationDate",
        "customerName",
        "shareWithResearchAsOfDate",
    ],
    transformation_ctx="DropFields_node1696107977144",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696107977144,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://erkin-stedi/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node2",
)

job.commit()
