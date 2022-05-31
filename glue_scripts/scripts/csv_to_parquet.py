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


job_args = getResolvedOptions(
    sys.argv, ["raw_dataset_uri", "target_dataset_uri", "dataset_path"]
)
# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            job_args["raw_dataset_uri"]
        ]
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("nconst", "string", "nconst", "string"),
        ("primaryName", "string", "primaryName", "string"),
        ("birthYear", "choice", "birthYear", "choice"),
        ("deathYear", "choice", "deathYear", "choice"),
        ("primaryProfession", "string", "primaryProfession", "string"),
        ("knownForTitles", "string", "knownForTitles", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": f'{job_args["target_dataset_uri"]}/partitioned/{job_args["dataset_path"]}',
        "partitionKeys": ["birthYear"],
    },
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": f'{job_args["target_dataset_uri"]}/unpartitioned/{job_args["dataset_path"]}'
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
