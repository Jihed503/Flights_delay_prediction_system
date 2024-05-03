import sys
from glue.config.config import ACCESS_KEY_ID, SECRET_ACCESS_KEY

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import SparkConf
    from pyspark.context import SparkContext

    # @params: [JOB_NAME, "ENTRYPOINT", "ENV"]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTRYPOINT", "ENV"])

    ## Access Datalake objects using PySpark
    # conf spark
    conf = (
        SparkConf()
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY_ID)
        .set("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY)
        .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("TRACE")
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    spark = glueContext.spark_session
    job = Job(glueContext)

except ImportError:
    import logging

    spark = ""
    logging.basicConfig(
        format="%(asctime)s\t%(module)s\t%(levelname)s\t%(message)s", level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    logger.warning("Package awsglue not found! Excepted if you run the code locally")
