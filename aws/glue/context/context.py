import sys
from glue.config.config import ACCESS_KEY_ID, SECRET_ACCESS_KEY
from pyspark.sql import SparkSession

try:
    # AWS Glue and PySpark specific imports
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import SparkConf
    from pyspark.context import SparkContext

    # Parameters used to initialize the job, received from command line arguments
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTRYPOINT", "ENV"])

    # Configuration of the Spark context for using AWS S3 with appropriate access rights and legacy parquet settings
    conf = (
        SparkConf()
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY_ID)
        .set("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY)
        .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    
    # Initializing Spark and Glue contexts with logging settings
    sc = SparkContext(conf=conf)
    sc.setLogLevel("TRACE")
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    spark = glueContext.spark_session.builder.getOrCreate()
 
    job = Job(glueContext)

except ImportError:
    # Fallback for local execution where AWS Glue libraries are not available
    spark = SparkSession.builder \
    .appName("Getting DataFrame Shape") \
    .getOrCreate()
 
    logging.basicConfig(
        format="%(asctime)s\t%(module)s\t%(levelname)s\t%(message)s", level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    logger.warning("Package awsglue not found! Expected if you run the code locally.")
