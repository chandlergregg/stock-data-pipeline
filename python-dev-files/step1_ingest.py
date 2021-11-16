from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, \
    StringType, TimestampType, DecimalType, IntegerType
from configreader import ConfigReader
from parsers import parse_line

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.addPyFile("parsers.py")

from azure.storage.blob import BlobServiceClient

reader = ConfigReader("config.cfg", "azure-storage")
config = reader.get_config()

# Get Azure storage info from config
storage_acct_name = config["account_name"]
storage_acct_access_key = config["access_key"]
storage_container = config["container_name"]
mount_root = config["mount_root"]

# Set Spark Azure storage account and key
storage_acct_key_str = f"fs.azure.account.key.{storage_acct_name}.blob.core.windows.net"
spark.conf.set(storage_acct_key_str, storage_acct_access_key)

# Set base Spark filepath for container
container_base_path = f"​wasbs://{storage_container}@{storage_acct_name}.blob.core.windows.net"
mount_base_path = f"{mount_root}/{storage_container}"

# Set up container client
blob_service_client = BlobServiceClient(account_url=f"https://{storage_acct_name}.blob.core.windows.net", \
    credential=storage_acct_access_key)
container_client = blob_service_client.get_container_client(storage_container)

input_dir = "data"

# Set filetype
file_type = "txt"
file_suffix = f".{file_type}"
suffix_len = len(file_suffix)

# Get list of file names
blob_list = container_client.list_blobs(name_starts_with=input_dir)
cont_filepaths = [ blob.name for blob in blob_list if blob.name[-suffix_len:] == file_suffix ]

spark_filepath_list = [ f"{mount_base_path}/{file}" for file in cont_filepaths ]
input_path = ",".join(spark_filepath_list)

raw = spark.sparkContext.textFile(spark_filepath_str)
parsed = raw.map(lambda line: parse_line(line))

# Establish common event schema
schema = StructType([ \
    StructField('trade_dt', DateType(), True), \
    StructField('rec_type', StringType(), True), \
    StructField('symbol', StringType(), True), \
    StructField('exchange', StringType(), True), \
    StructField('event_tm', TimestampType(), True), \
    StructField('event_seq_nb', IntegerType(), True), \
    StructField('arrival_tm', TimestampType(), True), \
    StructField('trade_pr', DecimalType(17,14), True), \
    StructField('bid_pr', DecimalType(17,14), True), \
    StructField('bid_size', IntegerType(), True), \
    StructField('ask_pr', DecimalType(17,14), True), \
    StructField('ask_size', IntegerType(), True), \
    StructField('partition', StringType(), True) \
])      

# Create dataframe with parsed data and schema
df = spark.createDataFrame(parsed, schema)

output_path = f"{mount_base_path}/ingested-data"
df.write.partitionBy("partition").mode("overwrite").parquet(output_path)
