from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, \
    StringType, TimestampType, DecimalType, IntegerType
from parsers import parse_line, parse_csv, parse_json
import glob

spark = SparkSession.builder.master("local[*]").appName('Data ingest').getOrCreate()

# Load all files matching "part-*" at same time
filenames = glob.glob("**/part-*", recursive=True)
filename_string = ",".join(filenames)

# Ingest raw and parse
raw = spark.sparkContext.textFile(filename_string)

## TODO: read schema from JSON
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

df.show(10)

df.write.partitionBy("partition").mode("overwrite").parquet("ingest-data")