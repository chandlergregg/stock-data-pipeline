from pyspark.sql import SparkSession

from configreader import ConfigReader

spark = SparkSession.builder.getOrCreate()

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

input_path = f"{mount_base_path}/ingested-data"

common_df = spark.read.parquet(input_path)
common_df = common_df.filter(common_df["partition"] != 'B')

trade = common_df.select("trade_dt", "symbol", "exchange", "event_tm", \
                            "event_seq_nb", "arrival_tm", "trade_pr")
common_df.createOrReplaceTempView('data')

row_ranked_data = spark.sql("""
    SELECT 
        *
        , ROW_NUMBER() over (
            PARTITION BY trade_dt, rec_type, symbol, exchange, event_tm, event_seq_nb 
            ORDER BY arrival_tm desc) as rn
    FROM data
""")
row_ranked_data.createOrReplaceTempView('row_ranked_data')

deduped_df = spark.sql("""
    SELECT
        trade_dt
        , rec_type
        , symbol
        , exchange
        , event_tm
        , event_seq_nb
        , arrival_tm
        , trade_pr
        , bid_pr
        , bid_size
        , ask_pr
        , ask_size
        , partition
    FROM row_ranked_data
    WHERE rn = 1
""")
deduped_df.createOrReplaceTempView('deduped_data')

trade_df = spark.sql("""
   SELECT
        trade_dt
        , rec_type
        , symbol
        , exchange
        , event_tm
        , event_seq_nb
        , arrival_tm
        , trade_pr
        , partition
    FROM deduped_data
    WHERE rec_type = 'T'
""") 
quote_df = spark.sql("""
    SELECT
        trade_dt
        , rec_type
        , symbol
        , exchange
        , event_tm
        , event_seq_nb
        , arrival_tm
        , bid_pr
        , bid_size
        , ask_pr
        , ask_size
        , partition
    FROM deduped_data
    WHERE rec_type = 'Q'
""")

# Write data partitioned by trade_dt and coalesced to 4 partitions per record type
output_path = f"{mount_base_path}/preprocessed-data"
trade_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet(f"{output_path}/trade/")
quote_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet(f"{output_path}/quote/")