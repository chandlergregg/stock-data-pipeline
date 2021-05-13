from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta

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

# Set current date, previous date
date_str = '2020-08-06'
curr_date = datetime.strptime(date_str, '%Y-%m-%d')
prev_date = curr_date - timedelta(days = 1)

input_path = f"{mount_base_path}/preprocessed-data"

# Read trades for current day, previous day
trade_fpath = f"{input_path}/trade/"
trade_df = spark.read.parquet(trade_fpath)
trade_df_curr = trade_df.filter(trade_df["trade_dt"] == curr_date)
trade_df_prev = trade_df.filter(trade_df["trade_dt"] == prev_date)

# Read quotes for current day
quote_fpath = f"{input_path}/quote/"
quote_df = spark.read.parquet(quote_fpath)
quote_df_curr = quote_df.filter(quote_df["trade_dt"] == curr_date)

# Create temp views for Spark SQL queries
trade_df_curr.createOrReplaceTempView("trades_curr")
trade_df_prev.createOrReplaceTempView("trades_prev")
quote_df_curr.createOrReplaceTempView("quotes_curr")

# Calculate trailing 30 minute moving avg trade price and update trade_df
trade_df_curr_mv_avg = spark.sql("""
    SELECT
        a.symbol
        , a.exchange
        , a.event_tm
        , a.event_seq_nb
        , a.trade_pr
        , AVG(b.trade_pr) as mov_avg_trade_pr
    FROM trades_curr a
    LEFT JOIN trades_curr b
        ON a.symbol = b.symbol
        AND a.exchange = b.exchange
        AND b.event_tm BETWEEN (a.event_tm - INTERVAL 30 MINUTES) AND a.event_tm
    GROUP BY 1,2,3,4,5
    ORDER BY event_tm ASC
""")
trade_df_curr_mv_avg.createOrReplaceTempView("trades_curr_mv_avg")

close_pr_df = spark.sql("""
    WITH ranked_cte as (
        SELECT 
            symbol
            , exchange
            , trade_pr
            , ROW_NUMBER() OVER (PARTITION BY symbol, exchange ORDER BY event_tm DESC) as rn
        FROM trades_prev
    )
    SELECT
        symbol
        , exchange
        , trade_pr as close_pr
    FROM ranked_cte
    WHERE rn = 1
""")
close_pr_df.createOrReplaceTempView("close_pr")

joined_df = spark.sql("""
        SELECT
            q.rec_type
            , q.trade_dt
            , q.symbol
            , q.exchange
            , q.event_tm
            , q.event_seq_nb
            , q.bid_pr
            , q.bid_size
            , q.ask_pr
            , q.ask_size
            , t.trade_pr
            , t.mov_avg_trade_pr
            , ROW_NUMBER() OVER (PARTITION BY q.symbol, q.exchange, q.event_tm ORDER BY t.event_tm DESC) as rn
        FROM quotes_curr q
        LEFT JOIN trades_curr_mv_avg t
            ON q.symbol = t.symbol
            AND q.exchange = t.exchange
            AND t.event_tm < q.event_tm
""")
joined_df.createOrReplaceTempView("joined")

final_df = spark.sql("""
    SELECT
        j.trade_dt
        , j.symbol
        , j.exchange
        , j.event_tm
        , j.event_seq_nb
        , j.bid_pr
        , j.bid_size
        , j.ask_pr
        , j.ask_size
        , j.trade_pr AS last_trade_pr
        , j.mov_avg_trade_pr AS last_mov_avg_pr
        , j.bid_pr - c.close_pr AS bid_pr_mv
        , j.ask_pr - c.close_pr AS ask_pr_mv
    FROM joined j
    LEFT JOIN close_pr c
        ON j.symbol = c.symbol
        AND j.exchange = c.exchange
    WHERE j.rn = 1
        AND j.rec_type = 'Q'
""")

final_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet(output_path)
