from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName('EOD data load').getOrCreate()

common_df = spark.read.parquet("ingest_data/partition=T", "ingest_data/partition=Q")
# trade_common = spark.read.parquet("ingest_data/partition=T")
common_df.printSchema()
common_df.count()

common_df = spark.read.parquet("ingest_data/")
common_df = common_df.filter(common_df["partition"] != 'B')
# trade_common = spark.read.parquet("ingest_data/partition=T")
common_df.printSchema()
common_df.count()

trade = common_df.select("trade_dt", "symbol", "exchange", "event_tm", \
                            "event_seq_nb", "arrival_tm", "trade_pr")
trade.show(5)

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
row_ranked_data.show(5)

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
deduped_df.count()
deduped_df.show(5)
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

trade_df.show(5)
quote_df.show(5)

# Write data partitioned by trade_dt and coalesced to 4 partitions per record type
trade_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet("EOD-load/trade/")
quote_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet("EOD-load/quote/")
