# Import pyspark and datetime
from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta

# Build spark session
spark = SparkSession.builder.master('local[*]').appName('Analytical ETL').getOrCreate()

# Set current date, previous date
date_str = '2020-08-06'
curr_date = datetime.strptime(date_str, '%Y-%m-%d')
prev_date = curr_date - timedelta(days = 1)

# Read trades for current day, previous day
trade_fpath = f'EOD-load/trade/'
trade_df = spark.read.parquet(trade_fpath)
trade_df_curr = trade_df.filter(trade_df["trade_dt"] == curr_date)
trade_df_prev = trade_df.filter(trade_df["trade_dt"] == prev_date)

# Read quotes for current day
quote_fpath = f'EOD-load/quote/'
quote_df = spark.read.parquet(quote_fpath)
quote_df_curr = quote_df.filter(quote_df["trade_dt"] == curr_date)

# Show sample of trades and count
trade_df_curr.show(5)
trade_df_curr.count()

# Show sample of trades and count
trade_df_prev.show(5)
trade_df_prev.count()

# Show sample of quotes and count
quote_df_curr.show(5)
quote_df_curr.count()

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
trade_df_curr_mv_avg.show(100)

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
close_pr_df.show(10)

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
joined_df.show(10)

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
final_df.count()

final_df.rdd.getNumPartitions()

final_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet("ETL-output")

spark.sql("""
    SELECT * FROM close_pr
    WHERE symbol in ('SYMA', 'SYMB', 'SYMC')
    ORDER BY symbol, exchange
""").show()