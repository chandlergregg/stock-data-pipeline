from pyspark.sql.types import *
from parsers import parse_line
from datetime import datetime, timedelta

class PipelineStep:

    def __init__(self, name, spark, mount_base_path, input_path, output_path):
        self.name = name
        self.spark = spark
        self.mount_base_path = mount_base_path
        self.input_path = input_path
        self.output_path = output_path

    def run(self):
        pass

class PipelineStep1(PipelineStep):
    
    def __init__(self, name, spark, mount_base_path, input_path, output_path):
        super().__init__(name, spark, mount_base_path, input_path, output_path)
        self.spark.sparkContext.addPyFile("parsers.py")

    def run(self):
        raw = self.spark.sparkContext.textFile(self.input_path)
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
        df = self.spark.createDataFrame(parsed, schema)

        output_path = f"{self.mount_base_path}/ingested-data"
        df.write.partitionBy("partition").mode("overwrite").parquet(output_path)

class PipelineStep2(PipelineStep):
    
    def run(self):

        common_df = self.spark.read.parquet(self.input_path)
        common_df = common_df.filter(common_df["partition"] != 'B')

        trade = common_df.select("trade_dt", "symbol", "exchange", "event_tm", \
                                    "event_seq_nb", "arrival_tm", "trade_pr")
        common_df.createOrReplaceTempView('data')

        row_ranked_data = self.spark.sql("""
            SELECT 
                *
                , ROW_NUMBER() over (
                    PARTITION BY trade_dt, rec_type, symbol, exchange, event_tm, event_seq_nb 
                    ORDER BY arrival_tm desc) as rn
            FROM data
        """)
        row_ranked_data.createOrReplaceTempView('row_ranked_data')

        deduped_df = self.spark.sql("""
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

        trade_df = self.spark.sql("""
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
        quote_df = self.spark.sql("""
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
        trade_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet(f"{self.output_path}/trade/")
        quote_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet(f"{self.output_path}/quote/")

class PipelineStep3(PipelineStep):
    
    def run(self):

        # Set current date, previous date
        date_str = '2020-08-06'
        curr_date = datetime.strptime(date_str, '%Y-%m-%d')
        prev_date = curr_date - timedelta(days = 1)

        # Read trades for current day, previous day
        trade_fpath = f"{self.input_path}/trade/"
        trade_df = self.spark.read.parquet(trade_fpath)
        trade_df_curr = trade_df.filter(trade_df["trade_dt"] == curr_date)
        trade_df_prev = trade_df.filter(trade_df["trade_dt"] == prev_date)

        # Read quotes for current day
        quote_fpath = f"{self.input_path}/quote/"
        quote_df = self.spark.read.parquet(quote_fpath)
        quote_df_curr = quote_df.filter(quote_df["trade_dt"] == curr_date)

        # Create temp views for Spark SQL queries
        trade_df_curr.createOrReplaceTempView("trades_curr")
        trade_df_prev.createOrReplaceTempView("trades_prev")
        quote_df_curr.createOrReplaceTempView("quotes_curr")

        # Calculate trailing 30 minute moving avg trade price and update trade_df
        trade_df_curr_mv_avg = self.spark.sql("""
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

        close_pr_df = self.spark.sql("""
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

        joined_df = self.spark.sql("""
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

        final_df = self.spark.sql("""
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

        final_df.coalesce(4).write.partitionBy("trade_dt").mode("overwrite").parquet(self.output_path)
