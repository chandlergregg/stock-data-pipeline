 import datetime
 import psycopg2

 class Tracker(object):
     """
     job_id, status, updated_time
     """
     def __init__(self, jobname, dbconfig):
         self.jobname = jobname
         self.dbconfig = dbconfig

    def assign_job_id(self):
        # [Construct the job ID and assign to the return variable]
        return job_id

    def update_job_status(self, status):

        job_id = self.assign_job_id()
        print(f"Job ID assigned: {job_id}")
        update_time = datetime.datetime.now()
        table_name = self.dbconfig.get("postgres", "job_tracker_table_name")
        connection = self.get_db_connection()

        try:
            # [ Execute the SQL statement to insert to job status table ]
        except (Exception, psycopg2.Error) as error:
            print("Error executing DB statement for job tracker.")
        return

    def get_job status(self, job_id)
        # Connect db and send SQL query
        table_name = self.dbconfig.get("postgres", "job_tracker_table_name")
        connection = self.get_db_connection()
        try:
            record = # [ Execute SQL query to get the record ]
            return record
        except (Exception, psycopg2.Error) as error:
            print("Error executing DB statement for job tracker.")
        return

    def get_db_connection(self):
        connection = None
        try:
            connection = # [ Initialize DB connection ]
        except (Exception, psycopg2.Error) as error:
            print("Error while connecitng to PostgreSQL", error)

        return connection

def run_reporter_etl(my_config):
    trade_date = my_config.get('production', 'processing_date')
    reporter = Reporter(spark, my_config)

    tracker = Tracker('analytical_etl', my_config)
    try:
        reporter.report(spark, trade_date, eod_dir)
        tracker.update_job_status("success")
    except Exception as e:
        print(e)
        tracker.update_job_status("failed")
    return