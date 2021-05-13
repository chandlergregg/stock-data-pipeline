import datetime
import psycopg2
import uuid
from configreader import ConfigReader

class Tracker(object):
    """
    Tracks job statuses by initializing them and updating them
    """
    def __init__(self, job_name, db_config):    
        """
        Initializes tracker with given job_name and config
        Generates random job_id and inserts "Running" status into db
        """
        self.job_name = job_name
        self.db_config = db_config
        self.job_id = self.__generate_job_id()
        self.__initialize_job_status()
    
    def __generate_job_id(self):
        return str(uuid.uuid4())

    def __get_db_connection(self):
        """
        Try to establish connection with db and return connection if successful
        """
        connection = None
        try:
            connection = psycopg2.connect(**self.db_config)
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to Postgres", error)
        
        return connection

    def __insert_into_db(self, status):
        """
        Insert records into db table with provided status
        """
        # Get connection and cursor
        connection = self.__get_db_connection()
        cursor = connection.cursor()
        
        # Prepare query and values
        query = f"""
            INSERT INTO stock_proj.pipeline_job_status (job_name, job_status_timestamp, job_status, job_id)
            VALUES (%s, %s, %s, %s);
        """
        values = (self.job_name, datetime.datetime.now(), status, self.job_id)

        # Try insert, otherwise error out
        try:
            cursor.execute(query, values)
            connection.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error while inserting into Postgres:", error)

        # Close cursor and connection
        cursor.close()
        connection.close()

    def __initialize_job_status(self):
        """
        Initialize job by inserting "Started" record into db
        """
        self.__insert_into_db("Started")
        print(f"Job status initialized for: {self.job_name}")

    def update_job_status(self, status):
        """
        Insert updated job status into db
        """
        self.__insert_into_db(status)
        print(f"Job status updated to '{status}'")

def main():
    reader = ConfigReader("config.cfg", "postgres")
    db_config = reader.get_config()
    tracker = Tracker("Test job", db_config)

if __name__ == "__main__":
    main()
    



# def run_reporter_etl(my_config):
#     trade_date = my_config.get('production', 'processing_date')
#     reporter = Reporter(spark, my_config)

#     tracker = Tracker('analytical_etl', my_config)
#     try:
#         # First, 

#         reporter.report(spark, trade_date, eod_dir)
#         tracker.update_job_status("success")
#     except Exception as e:
#         print(e)
#         tracker.update_job_status("failed")
#     return