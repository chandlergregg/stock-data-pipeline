import datetime
import psycopg2
import uuid
from configreader import ConfigReader

class Tracker:
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

class Reporter:

    def __init__(self, db_config, job_ids):
        # Get jobids to query
        self.job_ids = job_ids
        self.db_config = db_config

    def get_report(self):
         # Get connection and cursor
        connection = None
        try:
            connection = psycopg2.connect(**self.db_config)
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to Postgres", error)

        cursor = connection.cursor()

        # Attempt query and get results
        query = f"""
            SELECT * FROM stock_proj.pipeline_job_status
            WHERE job_id IN %(job_ids)s
            ORDER BY job_status_timestamp
        """
        try:
            cursor.execute(query, {"job_ids": tuple(self.job_ids)})
            results = cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Error while querying Postgres:", error)

        # Close cursor and connection
        cursor.close()
        connection.close()

        print("""| job_name \t | job_status_timestamp \t | job_status \t | job_id \t |""")
        for row in results:
            print(f"""{row[1]} | {row[2]} | {row[3]} | {row[4]}""")

# Code for testing tracker
def main():
    reader = ConfigReader("config.cfg", "postgres")
    db_config = reader.get_config()
    tracker = Tracker("Test job", db_config)
    reporter = Reporter(db_config, ["b0ca6902-8d7b-49a2-9a17-b94e42e839fc",
        "c3a2320c-19f1-4051-aa5f-85fc58d39ac9",
        "e74252e6-558c-49c1-aa13-99be2cc59585"])
    reporter.get_report()

if __name__ == "__main__":
    main()
