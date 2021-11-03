from pyspark.sql import SparkSession
from pyspark_etl_pipeline.configreader import ConfigReader
from pyspark_etl_pipeline.pipeline_steps import *
from pyspark_etl_pipeline.tracker import Tracker

def main():
    
    spark = SparkSession.builder.getOrCreate()
    from azure.storage.blob import BlobServiceClient

    storage_config = ConfigReader("config.cfg", "azure-storage").get_config()
    db_config = ConfigReader("config.cfg", "postgres").get_config()

    # Get Azure storage info from config
    storage_acct_name = storage_config["account_name"]
    storage_acct_access_key = storage_config["access_key"]
    storage_container = storage_config["container_name"]
    mount_root = storage_config["mount_root"]

    # Set Spark Azure storage account and key
    storage_acct_key_str = f"fs.azure.account.key.{storage_acct_name}.blob.core.windows.net"
    spark.conf.set(storage_acct_key_str, storage_acct_access_key)

    mount_base_path = f"{mount_root}/{storage_container}"
    input_dir = "data"

    # Set filetype
    file_type = "txt"
    file_suffix = f".{file_type}"
    suffix_len = len(file_suffix)

    # Set up container client
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_acct_name}.blob.core.windows.net", \
        credential=storage_acct_access_key)
    container_client = blob_service_client.get_container_client(storage_container)
    # Get list of file names
    blob_list = container_client.list_blobs(name_starts_with=input_dir)
    txtfile_paths = [ blob.name for blob in blob_list if blob.name[-suffix_len:] == file_suffix ]
    txtfile_full_paths = [ f"{mount_base_path}/{file}" for file in txtfile_paths ]

    steps = []
    step_1 = PipelineStep1("Step 1: Ingest", 
        spark=spark, 
        mount_base_path=mount_base_path,
        input_path=",".join(txtfile_full_paths), 
        output_path=f"{mount_base_path}/ingested-data")
    steps.append(step_1)
    
    step_2 = PipelineStep2("Step 2: Preprocess",
        spark=spark,
        mount_base_path=mount_base_path,    
        input_path=step_1.output_path,
        output_path=f"{mount_base_path}/preprocessed-data")
    steps.append(step_2)

    step_3 = PipelineStep3("Step 3: ETL",
        spark=spark,
        mount_base_path=mount_base_path,
        input_path=step_2.output_path,
        output_path=f"{mount_base_path}/ETL-output")
    steps.append(step_3)
    
    for step in steps:
        tracker = Tracker(job_name=step.name, db_config=db_config)
        try:
            step.run()
            tracker.update_job_status("Success")
        except Exception as e:
            print(e)
            tracker.update_job_status("Failed")

if __name__ == "__main__":
    main()
