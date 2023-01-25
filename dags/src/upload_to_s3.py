from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def multiple_upload_to_s3(**context):
    """uploading files from local_file_paths xcom one by one to s3 bucket"""

    s3_file_keys = []

    local_file_paths = context["task_instance"].xcom_pull(
        task_ids="fetch_plants_data_to_local", key="local_file_paths"
    )

    for file in local_file_paths:

        file_name = file
        bucket_name = "trefle-bucket"
        key = file.split("/")[-1]

        s3 = S3Hook("aws_default")
        s3.load_file(file_name, key, bucket_name, replace=True, encrypt=False)

        s3_file_keys.append(key)

    context["task_instance"].xcom_push("s3_file_paths", s3_file_keys)