CREATE STAGE IF NOT EXISTS STAGE_TREFLE_S3
    url = 's3://trefle-bucket'
    credentials = (aws_secret_key = '{{ var.value.get('aws_secret_key', 'fallback') }}' aws_key_id = '{{ var.value.get('aws_key_id', 'fallback') }}');