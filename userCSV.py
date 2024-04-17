import pandas as pd
import json
import boto3
import os

def lambda_handler(event, context):
    
    bucket = "zybooks-stream"
    key = "users.json"
    
    # Download the JSON file from S3
    s3_client = boto3.client('s3')
    download_path = '/tmp/input.json'
    s3_client.download_file(bucket, key, download_path)

    # Read the JSON file into a DataFrame
    df = pd.read_json(download_path)

    # Convert DataFrame to CSV
    csv_data = df.to_csv(index=False)

    # Upload the CSV file to S3
    upload_path = os.path.splitext(key)[0] + '.csv'
    upload_key = f'converted/{upload_path}'
    upload_file_path = '/tmp/output.csv'
    with open(upload_file_path, 'w') as f:
        f.write(csv_data)
    s3_client.upload_file(upload_file_path, bucket, upload_key)

    # Clean up
    os.remove(download_path)
    os.remove(upload_file_path)

    return {
        'statusCode': 200,
        'body': json.dumps('Conversion successful')
    }
