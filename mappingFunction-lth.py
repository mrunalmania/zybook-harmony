import boto3
import pandas as pd
from io import StringIO
from io import BytesIO
from datetime import datetime
import json

# Specify the bucket names and file names
src_bucket_name = 'zybooks-stream'
src_file_name = 'converted/users.csv'
dest_bucket_name = 'lib-lth'
dest_file_name = 'combined_data.csv'

s3_client = boto3.client('s3')


def add_link_and_section(row):
    zybook_code = row['right_zybook_code']
    section_number = row['right_section_number']
    chapter_number = row['right_chapter_number']
    link = f"https://learn.zybooks.com/zybook/{zybook_code}/chapter/{chapter_number}/section/{section_number}"
    section = f"{section_number}.{chapter_number}"
    return pd.Series({'link': link, 'zybook_code': section})


    
    

def lambda_handler(event, context):
    
    
    # Read the user2024.csv file from the zybook-stream bucket
    src_file_obj = s3_client.get_object(Bucket=src_bucket_name, Key=src_file_name)
    user2024_df = pd.read_csv(src_file_obj['Body'])
    
    # Read the combined.csv file from the lib-lth bucket
    dest_file_obj_1 = s3_client.get_object(Bucket=dest_bucket_name, Key=dest_file_name)
    combined_df = pd.read_csv(dest_file_obj_1['Body'])
    
    
    # Merge the two datasets based on the user_id column
    merged_df = pd.merge(combined_df, user2024_df, on=['user_id','zybook_id'], how='left')
    
    # # Add link and section columns to the merged_df
    merged_df[['link', 'section']] = merged_df.apply(add_link_and_section, axis=1)
    # print(merged_df)
    
    csv_buffer = StringIO()
    merged_df.to_csv(csv_buffer, index=False)
    
    
    s3_client.put_object(Bucket=dest_bucket_name, Key="new_combined_data.csv", Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': "r"
    }
