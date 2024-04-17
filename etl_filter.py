import boto3
import pandas as pd
from io import BytesIO
import json




s3_client = boto3.client('s3')
destination_bucket_name = 'lib-lth'

def invoke_mapping_lambda(event):
    lambda_client = boto3.client('lambda')
    
    payload = json.dumps(event)
    # Invoke the target Lambda function
    response = lambda_client.invoke(
        FunctionName='mappingFunction-lth',
        InvocationType='RequestResponse',  # Can be 'RequestResponse' or 'Event'
        Payload=payload
    )
    return response

def lambda_handler(event, context):
    # Initialize the S3 client
    
    
    # Define your bucket name
    bucket_name = 'athena-output-bucket-zybook-project'
    
    # List all objects in the bucket
    objects = s3_client.list_objects_v2(Bucket=bucket_name)
    
    # Define the prefix to filter specific folders
    prefix = 'zybook_id='
    
    
    # Initialize an empty DataFrame to store the combined data
    combined_df = pd.DataFrame()
    
    # List to store keys for deletion
    keys_to_delete = []
    
    # Define your destination bucket name and CSV file key
    csv_file_key = 'combined_data.csv'
    
    # Iterate over the objects and process files in specific folders
    for obj in objects['Contents']:
        key = obj['Key']
        
        if key.startswith(prefix):
            # Read the file
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            data = response['Body'].read()
            
            # Convert data to string
            data_str = data.decode('utf-8')
            
            # Create a DataFrame from the file data
            df = pd.read_csv(BytesIO(data))
            
            if not df.empty:
                # Filter the DataFrame based on the condition
                filtered_df = df[(df['sum(seconds)'] > 1800) & (df['max(r_score)'] != 1)]
                
                # Add the zybook_id column
                filtered_df['zybook_id'] = key.split('=')[1].split('/')[0]
                
                # Concatenate the DataFrame to the combined_df
                combined_df = pd.concat([combined_df, filtered_df])
        
        # Add the key to the list of keys for deletion
        keys_to_delete.append({'Key': key})
    
    
    
    # Save the combined_df to a CSV file
    csv_buffer = BytesIO()
    combined_df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=destination_bucket_name, Key=csv_file_key, Body=csv_buffer.getvalue())
    print(f"Saved combined_df to s3://{destination_bucket_name}/{csv_file_key}")

    
    # # Delete the objects from the S3 bucket
    # if keys_to_delete:
    #     response = s3_client.delete_objects(
    #         Bucket=bucket_name,
    #         Delete={
    #             'Objects': keys_to_delete
    #         }
    #     )
    #     print(f"Deleted {len(keys_to_delete)} objects from the bucket.")
    # else:
    #     print("No objects to delete.")
        
    
    # response = invoke_mapping_lambda(event)
    # return response
    
