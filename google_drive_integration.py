#!/usr/bin/env python

import tempfile
import boto3
import os
import sys
import json

from apiclient import discovery
from apiclient import errors
from apiclient.http import MediaFileUpload

def purge_bucket(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()

def get_service_account_credentials(credentials_parameter):
   
    from google.oauth2 import service_account
    import googleapiclient.discovery

    # get_parameter returns a dictionary object of the json string, so convert it
    # back to a string needed for getting the Google credentials object.
    #
    # Note that WithDecryption=True would be set if using a SecureString in Parameter Store
    ssm_client = boto3.client('ssm')
    creds_dict = ssm_client.get_parameter(Name=credentials_parameter, WithDecryption=False)['Parameter']['Value']
    creds_json = json.loads(creds_dict)

    scopes_list = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
    ]

    credentials = service_account.Credentials.from_service_account_info(creds_json, scopes=scopes_list)
    
    # note for using a credentials json file instead
    # service_account_file = './credentials.json'
    # credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes_list)
            
    return credentials


def get_folder_id(folder_id_parameter):

    
    # Note that WithDecryption=True would be set if using a SecureString in Parameter Store
    ssm_client = boto3.client('ssm')
    return ssm_client.get_parameter(Name=folder_id_parameter, WithDecryption=False)['Parameter']['Value']    

    
def upload_file(service, file_name_with_path, file_name, description, folder_id, mime_type):  
    
    media_body = MediaFileUpload(file_name_with_path, mimetype=mime_type)

    body = {
        'name': file_name,
        'title': file_name,
        'description': description,
        'mimeType': mime_type,
        'parents': [folder_id]
    }
    
    # note that supportsAllDrives=True is required or else the file upload will fail
    file = service.files().create(
        supportsAllDrives=True,
        body=body,
        media_body=media_body).execute()


    print('{}, {}'.format(file_name, file['id']))
    
    return file

    
def lambda_handler(event, context):
    """
    Pull the specified files from S3 and push to a Shared Folder in Google Drive.
    The payload passed in contains a list of Excel file names in the array fileList.
    """
    print('payload:', event)

    bucket = os.environ.get('REPORTS_BUCKET')
    credentials_parameter = os.environ.get('GOOGLE_CREDENTIALS_PARAMETER')
    folder_id_parameter = os.environ.get('GOOGLE_SHARED_FOLDER_ID_PARAMETER')

    credentials = get_service_account_credentials(credentials_parameter)
    folder_id = get_folder_id(folder_id_parameter)
    
    # note regarding cache_discovery=False
    # https://github.com/googleapis/google-api-python-client/issues/299
    service = discovery.build('drive', 'v3', credentials=credentials, cache_discovery=False)

    s3_client = boto3.client('s3')
    
    # for file_name in event['fileList']:
    #     download_path = '/tmp/{}'.format(file_name)
    #     s3_client.download_file(bucket, file_name, download_path)
    #     upload_file(service, download_path, file_name, file_name, folder_id, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response = s3_client.list_objects_v2(Bucket=bucket)
    
    for obj in response.get('Contents', []):
        key = obj['Key']
        print(f'Downloading {key}')
        
        # Download the file to the /tmp directory in the Lambda function
        download_path = f'/tmp/{key}'
        s3_client.download_file(bucket, key, download_path)
        upload_file(service, download_path, key, key, folder_id, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    
    purge_bucket("lib-lth")
        
    # download_path = '/tmp/final_file.csv'
    
    # s3_client.download_file(bucket, 'combined_data.csv', download_path)
    # upload_file(service, download_path, 'final_file.csv', 'final_file.csv', folder_id, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
