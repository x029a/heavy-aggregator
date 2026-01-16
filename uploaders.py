import os
import logging
import requests
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

logger = logging.getLogger("HeavyAggregator")

class Uploader:
    def upload(self, file_path):
        raise NotImplementedError

class S3Uploader(Uploader):
    def __init__(self, bucket, region=None, access_key=None, secret_key=None):
        self.bucket = bucket
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key

    def upload(self, file_path):
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False

        file_name = os.path.basename(file_path)
        
        # Configure Boto3 Client
        # If keys are provided, use them. Otherwise rely on env vars or ~/.aws/credentials
        try:
            if self.access_key and self.secret_key:
                s3 = boto3.client(
                    's3', 
                    region_name=self.region,
                    aws_access_key_id=self.access_key, 
                    aws_secret_access_key=self.secret_key
                )
            else:
                s3 = boto3.client('s3', region_name=self.region)
                
            logger.info(f"Uploading {file_name} to s3://{self.bucket}/{file_name}...")
            s3.upload_file(file_path, self.bucket, file_name)
            logger.info("Upload Successful.")
            return True
            
        except NoCredentialsError:
            logger.error("AWS Credentials not found.")
            return False
        except ClientError as e:
            logger.error(f"S3 Client Error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 upload: {e}")
            return False

class WebhookUploader(Uploader):
    def __init__(self, url):
        self.url = url

    def upload(self, file_path):
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False
            
        file_name = os.path.basename(file_path)
        logger.info(f"Posting {file_name} to {self.url}...")
        
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (file_name, f)}
                # You might want to add headers or auth here in the future
                resp = requests.post(self.url, files=files, timeout=60)
                
            if resp.status_code in [200, 201, 202, 204]:
                logger.info(f"Upload Successful (Status: {resp.status_code})")
                return True
            else:
                logger.error(f"Upload Failed. Status: {resp.status_code}, Response: {resp.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Webhook connection error: {e}")
            return False

def get_uploader(settings):
    provider = settings.get('upload_provider', '').upper()
    
    if provider == 'S3':
        return S3Uploader(
            bucket=settings.get('s3_bucket'),
            region=settings.get('s3_region'),
            access_key=settings.get('s3_access_key'),
            secret_key=settings.get('s3_secret_key')
        )
    elif provider == 'WEBHOOK':
        return WebhookUploader(
            url=settings.get('webhook_url')
        )
    return None
