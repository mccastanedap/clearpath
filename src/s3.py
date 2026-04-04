import boto3
import pandas as pd
from io import StringIO

def read_csv_from_s3(bucket_name, file_key):
    """
    Reads a CSV file from S3 and returns a pandas dataframe.
    """
    s3_client = boto3.client('s3')
    
    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_key
    )
    
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    
    return df