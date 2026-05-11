import pandas as pd
from io import StringIO

from src.aws import get_s3_client


def read_csv_from_s3(bucket_name, file_key):
    """
    Reads a CSV file from S3 and returns a pandas dataframe.
    """
    s3_client = get_s3_client()

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=file_key
    )

    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))

    return df
