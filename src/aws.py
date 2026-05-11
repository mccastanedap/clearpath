"""Reusable AWS client factories."""

import boto3

from src.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
)


def get_s3_client():
    """Return a boto3 S3 client.

    If explicit credentials are configured (local/dev via .env or Streamlit
    secrets), they are passed in. Otherwise boto3's default credential chain
    is used (IAM role, instance profile, ~/.aws/credentials, etc.).
    """
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        return boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
    return boto3.client("s3", region_name=AWS_REGION)
