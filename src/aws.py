"""Reusable AWS client factories."""

import boto3

from src.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
)


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)
