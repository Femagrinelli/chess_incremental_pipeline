"""
S3 helper functions.
The daily design avoids list calls in the hot path and relies on deterministic keys.
"""

import json
import logging
import os
from functools import lru_cache
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_client():
    endpoint_url = os.environ.get("S3_ENDPOINT_URL") or None
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        config=Config(s3={"addressing_style": "path"}),
    )


def _get_bucket() -> str:
    return os.environ.get("S3_BUCKET", "chess-lake")


def upload_json(object_path: str, data: dict, metadata: Optional[dict] = None) -> None:
    client = _get_client()
    bucket = _get_bucket()
    payload = json.dumps(data, ensure_ascii=False, default=str, sort_keys=True).encode("utf-8")
    cleaned_metadata = {str(k): str(v) for k, v in (metadata or {}).items() if v is not None}

    client.put_object(
        Bucket=bucket,
        Key=object_path,
        Body=payload,
        ContentType="application/json",
        Metadata=cleaned_metadata,
    )
    logger.info("Uploaded s3://%s/%s", bucket, object_path)


def download_json(object_path: str) -> Optional[dict]:
    client = _get_client()
    bucket = _get_bucket()
    try:
        response = client.get_object(Bucket=bucket, Key=object_path)
        return json.loads(response["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in {"NoSuchKey", "404"}:
            return None
        raise


def get_object_head(object_path: str) -> Optional[dict]:
    client = _get_client()
    bucket = _get_bucket()
    try:
        return client.head_object(Bucket=bucket, Key=object_path)
    except ClientError as e:
        if e.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return None
        raise


def object_exists(object_path: str) -> bool:
    return get_object_head(object_path) is not None


def list_objects(prefix: str) -> list[str]:
    client = _get_client()
    bucket = _get_bucket()
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    keys = []
    for page in pages:
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys
