"""
Shared S3 utility used by upload-service and transcoding-service.
Falls back to local filesystem if AWS credentials are not configured.
"""
import os
from typing import TYPE_CHECKING, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "")
S3_PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET", "")
CDN_BASE_URL = os.environ.get("CDN_BASE_URL", "")

USE_S3 = bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and S3_RAW_BUCKET)


def get_s3_client() -> "S3Client":
    return boto3.client(  # type: ignore[reportUnknownMemberType]
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def upload_to_s3(
    local_path: str,
    s3_key: str,
    bucket: str,
    content_type: str = "application/octet-stream",
) -> bool:
    """Upload a local file to S3. Returns True on success."""
    if not USE_S3:
        print(f"[S3] S3 not configured - skipping upload of {s3_key}")
        return False

    try:
        s3 = get_s3_client()
        s3.upload_file(
            local_path, bucket, s3_key,
            ExtraArgs={"ContentType": content_type}
        )
        print(f"[S3] Uploaded {local_path} -> s3://{bucket}/{s3_key}")
        return True
    except (NoCredentialsError, ClientError) as e:
        print(f"[S3] Upload failed: {e}")
        return False


def upload_bytes_to_s3(
    data: bytes,
    s3_key: str,
    bucket: str,
    content_type: str = "application/octet-stream",
) -> bool:
    """Upload raw bytes to S3."""
    if not USE_S3:
        return False

    try:
        s3 = get_s3_client()
        s3.put_object(
            Body=data, Bucket=bucket,
            Key=s3_key, ContentType=content_type,
        )
        print(f"[S3] Uploaded bytes -> s3://{bucket}/{s3_key}")
        return True
    except (NoCredentialsError, ClientError) as e:
        print(f"[S3] Bytes upload failed: {e}")
        return False


def delete_from_s3(s3_key: str, bucket: str) -> bool:
    """Delete a file from S3."""
    if not USE_S3:
        return False
    try:
        s3 = get_s3_client()
        s3.delete_object(Bucket=bucket, Key=s3_key)
        print(f"[S3] Deleted s3://{bucket}/{s3_key}")
        return True
    except (NoCredentialsError, ClientError) as e:
        print(f"[S3] Delete failed: {e}")
        return False


def get_cdn_url(s3_key: str) -> str:
    """Return the CDN URL for a processed file or fallback streaming path."""
    if CDN_BASE_URL:
        base = CDN_BASE_URL.rstrip('/')
        # Ensure the URL has a protocol scheme
        if not base.startswith('http'):
            base = f"https://{base}"
        return f"{base}/{s3_key}"

    # Fallback: serve via local streaming-service
    return f"/stream-local/{s3_key}"


def generate_presigned_url(
    s3_key: str, bucket: str, expiry: int = 3600,
) -> Optional[str]:
    """Generate a short-lived pre-signed URL for direct S3 access."""
    if not USE_S3:
        return ""

    try:
        s3 = get_s3_client()
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": s3_key},
            ExpiresIn=expiry
        )
        return url
    except Exception as e:
        print(f"[S3] Presigned URL failed: {e}")
        return ""
