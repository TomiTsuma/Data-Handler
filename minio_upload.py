#!/usr/bin/env python3
"""
Standalone MinIO upload utility with hardcoded credentials.
"""

from pathlib import Path
from minio import Minio
from minio.error import S3Error
import time

# Hardcoded credentials
MINIO_ENDPOINT = "100.127.65.29:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_SECURE = False


def get_minio_client() -> Minio:
    """Create and return a MinIO client instance."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Create bucket if it doesn't exist."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")


def upload_file(bucket: str, source_path: str | Path, object_name: str | None = None,
                content_type: str | None = None, max_retries: int = 3) -> bool:
    """
    Upload a file to MinIO with retry logic.

    Args:
        bucket: Target bucket name
        source_path: Path to the file to upload
        object_name: Object name in MinIO (defaults to source filename)
        content_type: MIME type of the file
        max_retries: Maximum number of retry attempts

    Returns:
        True if upload succeeded, False otherwise
    """
    source_path = Path(source_path)
    if not source_path.exists():
        print(f"Error: File not found: {source_path}")
        return False

    if object_name is None:
        object_name = source_path.name

    client = get_minio_client()
    ensure_bucket(client, bucket)

    last_exception = None

    for attempt in range(max_retries):
        try:
            client.fput_object(
                bucket,
                object_name,
                str(source_path),
                content_type=content_type
            )
            print(f"Uploaded {source_path} to s3://{bucket}/{object_name}")
            return True
        except S3Error as exc:
            last_exception = exc
            print(f"Upload attempt {attempt + 1} failed: {exc}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)

    print(f"Failed to upload {source_path} after {max_retries} retries: {last_exception}")
    return False


def delete_object(bucket: str, object_name: str) -> bool:
    """Delete an object from MinIO."""
    client = get_minio_client()
    try:
        client.remove_object(bucket, object_name)
        print(f"Deleted s3://{bucket}/{object_name}")
        return True
    except S3Error as exc:
        print(f"Failed to delete s3://{bucket}/{object_name}: {exc}")
        return False


def delete_prefix(bucket: str, prefix: str) -> int:
    """Delete all objects with a given prefix. Returns count of deleted objects."""
    client = get_minio_client()
    deleted = 0
    try:
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        for obj in objects:
            client.remove_object(bucket, obj.object_name)
            deleted += 1
            print(f"Deleted s3://{bucket}/{obj.object_name}")
    except S3Error as exc:
        print(f"Failed to delete prefix {prefix}: {exc}")
    return deleted


def upload_files_to_prefix(bucket: str, source_dir: str | Path, prefix: str = "",
                           content_type: str | None = None, max_retries: int = 3) -> list[str]:
    """
    Upload all files from a directory to MinIO with a common prefix.

    Args:
        bucket: Target bucket name
        source_dir: Directory containing files to upload
        prefix: Object prefix (folder path) in MinIO
        content_type: MIME type for all files
        max_retries: Maximum retry attempts per file

    Returns:
        List of successfully uploaded object names
    """
    source_dir = Path(source_dir)
    uploaded = []

    for file_path in source_dir.iterdir():
        if file_path.is_file():
            object_name = f"{prefix}/{file_path.name}" if prefix else file_path.name
            if upload_file(bucket, str(file_path), object_name, content_type, max_retries):
                uploaded.append(object_name)

    return uploaded


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage:")
        print("  python minio_upload.py <bucket> <source_path> [object_name]  - Upload single file")
        print("  python minio_upload.py <bucket> <source_dir> --dir [prefix]  - Upload directory")
        print("  python minio_upload.py <bucket> delete <object_name>          - Delete single object")
        print("  python minio_upload.py <bucket> delete-prefix <prefix>        - Delete all objects with prefix")
        sys.exit(1)

    bucket = sys.argv[1]
    arg1 = sys.argv[2]
    arg2 = sys.argv[3] if len(sys.argv) > 3 else None
    arg3 = sys.argv[4] if len(sys.argv) > 4 else None

    if arg1 == "--dir":
        # bucket --dir [prefix]
        source_dir = arg2
        prefix = arg3 if arg3 else ""
        uploaded = upload_files_to_prefix(bucket, source_dir, prefix)
        print(f"\nUploaded {len(uploaded)} files to s3://{bucket}/{prefix}")
        sys.exit(0)
    elif arg1 == "delete-prefix":
        # bucket delete-prefix <prefix>
        prefix = arg2
        deleted = delete_prefix(bucket, prefix)
        print(f"\nDeleted {deleted} objects from s3://{bucket}/{prefix}")
        sys.exit(0)
    elif arg1 == "delete":
        # bucket delete <object_name>
        success = delete_object(bucket, arg2)
        sys.exit(0 if success else 1)
    elif arg2 == "--dir":
        # bucket source --dir [prefix]
        source_dir = arg1
        prefix = arg3 if arg3 else ""
        uploaded = upload_files_to_prefix(bucket, source_dir, prefix)
        print(f"\nUploaded {len(uploaded)} files to s3://{bucket}/{prefix}")
        sys.exit(0)
    else:
        # Single file upload: bucket source [object_name]
        source = arg1
        object_name = arg2
        success = upload_file(bucket, source, object_name)
        sys.exit(0 if success else 1)
