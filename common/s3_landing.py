"""
S3 landing zone layout, shared by the uploader and the loader.

    s3://<bucket>/<prefix>/<entity>/batch=<YYYY-MM>/<entity>.csv
    s3://<bucket>/<prefix>/_control/batch=<YYYY-MM>/control.json

Hive-style `batch=` folders keep each monthly extract separate, so one month can
be re-landed or re-loaded without touching the others.

boto3 reads AWS_ENDPOINT_URL from the environment, so the same code talks to
real AWS S3 or to the local S3 emulator in docker-compose / CI.
"""

import os
import re
from pathlib import Path

CONTROL_FILE = "control.json"
BATCH_RE = re.compile(r"batch=(\d{4}-\d{2})/")


def s3_client():
    """boto3 S3 client. A local emulator (AWS_ENDPOINT_URL set) needs path-style
    URLs (http://host:port/bucket/key); real AWS keeps the default style."""
    import boto3
    from botocore.config import Config

    style = "path" if os.environ.get("AWS_ENDPOINT_URL") else "auto"
    return boto3.client("s3", config=Config(s3={"addressing_style": style}))


def object_key(prefix: str, batch_id: str, file_name: str) -> str:
    entity = "_control" if file_name == CONTROL_FILE else Path(file_name).stem
    return f"{prefix}/{entity}/batch={batch_id}/{file_name}"


def list_batches(client, bucket: str, prefix: str) -> list[str]:
    """Batches that have a control file, i.e. were landed completely."""
    batches = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/_control/"):
        for obj in page.get("Contents", []):
            match = BATCH_RE.search(obj["Key"])
            if match and obj["Key"].endswith(CONTROL_FILE):
                batches.add(match.group(1))
    return sorted(batches)


def download_batch(client, bucket: str, prefix: str, batch_id: str,
                   file_names: list[str], dest_root: Path) -> Path:
    """Download one batch into dest_root/<batch_id>/ and return that folder."""
    batch_dir = dest_root / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    for file_name in [*file_names, CONTROL_FILE]:
        client.download_file(bucket, object_key(prefix, batch_id, file_name),
                             str(batch_dir / file_name))
    return batch_dir
