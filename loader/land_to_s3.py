"""
Land the monthly extracts in the S3 landing zone (the "data lake" entry point).

    s3://<bucket>/raw/<entity>/batch=<YYYY-MM>/<entity>.csv
    s3://<bucket>/raw/_control/batch=<YYYY-MM>/control.json

The control file is uploaded LAST, so a batch only becomes visible to the loader
once all of its files are in place (the loader lists batches by control file).
Uploading the same batch again overwrites the same keys, so it is safe to rerun.

Works against real AWS or the local S3 emulator in docker-compose: boto3 reads
AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from the
environment / .env.

Usage:
    python loader/land_to_s3.py                         # land all batches
    python loader/land_to_s3.py --batch 2026-02         # land one batch
    python loader/land_to_s3.py --create-bucket         # local emulator: create the bucket first
"""

import argparse
import os
import sys
from pathlib import Path

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.s3_landing import CONTROL_FILE, object_key, s3_client  # noqa: E402
from common.settings import s3_location  # noqa: E402

DATA_FILES = ["customers.csv", "accounts.csv", "transactions.csv", "balances.csv"]


def ensure_bucket(client, bucket):
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError:
        pass
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    if region == "us-east-1":
        client.create_bucket(Bucket=bucket)
    else:
        client.create_bucket(Bucket=bucket,
                             CreateBucketConfiguration={"LocationConstraint": region})
    print(f"  created bucket s3://{bucket}")


def land_batch(client, bucket, prefix, batch_dir):
    batch_id = batch_dir.name
    for file_name in [*DATA_FILES, CONTROL_FILE]:   # control file last
        path = batch_dir / file_name
        if not path.exists():
            raise FileNotFoundError(f"{path} is missing; regenerate the extracts first")
        client.upload_file(str(path), bucket, object_key(prefix, batch_id, file_name))
    return len(DATA_FILES) + 1


def main():
    parser = argparse.ArgumentParser(description="Upload extracts to the S3 landing zone")
    parser.add_argument("--data", default="data/extracts")
    parser.add_argument("--batch", help="land only this batch, e.g. 2026-02")
    parser.add_argument("--create-bucket", action="store_true",
                        help="create the bucket if it does not exist (local emulator)")
    args = parser.parse_args()

    bucket, prefix = s3_location()
    client = s3_client()
    if args.create_bucket:
        ensure_bucket(client, bucket)

    batches = sorted(p for p in Path(args.data).iterdir() if p.is_dir())
    if args.batch:
        batches = [b for b in batches if b.name == args.batch]
        if not batches:
            sys.exit(f"Batch {args.batch} not found in {args.data}")

    for batch_dir in batches:
        n = land_batch(client, bucket, prefix, batch_dir)
        print(f"  {batch_dir.name}: {n} files -> s3://{bucket}/{prefix}/<entity>/batch={batch_dir.name}/")
    print("All batches landed.")


if __name__ == "__main__":
    main()
