#!/usr/bin/env python3
"""
Glue Python Shell job (INSECURE): Download .json.gz files, upload to S3, unzip, and delete gz.
"""

import argparse
import logging
import sys
import time
import ssl
import gzip
import io
import urllib.request
import boto3
import botocore
from urllib.error import URLError, HTTPError

# ------------------ Default URLs ------------------
DEFAULT_REVIEW_URL = "https://jmcauley.ucsd.edu/data/amazon_v2/categoryFiles/AMAZON_FASHION.json.gz"
DEFAULT_META_URL   = "https://jmcauley.ucsd.edu/data/amazon_v2/metaFiles2/meta_AMAZON_FASHION.json.gz"

# ------------------ Setup logging ------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ------------------ Normalize AWS Glue args ------------------
def normalize_arg_variants():
    new_argv = [sys.argv[0]]
    for token in sys.argv[1:]:
        if token.startswith("--") and "_" in token and not token.startswith("--aws"):
            new_argv.append(token.replace("_", "-"))
        else:
            new_argv.append(token)
    sys.argv[:] = new_argv

normalize_arg_variants()

# ------------------ Argparse ------------------
parser = argparse.ArgumentParser(description="Download, upload, unzip JSON.GZ datasets to S3.")
parser.add_argument("--s3-bucket", "--s3_bucket", dest="s3_bucket", required=True, help="Target S3 bucket")
parser.add_argument("--s3-prefix", "--s3_prefix", dest="s3_prefix", default="", help="Optional prefix (no leading slash)")
parser.add_argument("--review-url", "--review_url", dest="review_url", default=DEFAULT_REVIEW_URL, help="Review dataset URL")
parser.add_argument("--meta-url", "--meta_url", dest="meta_url", default=DEFAULT_META_URL, help="Meta dataset URL")
parser.add_argument("--review-key", "--review_key", dest="review_key", default=None, help="S3 object key for review file")
parser.add_argument("--meta-key", "--meta_key", dest="meta_key", default=None, help="S3 object key for meta file")
parser.add_argument("--upload-raw", "--upload_raw", dest="upload_raw", default="true", choices=["true", "false"])
parser.add_argument("--timeout", dest="timeout", type=int, default=60, help="HTTP timeout seconds")
parser.add_argument("--retries", dest="retries", type=int, default=3, help="Retry attempts")
parsed, unknown = parser.parse_known_args()

# ------------------ Utility helpers ------------------
def infer_filename_from_url(url: str) -> str:
    return url.rstrip("/").split("/")[-1]

def make_key(prefix: str, name: str) -> str:
    if not prefix:
        return name
    return f"{prefix.rstrip('/')}/{name}"

# ------------------ Downloader + S3 upload ------------------
def download_url_to_s3(url: str, bucket: str, key: str, timeout: int, retries: int) -> bool:
    """Download URL (with insecure SSL) and upload to S3 as .gz"""
    ssl_ctx = ssl._create_unverified_context()
    s3_client = boto3.client("s3")
    attempt = 0
    while attempt <= retries:
        try:
            logger.info(f"Downloading (insecure): {url} (attempt {attempt+1})")
            with urllib.request.urlopen(url, timeout=timeout, context=ssl_ctx) as resp:
                s3_client.upload_fileobj(resp, bucket, key)
            logger.info(f"Uploaded .gz file to s3://{bucket}/{key}")
            return True
        except (HTTPError, URLError, botocore.exceptions.BotoCoreError, OSError) as e:
            attempt += 1
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt > retries:
                logger.error(f"Exceeded retries for {url}")
                return False
            time.sleep(2 ** attempt)
    return False

def unzip_s3_gz_to_json(bucket: str, gz_key: str, json_key: str) -> bool:
    """Download .gz from S3, decompress, and upload .json back to S3"""
    s3 = boto3.client("s3")
    try:
        logger.info(f"Downloading and decompressing s3://{bucket}/{gz_key}")
        gz_obj = s3.get_object(Bucket=bucket, Key=gz_key)
        gz_stream = io.BytesIO(gz_obj["Body"].read())

        with gzip.GzipFile(fileobj=gz_stream, mode="rb") as gz:
            json_bytes = gz.read()

        # Upload the decompressed JSON
        s3.put_object(Bucket=bucket, Key=json_key, Body=json_bytes)
        logger.info(f"Uploaded decompressed JSON to s3://{bucket}/{json_key}")
        return True
    except Exception as e:
        logger.error(f"Error unzipping {gz_key}: {e}")
        return False

def delete_s3_key(bucket: str, key: str):
    """Delete a specific object from S3"""
    s3 = boto3.client("s3")
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Deleted gz file s3://{bucket}/{key}")
    except Exception as e:
        logger.warning(f"Failed to delete {key}: {e}")

# ------------------ Main ------------------
def main():
    s3_bucket = parsed.s3_bucket
    s3_prefix = parsed.s3_prefix.strip().strip("/")
    upload_raw = parsed.upload_raw.lower() == "true"
    timeout = parsed.timeout
    retries = parsed.retries

    review_url = parsed.review_url
    meta_url = parsed.meta_url

    review_gz = parsed.review_key or infer_filename_from_url(review_url)
    meta_gz   = parsed.meta_key or infer_filename_from_url(meta_url)

    review_gz_key = make_key(s3_prefix, f"reviews/{review_gz}")
    meta_gz_key   = make_key(s3_prefix, f"meta/{meta_gz}")

    review_json_key = review_gz_key.replace(".gz", "")
    meta_json_key   = meta_gz_key.replace(".gz", "")

    results = {}

    if upload_raw:
        # REVIEW dataset
        logger.info("Processing review dataset...")
        if download_url_to_s3(review_url, s3_bucket, review_gz_key, timeout, retries):
            if unzip_s3_gz_to_json(s3_bucket, review_gz_key, review_json_key):
                delete_s3_key(s3_bucket, review_gz_key)
                results["review"] = f"s3://{s3_bucket}/{review_json_key}"
        # META dataset
        logger.info("Processing meta dataset...")
        if download_url_to_s3(meta_url, s3_bucket, meta_gz_key, timeout, retries):
            if unzip_s3_gz_to_json(s3_bucket, meta_gz_key, meta_json_key):
                delete_s3_key(s3_bucket, meta_gz_key)
                results["meta"] = f"s3://{s3_bucket}/{meta_json_key}"

    logger.info("✅ Job finished.")
    logger.info("Generated JSON files:")
    for k, v in results.items():
        logger.info(f" - {k}: {v}")

if __name__ == "__main__":
    main()