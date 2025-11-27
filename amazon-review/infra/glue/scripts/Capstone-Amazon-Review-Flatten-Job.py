#!/usr/bin/env python3
"""
Glue Spark job: read extracted JSON files from S3, robustly parse JSON lines (handles duplicate keys),
recursively flatten, and write to separate prefixes (parquet/csv/json).

Behavior:
 - Writes detailed CloudWatch-visible logs (root logger).
 - Writes S3 markers: _JOB_STARTED_<JOB>, _SUCCESS, _JOB_FINISHED_<JOB>, _JOB_FAILED_<JOB>.
 - If any dataset fails, the job will exit with non-zero status (SystemExit) so Glue run is marked FAILED,
   unless you explicitly allow soft-failure via --fail_on_error false.

Glue job args (pass as Job parameters):
 --JOB_NAME                (provided by Glue)
 --s3_bucket               target bucket (required)
 --input_prefix            input prefix where json files live (optional; default '')
 --review_json_key         path under prefix to review json (e.g. reviews/AMAZON_FASHION.json)
 --meta_json_key           path under prefix to meta json (e.g. meta/meta_AMAZON_FASHION.json)
 --review_output_prefix    output prefix (under s3_bucket) for flattened reviews (required)
 --meta_output_prefix      output prefix for flattened meta (required)
 --output_format           parquet|csv|json (default parquet)
 --partition_by            optional comma-separated columns to partition parquet output (ignored for csv/json)
 --coalesce               optional int: coalesce output files to N (default: 0 -> no coalesce)
 --compression            compression codec: for parquet 'snappy'|'gzip'|'none' (default snappy). For csv use 'gzip' or 'none'.
 --fail_on_error          true|false  (default true) -> if true, raise SystemExit on any dataset failure (marks Glue run FAILED)
"""

from __future__ import annotations
import sys
import os
import logging
import socket
import traceback
import json
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType
import boto3
import botocore

# ----------------- Helpers for CLI args (handles Glue injected args) -----------------
def parse_optional_arg(name: str, default=None):
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            val = sys.argv[idx + 1]
            if not val.startswith("--"):
                return val
    # also try underscored variant
    alt_flag = f"--{name.replace('-', '_')}"
    if alt_flag in sys.argv:
        idx = sys.argv.index(alt_flag)
        if idx + 1 < len(sys.argv):
            val = sys.argv[idx + 1]
            if not val.startswith("--"):
                return val
    return default

# Required via Glue (JOB_NAME, s3_bucket)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket'])
JOB_NAME = args['JOB_NAME']
S3_BUCKET = args['s3_bucket']

# Optional parameters
INPUT_PREFIX = (parse_optional_arg('input_prefix', '') or '').strip().strip('/')
REVIEW_JSON_KEY = parse_optional_arg('review_json_key', 'reviews/AMAZON_FASHION.json')
META_JSON_KEY = parse_optional_arg('meta_json_key', 'meta/meta_AMAZON_FASHION.json')
REVIEW_OUTPUT_PREFIX = parse_optional_arg('review_output_prefix', 'flattened/reviews')
META_OUTPUT_PREFIX = parse_optional_arg('meta_output_prefix', 'flattened/meta')
OUTPUT_FORMAT = (parse_optional_arg('output_format', 'parquet') or 'parquet').lower()
PARTITION_BY = parse_optional_arg('partition_by', None)
COALESCE = int(parse_optional_arg('coalesce', '0') or 0)
COMPRESSION = (parse_optional_arg('compression', None) or '').lower()
# fail_on_error default TRUE to ensure Glue job marks FAILED on failure; can be overridden
FAIL_ON_ERROR = (parse_optional_arg('fail_on_error', None) or parse_optional_arg('fail_on_error', 'true') or 'true').lower() in ('1','true','yes')

# Set sensible defaults
if OUTPUT_FORMAT == 'parquet' and not COMPRESSION:
    COMPRESSION = 'snappy'
if OUTPUT_FORMAT == 'csv' and not COMPRESSION:
    COMPRESSION = 'none'

# ----------------- Spark / Glue contexts -----------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Root Python logging (ensures messages appear in CloudWatch driver logs)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
pylogger = logging.getLogger()
pylogger.setLevel(logging.INFO)
glue_logger = glueContext.get_logger()

# S3 client for marker writes
s3_client = boto3.client('s3')

# ----------------- Small utility functions -----------------
def s3_uri(bucket: str, prefix: str) -> str:
    if not prefix:
        return f"s3://{bucket}/"
    return f"s3://{bucket}/{prefix.rstrip('/')}"

def full_input_path(prefix: str, key: str) -> str:
    if not prefix:
        return f"s3://{S3_BUCKET}/{key.lstrip('/')}"
    return f"s3://{S3_BUCKET}/{prefix.rstrip('/')}/{key.lstrip('/')}"

def make_s3_key(prefix: str, name: str) -> str:
    if not prefix:
        return name
    return f"{prefix.rstrip('/')}/{name}"

def write_s3_marker(bucket: str, prefix: str, name: str, content: str = ""):
    """Write a small marker object to S3. prefix may be empty string."""
    key = make_s3_key(prefix, name) if prefix else name
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=content.encode() if isinstance(content, str) else content)
        pylogger.info("Wrote marker s3://%s/%s", bucket, key)
    except Exception:
        pylogger.exception("Failed to write marker s3://%s/%s", bucket, key)

# ----------------- Collision-safe naming helper -----------------
def unique_name(base: str, existing: set) -> str:
    """Return a name based on `base` that's not in `existing` and add it to existing."""
    if base not in existing:
        existing.add(base)
        return base
    i = 1
    while True:
        candidate = f"{base}_{i}"
        if candidate not in existing:
            existing.add(candidate)
            return candidate
        i += 1

# ----------------- Flattening utilities (collision-safe) -----------------
def flatten_structs(df: DataFrame) -> DataFrame:
    """
    Expand top-level StructType columns into separate columns (parent_child), avoiding name collisions.
    """
    schema = df.schema
    struct_fields = [f for f in schema.fields if isinstance(f.dataType, StructType)]
    if not struct_fields:
        return df

    existing = set(df.columns)
    select_cols = []
    for field in schema.fields:
        name = field.name
        dtype = field.dataType
        if isinstance(dtype, StructType):
            if name in existing:
                existing.remove(name)
            for child in dtype:
                child_name = child.name
                base = f"{name}_{child_name}"
                uniq = unique_name(base, existing)
                select_cols.append(col(f"{name}.{child_name}").alias(uniq))
                pylogger.debug("Expanding struct %s.%s -> %s", name, child_name, uniq)
        else:
            if name in existing:
                existing.remove(name)
                select_cols.append(col(name))
            else:
                safe = unique_name(name, existing)
                select_cols.append(col(name).alias(safe))
                pylogger.debug("Renaming non-struct column %s -> %s to avoid conflict", name, safe)
    return df.select(*select_cols)

def find_array_column(df: DataFrame):
    """Return first array column name if any, else None."""
    for f in df.schema.fields:
        if isinstance(f.dataType, ArrayType):
            return f.name
    return None

def sanitize_column_names(df: DataFrame) -> DataFrame:
    """Ensure column names contain no dots and are unique. Apply deterministic suffixes if needed."""
    cols = list(df.columns)
    new_cols = []
    existing = set()
    for c in cols:
        clean = c.replace(".", "_")
        if clean in existing:
            clean = unique_name(clean, existing)
            pylogger.info("sanitize: renamed duplicate column to %s", clean)
        else:
            existing.add(clean)
        new_cols.append((c, clean))
    out = df
    for orig, new in new_cols:
        if orig != new:
            out = out.withColumnRenamed(orig, new)
    return out

def flatten_df(df: DataFrame, max_iters: int = 50) -> DataFrame:
    """
    Recursively flatten structs and explode arrays.
    """
    cur = df
    it = 0
    while it < max_iters:
        it += 1
        schema = cur.schema
        has_struct = any(isinstance(f.dataType, StructType) for f in schema.fields)
        arr_col = find_array_column(cur)
        if not has_struct and not arr_col:
            pylogger.info("Flattening complete after %d iterations", it-1)
            break
        if has_struct:
            pylogger.info("Iteration %d: expanding struct fields", it)
            cur = flatten_structs(cur)
            continue
        if arr_col:
            pylogger.info("Iteration %d: exploding array column '%s'", it, arr_col)
            cur = cur.withColumn(arr_col, explode_outer(col(arr_col)))
            continue
    if it >= max_iters:
        pylogger.warning("Reached max iterations in flatten_df; result may still contain nested types.")
    cur = sanitize_column_names(cur)
    return cur

# ----------------- Robust read_json: distributed Python parsing -----------------
def _object_pairs_lastwin(pairs):
    """object_pairs_hook that keeps the last value when duplicate keys occur."""
    d = {}
    for k, v in pairs:
        d[k] = v  # last write wins
    return d

def parse_json_line_safe(line: str):
    """Parse a JSON line string to python dict, return None on parse error."""
    try:
        s = line.strip()
        if not s:
            return None
        return json.loads(s, object_pairs_hook=_object_pairs_lastwin)
    except Exception:
        return None

def read_json(path: str):
    """
    Read JSON lines from S3 path via sc.textFile, parse with python json.loads (distributed),
    and convert to DataFrame. This avoids Spark's duplicate-key schema inference errors.
    """
    pylogger.info("Robust reading JSON via sc.textFile from %s", path)
    try:
        rdd = sc.textFile(path)
    except Exception as e:
        pylogger.exception("sc.textFile failed for path=%s : %s", path, e)
        raise

    parsed = rdd.map(lambda ln: ln.strip()).filter(lambda ln: ln != "").map(lambda ln: (ln, parse_json_line_safe(ln)))
    total = parsed.count()
    parsed_ok = parsed.filter(lambda t: t[1] is not None)
    parsed_bad = parsed.filter(lambda t: t[1] is None)
    ok_count = parsed_ok.count()
    bad_count = parsed_bad.count()
    pylogger.info("Parsed JSON lines: total=%d ok=%d failed=%d", total, ok_count, bad_count)

    if ok_count == 0:
        pylogger.warning("No valid JSON records parsed from %s", path)
        return spark.createDataFrame([], schema=None)

    dict_rdd = parsed_ok.map(lambda t: t[1])
    try:
        df = spark.createDataFrame(dict_rdd)
        pylogger.info("Created DataFrame from parsed JSON with columns: %s", df.columns)
    except Exception as e:
        pylogger.exception("Failed to create DataFrame from parsed JSON RDD: %s", e)
        sample = dict_rdd.take(1000)
        if not sample:
            return spark.createDataFrame([], schema=None)
        df = spark.createDataFrame(sample)
    return df

# ----------------- I/O write utilities -----------------
def write_df(df: DataFrame, out_uri: str):
    pylogger.info("Writing DataFrame to %s as %s (partition_by=%s coalesce=%d compression=%s)",
                  out_uri, OUTPUT_FORMAT, PARTITION_BY, COALESCE, COMPRESSION)

    if OUTPUT_FORMAT == 'parquet':
        writer = df.write.mode('overwrite')
        # guard: treat 'none'/'null' as no partitioning
        if PARTITION_BY and PARTITION_BY.lower() not in ("none", "null", "false", ""):
            parts = [c.strip() for c in PARTITION_BY.split(',') if c.strip()]
            if parts:
                writer = writer.partitionBy(*parts)
        if COMPRESSION and COMPRESSION != 'none':
            writer = writer.option('compression', COMPRESSION)
        if COALESCE > 0:
            df.coalesce(COALESCE).write.mode('overwrite').option('compression', COMPRESSION if COMPRESSION != 'none' else None).parquet(out_uri)
        else:
            writer.parquet(out_uri)

    elif OUTPUT_FORMAT == 'csv':
        if PARTITION_BY:
            pylogger.warning("CSV output requested; partition_by is ignored for CSV.")
        writer = df.write.mode('overwrite').option('header', 'true')
        if COMPRESSION and COMPRESSION != 'none':
            writer = writer.option('compression', COMPRESSION)
        if COALESCE > 0:
            df.coalesce(COALESCE).write.mode('overwrite').option('header', 'true').csv(out_uri)
        else:
            df.write.mode('overwrite').option('header', 'true').csv(out_uri)

    elif OUTPUT_FORMAT == 'json':
        if COALESCE > 0:
            df.coalesce(COALESCE).write.mode('overwrite').json(out_uri)
        else:
            df.write.mode('overwrite').json(out_uri)
    else:
        raise ValueError(f"Unsupported output format: {OUTPUT_FORMAT}")

# ----------------- Main processing per dataset -----------------
def process_one(input_path: str, output_prefix: str, label: str) -> bool:
    pylogger.info("START processing %s: input=%s output_prefix=%s", label, input_path, output_prefix)
    try:
        df = read_json(input_path)
        pylogger.info("%s: initial schema/cols: %s", label, ", ".join([f"{n}:{t}" for n,t in zip(df.columns, df.dtypes)]))
        flattened = flatten_df(df)
        pylogger.info("%s: flattened schema cols=%d", label, len(flattened.columns))

        out_uri = s3_uri(S3_BUCKET, output_prefix)
        write_df(flattened, out_uri)
        pylogger.info("%s: wrote flattened output to %s", label, out_uri)

        # write _SUCCESS marker under the output prefix
        write_s3_marker(S3_BUCKET, output_prefix, "_SUCCESS", f"{JOB_NAME} {label} {datetime.utcnow().isoformat()}")
        return True
    except Exception as e:
        pylogger.exception("%s: failed to process %s : %s", label, input_path, e)
        # write failure marker with stack trace
        write_s3_marker(S3_BUCKET, output_prefix, f"_FAILED_{JOB_NAME}.txt", traceback.format_exc())
        return False

# ----------------- Entrypoint -----------------
def main():
    pylogger.info("JOB START: %s", JOB_NAME)
    pylogger.info("Host: %s, Python: %s", socket.gethostname(), sys.version.replace('\n', ' '))
    pylogger.info("Parameters: S3_BUCKET=%s INPUT_PREFIX=%s REVIEW_KEY=%s META_KEY=%s REVIEW_OUTPUT=%s META_OUTPUT=%s OUTPUT_FORMAT=%s PARTITION_BY=%s COALESCE=%d COMPRESSION=%s FAIL_ON_ERROR=%s",
                  S3_BUCKET, INPUT_PREFIX, REVIEW_JSON_KEY, META_JSON_KEY, REVIEW_OUTPUT_PREFIX, META_OUTPUT_PREFIX,
                  OUTPUT_FORMAT, PARTITION_BY, COALESCE, COMPRESSION, FAIL_ON_ERROR)

    # Write a job started marker at input prefix
    try:
        write_s3_marker(S3_BUCKET, INPUT_PREFIX or '', f"_JOB_STARTED_{JOB_NAME}.txt",
                        f"{JOB_NAME} started at {datetime.utcnow().isoformat()}")
    except Exception:
        pylogger.exception("Failed writing job started marker")

    review_input = full_input_path(INPUT_PREFIX, REVIEW_JSON_KEY)
    meta_input = full_input_path(INPUT_PREFIX, META_JSON_KEY)
    pylogger.info("Computed input paths: review=%s meta=%s", review_input, meta_input)

    review_ok = process_one(review_input, REVIEW_OUTPUT_PREFIX, "review_dataset")
    meta_ok = process_one(meta_input, META_OUTPUT_PREFIX, "meta_dataset")

    pylogger.info("Processing summary: review_ok=%s meta_ok=%s", review_ok, meta_ok)

    if review_ok and meta_ok:
        pylogger.info("Both datasets processed successfully.")
        write_s3_marker(S3_BUCKET, REVIEW_OUTPUT_PREFIX, f"_JOB_FINISHED_{JOB_NAME}.txt", datetime.utcnow().isoformat())
        write_s3_marker(S3_BUCKET, META_OUTPUT_PREFIX, f"_JOB_FINISHED_{JOB_NAME}.txt", datetime.utcnow().isoformat())
    else:
        pylogger.error("One or more datasets failed. review_ok=%s meta_ok=%s", review_ok, meta_ok)
        write_s3_marker(S3_BUCKET, INPUT_PREFIX or '', f"_JOB_FAILED_{JOB_NAME}.txt",
                        f"review_ok={review_ok} meta_ok={meta_ok} time={datetime.utcnow().isoformat()}")

        # Fail the job (so Glue marks run as FAILED) unless user explicitly disabled it
        if FAIL_ON_ERROR:
            # raise SystemExit with non-zero code to ensure Glue marks FAILED
            raise SystemExit(f"One or more datasets failed: review_ok={review_ok} meta_ok={meta_ok}")
        else:
            pylogger.warning("FAIL_ON_ERROR is false; finishing job with SUCCEEDED state despite failures.")

    pylogger.info("JOB END: %s", JOB_NAME)

if __name__ == "__main__":
    main()