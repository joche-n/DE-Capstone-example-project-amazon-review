#!/usr/bin/env python3
"""
handler.py — Capstone amazon Lambda (SNS Subscriber)
------------------------------------------------------

Purpose:
- Invoked automatically when an SNS message is published.
- Processes Glue job completion notifications.
- Logs event details, and can trigger downstream tasks (e.g., dbt run, SSM update).

Supports two trigger types:
1. SNS → Lambda (production)
2. Direct invocation (for local or Step Functions testing)

Environment Variables:
  PROJECT_NAME    -> Project name tag for logs
  ENV             -> Environment name (dev/stg/prod)
  SNS_TOPIC_ARN   -> (optional) for re-publishing or debug notifications

Expected SNS Message (published by Step Functions):
{
  "Project": "capstone-amazon",
  "Environment": "dev",
  "GlueJob": "capstone-amazon-job",
  "Status": "SUCCEEDED",
  "StartTime": "2025-10-24T12:00:00Z",
  "EndTime": "2025-10-24T12:10:00Z"
}
"""

import os
import json
import boto3
import logging
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns = boto3.client("sns")

def parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize input into a flat dictionary:
    - If invoked via SNS, unwrap the SNS 'Message' JSON.
    - If direct invocation (e.g., Step Functions or test), return event as-is.
    """
    # SNS trigger case
    if "Records" in event and event["Records"][0].get("EventSource") == "aws:sns":
        sns_record = event["Records"][0]["Sns"]
        msg_str = sns_record.get("Message", "{}")
        try:
            message = json.loads(msg_str)
        except json.JSONDecodeError:
            logger.warning("SNS Message was not valid JSON: %s", msg_str)
            message = {"raw_message": msg_str}
        logger.info("Unwrapped SNS message: %s", json.dumps(message))
        return message

    # Direct trigger (test, Step Functions)
    logger.info("Direct invocation event detected.")
    return event


def lambda_handler(event, context):
    logger.info("Received raw event: %s", json.dumps(event))

    # Common metadata
    project = os.getenv("PROJECT_NAME", "capstone-amazon")
    env = os.getenv("ENV", "dev")

    # Parse event (handles SNS or direct)
    message = parse_event(event)

    glue_status = message.get("Status", message.get("status", "UNKNOWN"))
    job_name = message.get("GlueJob", message.get("glue_job_name", "N/A"))
    start_time = message.get("StartTime", message.get("start_time"))
    end_time = message.get("EndTime", message.get("end_time"))

    logger.info(
        "Processed Glue Job event — Job: %s | Status: %s | Start: %s | End: %s",
        job_name, glue_status, start_time, end_time
    )

    # Example placeholder for dbt or downstream trigger
    # You can add your own logic here later
    if glue_status.upper() == "SUCCEEDED":
        logger.info("Glue job succeeded — ready to trigger downstream task.")
        # e.g. trigger_dbt_run()
    elif glue_status.upper() == "FAILED":
        logger.warning("Glue job failed — check logs or retry policy.")
        # e.g. notify_ops_team()

    return {
        "Project": project,
        "Environment": env,
        "GlueJob": job_name,
        "Status": glue_status,
        "StartTime": start_time,
        "EndTime": end_time,
        "Result": "Lambda executed successfully"
    }
