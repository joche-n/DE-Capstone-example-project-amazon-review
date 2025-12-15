#!/usr/bin/env python3
"""
update_trust.py

Usage:
  ./update_trust.py <role-name> <snowflake-iam-user-arn> <snowflake-external-id>

This script updates the assume-role-policy for an existing IAM role to allow only the
given Snowflake IAM user ARN to assume the role, and requires the provided ExternalId.

Requirements:
  - Python 3.8+
  - boto3 installed: pip install boto3
  - AWS credentials available in environment (or via instance/profile) with iam:UpdateAssumeRolePolicy permission
"""

from __future__ import annotations
import sys
import json
import logging
import tempfile
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound

LOG = logging.getLogger("update_trust")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def build_trust_policy(sf_iam_user_arn: str, sf_external_id: str) -> dict:
    """Return assume-role-policy document dict restricting to Snowflake principal + ExternalId."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": sf_iam_user_arn},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": sf_external_id}},
            }
        ],
    }


def update_assume_role_policy(role_name: str, policy_doc: dict, iam_client) -> None:
    """Call AWS IAM to update the assume role policy."""
    policy_json = json.dumps(policy_doc)
    LOG.info("Updating assume-role-policy for role: %s", role_name)
    try:
        iam_client.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=policy_json,
        )
    except ClientError as e:
        LOG.error("AWS ClientError while updating assume-role-policy: %s", e)
        raise
    LOG.info("Successfully updated assume-role-policy for role: %s", role_name)


def main(argv: Optional[list[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if len(argv) != 3:
        LOG.error("Usage: %s <role-name> <snowflake-iam-user-arn> <snowflake-external-id>", sys.argv[0])
        return 2

    role_name, sf_iam_user_arn, sf_external_id = argv

    if not sf_iam_user_arn or not sf_external_id:
        LOG.info("Snowflake principal or external id empty; skipping trust update.")
        return 0

    LOG.info("Role: %s", role_name)
    LOG.info("Snowflake IAM user ARN: %s", sf_iam_user_arn)
    LOG.info("Snowflake External ID: %s", sf_external_id)

    try:
        iam_client = boto3.client("iam")
    except (NoCredentialsError, ProfileNotFound) as e:
        LOG.error("AWS credentials/configuration error: %s", e)
        return 3

    policy = build_trust_policy(sf_iam_user_arn, sf_external_id)

    # write temp file for debugging/forensics (optional)
    with tempfile.NamedTemporaryFile("w", prefix="sf-trust-", suffix=".json", delete=False) as tmp:
        json.dump(policy, tmp, indent=2)
        tmp_path = tmp.name

    LOG.info("Temporary trust policy written to %s", tmp_path)

    try:
        update_assume_role_policy(role_name, policy, iam_client)
    except Exception:
        LOG.error("Failed to update assume-role-policy.")
        # keep temp file for debugging
        LOG.info("Trust policy left at %s for inspection", tmp_path)
        return 4

    # remove temp file on success
    try:
        import os
        os.remove(tmp_path)
    except Exception:
        LOG.debug("Could not remove temp file %s (non-fatal).", tmp_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())