"""
S3-compatible storage abstraction for Cloudflare R2.

Uses boto3 with an endpoint_url parameter to support R2's S3-compatible API.
All operations work with in-memory bytes (no disk writes).
"""
import logging
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError, BotoCoreError


class S3Storage:
    """
    Abstraction layer over boto3 for S3-compatible object storage (Cloudflare R2).

    Provides:
    - list_keys: List all objects under a prefix (with pagination)
    - get_bytes: Download an object as bytes
    - put_bytes: Upload bytes to a key
    - move_object: Copy + delete (rename/move within bucket)
    """

    def __init__(
        self,
        bucket_name: str,
        access_key: str,
        secret_key: str,
        endpoint_url: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the S3 client.

        Supports:
        - AWS S3: leave endpoint_url as None (uses AWS default endpoint)
        - Cloudflare R2: provide the R2 endpoint URL
        - MinIO (on-premise): provide the MinIO endpoint URL

        Args:
            bucket_name: Name of the S3 bucket (e.g. 'app-bucket')
            access_key: S3-compatible access key ID
            secret_key: S3-compatible secret access key
            endpoint_url: Optional custom endpoint (for R2/MinIO).
                          If None, uses AWS S3 default.
            logger: Optional logger instance
        """
        self.bucket_name = bucket_name
        self.logger = logger or logging.getLogger(__name__)

        # Build client kwargs — only add endpoint_url if it's provided
        client_kwargs = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
        }
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        self.client = boto3.client("s3", **client_kwargs)

    # ── List objects ──────────────────────────────────────────────────────

    def list_keys(self, prefix: str) -> List[str]:
        """
        List all object keys under a given prefix.

        Handles pagination for buckets with more than 1000 objects.

        Args:
            prefix: S3 key prefix (e.g. 'unparsing/' or 'unparsing/2024/')

        Returns:
            List of full S3 keys (strings)
        """
        keys: List[str] = []
        continuation_token: Optional[str] = None

        try:
            while True:
                params = {
                    "Bucket": self.bucket_name,
                    "Prefix": prefix,
                }
                if continuation_token:
                    params["ContinuationToken"] = continuation_token

                response = self.client.list_objects_v2(**params)

                if "Contents" in response:
                    for obj in response["Contents"]:
                        keys.append(obj["Key"])

                if response.get("IsTruncated"):
                    continuation_token = response.get("NextContinuationToken")
                else:
                    break

            self.logger.info(
                f"Listed {len(keys)} objects under prefix '{prefix}'."
            )
            return keys

        except (ClientError, BotoCoreError) as e:
            self.logger.error(
                f"Error listing objects under '{prefix}': {e}", exc_info=True
            )
            return []

    # ── Download object ───────────────────────────────────────────────────

    def get_bytes(self, key: str) -> Optional[bytes]:
        """
        Download an object's content as bytes (in-memory).

        Args:
            key: Full S3 key of the object

        Returns:
            Bytes content, or None if the object doesn't exist or an error occurs.
        """
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            content = response["Body"].read()
            self.logger.debug(f"Downloaded {len(content)} bytes from '{key}'.")
            return content
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "NoSuchKey":
                self.logger.warning(f"Object not found: '{key}'")
            else:
                self.logger.error(
                    f"Error downloading '{key}': {e}", exc_info=True
                )
            return None
        except BotoCoreError as e:
            self.logger.error(
                f"Network error downloading '{key}': {e}", exc_info=True
            )
            return None

    # ── Upload object ─────────────────────────────────────────────────────

    def put_bytes(self, key: str, content: bytes) -> bool:
        """
        Upload bytes to an S3 key.

        Args:
            key: Destination S3 key
            content: Bytes to upload

        Returns:
            True if successful, False otherwise.
        """
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=content,
            )
            self.logger.info(
                f"Uploaded {len(content)} bytes to '{key}'."
            )
            return True
        except (ClientError, BotoCoreError) as e:
            self.logger.error(
                f"Error uploading to '{key}': {e}", exc_info=True
            )
            return False

    # ── Move object (copy + delete) ───────────────────────────────────────

    def move_object(self, src_key: str, dest_key: str) -> bool:
        """
        Move/rename an object within the same bucket.

        Performs a copy followed by a delete of the source.

        Args:
            src_key: Current S3 key
            dest_key: Destination S3 key

        Returns:
            True if successful, False otherwise.
        """
        try:
            copy_source = {"Bucket": self.bucket_name, "Key": src_key}

            self.client.copy_object(
                Bucket=self.bucket_name,
                CopySource=copy_source,
                Key=dest_key,
            )

            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=src_key,
            )

            self.logger.info(f"Moved '{src_key}' → '{dest_key}'.")
            return True

        except (ClientError, BotoCoreError) as e:
            self.logger.error(
                f"Error moving '{src_key}' to '{dest_key}': {e}",
                exc_info=True,
            )
            return False