"""
Sensor for monitoring file changes in VK Cloud S3 Object Storage.
Separate from MinIO sensor for clear separation of concerns.
Uses report_routine_loading_* tables for logging (separate from sandbox).
"""
import pandas as pd
import re
import uuid
from datetime import datetime
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from vk_cloud_operators.vk_cloud_file_status_mixin import VKCloudFileStatusMixin


class VKCloudS3Sensor(BaseSensorOperator, VKCloudFileStatusMixin):
    """
    Sensor for monitoring file changes in VK Cloud S3 buckets.
    Specifically designed for VK Cloud Object Storage (hb.ru-msk.vkcloud-storage.ru).

    Features:
    - Uses VK Cloud S3 connection by default
    - Tracks file status in PostgreSQL (report_routine_loading_* tables)
    - Detects new/changed/missing files
    - Designed for financial forms processing (Form 1, Form 2)
    """

    template_fields = ('s3_prefix', 's3_buckets')

    def __init__(
        self,
        s3_conn_id: str = "rzdm_lake_s3",
        s3_buckets: list = None,
        s3_bucket_connections: dict = None,
        s3_prefix: str = "",
        file_pattern: str = None,
        postgres_conn_id: str = "airflow_db",
        enable_logging: bool = True,
        **kwargs,
    ):
        """
        Args:
            s3_conn_id: ID of VK Cloud S3 connection (default: rzdm_lake_s3)
            s3_buckets: List of buckets to monitor
            s3_bucket_connections: Dict mapping bucket names to connection IDs (optional)
            s3_prefix: Prefix (folder) in S3 to filter files
            file_pattern: Optional regex pattern to filter files
            postgres_conn_id: ID of PostgreSQL connection for logging
            enable_logging: Enable/disable PostgreSQL logging (default: True)
            **kwargs: Additional parameters for BaseSensorOperator
        """
        super().__init__(**kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_prefix = s3_prefix
        self.s3_buckets = s3_buckets or ["rzdm-dev-data-lake"]
        self.s3_bucket_connections = s3_bucket_connections or {}
        self.file_pattern = file_pattern
        self.postgres_conn_id = postgres_conn_id
        self.enable_logging = enable_logging

    def get_postgres_hook(self, database: str = "service_db"):
        """Get PostgreSQL hook for logging"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        return PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            database=database
        )

    def get_s3_hook(self, bucket_name: str = None):
        """Get VK Cloud S3 hook for specific bucket"""
        # Use bucket-specific connection if available
        if bucket_name and bucket_name in self.s3_bucket_connections:
            conn_id = self.s3_bucket_connections[bucket_name]
            return S3Hook(aws_conn_id=conn_id)
        # Fall back to default connection
        return S3Hook(aws_conn_id=self.s3_conn_id)

    def is_financial_form(self, file_name: str) -> bool:
        """Detect if file is a financial form (Form 1 or Form 2, any year, any period)"""
        patterns = [
            r'форма\s*[12].*\d{4}.*\.xls[xmb]?$',
            r'form\s*[12].*\d{4}.*\.xls[xmb]?$',
        ]
        file_name_lower = file_name.lower()
        return any(re.search(pattern, file_name_lower) for pattern in patterns)

    def matches_pattern(self, file_name: str) -> bool:
        """Check if file matches the configured pattern"""
        if not self.file_pattern:
            return True
        return bool(re.search(self.file_pattern, file_name, re.IGNORECASE))

    def _get_start_date(self, context):
        """Get start_date as datetime object"""
        start_date = context.get('start_date')
        if isinstance(start_date, str):
            try:
                return datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            except ValueError:
                return datetime.utcnow()
        return start_date

    def execute(self, context):
        """Override execute to bypass run_duration issues"""
        if 'start_date' in context and isinstance(context['start_date'], str):
            context['start_date'] = self._get_start_date(context)
        return self.poke(context)

    def _get_file_info(self):
        """Get information about current files in VK Cloud S3"""
        objects = []

        for bucket in self.s3_buckets:
            try:
                # Get bucket-specific S3 hook
                hook = self.get_s3_hook(bucket)
                keys = hook.list_keys(bucket_name=bucket, prefix=self.s3_prefix)
                if keys:
                    for key in keys:
                        # Skip folder entries
                        if key.endswith('/'):
                            continue

                        # Get clean file name without prefix
                        clean_key = key[len(self.s3_prefix):] if key.startswith(self.s3_prefix) else key

                        # Skip if doesn't match pattern
                        if not self.matches_pattern(clean_key):
                            continue

                        # Check if financial form
                        is_financial_form = self.is_financial_form(clean_key)

                        if is_financial_form:
                            file_name = clean_key
                            user_name = "financial_forms"
                            guid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"vk_cloud_{key}"))
                        else:
                            file_name = clean_key
                            user_name = "vk_cloud"
                            guid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"vk_cloud_{key}"))

                        # Get object metadata
                        obj = hook.get_key(key, bucket_name=bucket)
                        objects.append({
                            "key": key,
                            "file_name": file_name,
                            "user_name": user_name,
                            "guid": guid,
                            "bucket": bucket,
                            "dir_name": self.s3_prefix,
                            "table_name": file_name.replace(" ", "_").replace(".", "_"),
                            "last_modified": pd.to_datetime(obj.last_modified),
                            "size": int(obj.content_length),
                            "is_financial_form": is_financial_form,
                        })

            except Exception as e:
                self.log.error(f"Error listing bucket {bucket}: {e}")
                continue

        return pd.DataFrame(
            objects,
            columns=[
                "key", "file_name", "user_name", "guid", "bucket", "dir_name",
                "table_name", "last_modified", "size", "is_financial_form"
            ]
        ) if objects else pd.DataFrame()

    def poke(self, context):
        """Check for files in VK Cloud S3 with logging"""
        current_df = self._get_file_info()

        # Get missing files from DB (if logging enabled)
        missing_files = []
        if self.enable_logging and not current_df.empty:
            current_keys = current_df["key"].tolist()
            try:
                missing_files = self.get_missing_vk_cloud_files(current_keys, self.s3_buckets)
                if missing_files:
                    self.log.info(f"Found {len(missing_files)} files missing from S3")
                    context["ti"].xcom_push(key="missing_files", value=missing_files)
            except Exception as e:
                self.log.warning(f"Could not check missing files (logging may not be set up): {e}")

        if current_df.empty:
            self.log.info("No files found in VK Cloud S3 buckets")
            return bool(missing_files)

        self.log.info(f"Found {len(current_df)} files in VK Cloud S3:")
        for _, row in current_df.iterrows():
            self.log.info(f"  - {row['file_name']} ({row['size']} bytes)")

        # Check each file status
        new_files = []
        changed_files = []
        no_change_files = []
        error_files = []

        for _, row in current_df.iterrows():
            file_info = {
                "key": row["key"],
                "file_name": row["file_name"],
                "user_name": row["user_name"],
                "guid": row["guid"],
                "bucket": row["bucket"],
                "dir_name": row["dir_name"],
                "table_name": row["table_name"],
                "last_modified": (
                    row["last_modified"].strftime("%Y-%m-%d %H:%M:%S")
                    if pd.notna(row["last_modified"]) else None
                ),
                "size": int(row["size"]),
                "is_financial_form": row["is_financial_form"],
            }

            if self.enable_logging:
                try:
                    # Check file status in PostgreSQL
                    result = self.check_vk_cloud_file_change(
                        file_id=row["guid"],
                        file_name=row["file_name"],
                        file_key=row["key"],
                        bucket_name=row["bucket"],
                        dir_name=row["dir_name"],
                        last_modified=row["last_modified"],
                        file_size=row["size"],
                    )

                    if result == 1:
                        file_info["status"] = "new"
                        new_files.append(file_info)
                    elif result == 2:
                        file_info["status"] = "changed"
                        changed_files.append(file_info)
                    elif result == 3:
                        file_info["status"] = "error"
                        error_files.append(file_info)
                    else:  # result == 4
                        file_info["status"] = "no_change"
                        no_change_files.append(file_info)

                except Exception as e:
                    self.log.warning(
                        f"Could not check file status for {row['file_name']} "
                        f"(logging may not be set up): {e}"
                    )
                    # Fallback: treat as new
                    file_info["status"] = "new"
                    new_files.append(file_info)
            else:
                # No logging - treat all as new
                file_info["status"] = "new"
                new_files.append(file_info)

        # Log results
        self._log_results(new_files, changed_files, no_change_files, error_files)

        # Push files for processing
        processed_files = new_files + changed_files
        if processed_files:
            self.log.info(f"Pushing {len(processed_files)} files to XCom for processing")
            context["ti"].xcom_push(key="changed_files", value=processed_files)

        return bool(processed_files) or bool(missing_files)

    def _log_results(self, new_files, changed_files, no_change_files, error_files):
        """Log file detection results"""
        self.log.info("=" * 60)
        self.log.info("VK CLOUD S3 SENSOR RESULTS")
        self.log.info("=" * 60)
        self.log.info(f"New files: {len(new_files)}")
        self.log.info(f"Changed files: {len(changed_files)}")
        self.log.info(f"Unchanged files: {len(no_change_files)}")
        self.log.info(f"Error files: {len(error_files)}")

        if new_files:
            self.log.info("New files:")
            for f in new_files:
                self.log.info(f"  - {f['file_name']}")

        if changed_files:
            self.log.info("Changed files:")
            for f in changed_files:
                self.log.info(f"  - {f['file_name']}")

        if error_files:
            self.log.warning("Files with errors:")
            for f in error_files:
                self.log.warning(f"  - {f['file_name']}")

        self.log.info("=" * 60)
