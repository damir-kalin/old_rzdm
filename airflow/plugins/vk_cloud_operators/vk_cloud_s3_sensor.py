"""
Sensor for monitoring file changes in VK Cloud S3 Object Storage.
Separate from MinIO sensor for clear separation of concerns.
"""
import pandas as pd
import re
import uuid
from datetime import datetime
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class VKCloudS3Sensor(BaseSensorOperator):
    """
    Sensor for monitoring file changes in VK Cloud S3 buckets.
    Specifically designed for VK Cloud Object Storage (hb.ru-msk.vkcloud-storage.ru).

    Unlike MinioFileSensor, this sensor:
    - Uses VK Cloud S3 connection by default
    - Does NOT rely on Postgres metadata tracking (simpler flow)
    - Treats all matching files as "new" for processing
    - Designed for financial forms processing
    """

    template_fields = ('s3_prefix', 's3_buckets')

    def __init__(
        self,
        s3_conn_id: str = "rzdm_lake_s3",
        s3_buckets: list = None,
        s3_prefix: str = "",
        file_pattern: str = None,
        **kwargs,
    ):
        """
        Args:
            s3_conn_id: ID of VK Cloud S3 connection (default: rzdm_lake_s3)
            s3_buckets: List of buckets to monitor
            s3_prefix: Prefix (folder) in S3 to filter files
            file_pattern: Optional regex pattern to filter files (e.g., r'форма.*2025.*\.xls')
            **kwargs: Additional parameters for BaseSensorOperator
        """
        super().__init__(**kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_prefix = s3_prefix
        self.s3_buckets = s3_buckets or ["rzdm-dev-data-lake"]
        self.file_pattern = file_pattern

    def get_s3_hook(self):
        """Get VK Cloud S3 hook"""
        return S3Hook(aws_conn_id=self.s3_conn_id)

    def is_financial_form(self, file_name: str) -> bool:
        """Detect if file is a financial form (Form 1 or Form 2, 2025 year)"""
        patterns = [
            r'форма\s*[12].*2025.*\.xls[x]?$',
            r'form\s*[12].*2025.*\.xls[x]?$',
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
        hook = self.get_s3_hook()
        objects = []

        for bucket in self.s3_buckets:
            try:
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
                            guid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"vk_cloud_{clean_key}"))
                        else:
                            # Parse standard file name pattern
                            file_name = clean_key
                            user_name = "vk_cloud"
                            guid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"vk_cloud_{clean_key}"))

                        # Get object metadata
                        obj = hook.get_key(key, bucket_name=bucket)
                        objects.append({
                            "key": key,
                            "file_name": file_name,
                            "user_name": user_name,
                            "guid": guid,
                            "bucket": bucket,
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
                "key", "file_name", "user_name", "guid", "bucket",
                "table_name", "last_modified", "size", "is_financial_form"
            ]
        ) if objects else pd.DataFrame()

    def poke(self, context):
        """Check for files in VK Cloud S3"""
        current_df = self._get_file_info()

        if current_df.empty:
            self.log.info("No files found in VK Cloud S3 buckets")
            return False

        self.log.info(f"Found {len(current_df)} files in VK Cloud S3:")
        for _, row in current_df.iterrows():
            self.log.info(f"  - {row['file_name']} ({row['size']} bytes)")

        # All files are treated as "new" for processing
        processed_files = []
        for _, row in current_df.iterrows():
            file_info = {
                "key": row["key"],
                "file_name": row["file_name"],
                "user_name": row["user_name"],
                "guid": row["guid"],
                "bucket": row["bucket"],
                "table_name": row["table_name"],
                "last_modified": (
                    row["last_modified"].strftime("%Y-%m-%d %H:%M:%S")
                    if pd.notna(row["last_modified"]) else None
                ),
                "size": int(row["size"]),
                "status": "new",
                "is_financial_form": row["is_financial_form"],
            }
            processed_files.append(file_info)

        if processed_files:
            self.log.info(f"Pushing {len(processed_files)} files to XCom for processing")
            context["ti"].xcom_push(key="changed_files", value=processed_files)
            return True

        return False
