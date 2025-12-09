"""
Mixin for managing VK Cloud S3 file status in PostgreSQL.
Uses forms_file_metadata tables (separate from sandbox file_metadata).
"""
from typing import Optional, List, Dict, Any


class VKCloudFileStatusMixin:
    """
    Mixin for managing VK Cloud S3 file status in PostgreSQL.
    Provides methods for updating statuses, tracking history, and getting metadata.

    Uses tables:
    - forms_file_metadata (current status)
    - forms_file_metadata_history (full history via trigger)
    - processing_status (status codes)
    - processing_error (error codes)
    - buckets (bucket registry)
    """

    def get_postgres_hook(self, database: str = "service_db"):
        """Must be implemented by the class using this mixin"""
        raise NotImplementedError("get_postgres_hook must be implemented")

    def check_vk_cloud_file_change(
        self,
        file_id: str,
        file_name: str,
        file_key: str,
        bucket_name: str,
        dir_name: str,
        last_modified,
        file_size: int,
        user_name: str = "financial_forms",
    ) -> int:
        """
        Check if VK Cloud file has changed and update metadata.
        Calls check_update_forms_file_s3() stored function.

        Args:
            file_id: Unique file identifier (GUID)
            file_name: Name of the file
            file_key: Full S3 key
            bucket_name: S3 bucket name
            dir_name: Directory/prefix in bucket (not used in current schema)
            last_modified: Last modification timestamp
            file_size: File size in bytes
            user_name: User/creator name

        Returns:
            int: 1=new, 2=changed, 3=error_extension, 4=no_change
        """
        hook = self.get_postgres_hook(database="service_db")
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            sql = """
                SELECT public.check_update_forms_file_s3(
                    %s::VARCHAR, %s::VARCHAR, %s::VARCHAR,
                    %s::VARCHAR, %s::TIMESTAMP, %s::BIGINT, %s::VARCHAR
                );
            """
            cur.execute(sql, (
                file_name, user_name, file_id,
                bucket_name, last_modified, file_size, file_key
            ))
            result = cur.fetchone()
            conn.commit()
            return result[0] if result else 4
        except Exception as e:
            conn.rollback()
            self.log.error(f"Error checking VK Cloud file {file_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    def update_vk_cloud_file_status(
        self,
        file_id: str,
        status_code: str,
        error_code: Optional[str] = None
    ) -> bool:
        """
        Update VK Cloud file status.
        Calls update_status_forms_file_s3() stored function.

        Args:
            file_id: Unique file identifier (GUID)
            status_code: Status code. Supported values:
                - 'file_detected' (mapped from FILE_DETECTED)
                - 'reading_file' (mapped from VALIDATING)
                - 'file_load_to_store' / 'stage_db_load_completed' (mapped from LOADING_STAGE)
                - 'sandbox_db_load' (mapped from LOADING_RDV)
                - 'processing_completed' (mapped from COMPLETED)
                - 'processing_failed' (mapped from ERROR)
                - Error codes: 'extension_error', 'merged_cells_detected', etc.
            error_code: Optional error code (not used, status_code handles errors)

        Returns:
            bool: True if successful
        """
        # Map generic status codes to forms-specific codes
        status_mapping = {
            'FILE_DETECTED': 'file_detected',
            'VALIDATING': 'reading_file',
            'LOADING_STAGE': 'stage_db_load_completed',
            'LOADING_RDV': 'sandbox_db_load',
            'LOADING_BDV': 'sandbox_db_load',
            'COMPLETED': 'processing_completed',
            'ERROR': 'processing_failed',
        }

        # Map error codes
        error_mapping = {
            'ERR_EXTENSION': 'extension_error',
            'ERR_FILENAME': 'extension_error',
            'ERR_COLUMNS': 'merged_cells_detected',
            'ERR_HEADER': 'merged_cells_detected',
            'ERR_LOAD_STAGE': 'processing_failed',
            'ERR_LOAD_RDV': 'processing_failed',
            'ERR_LOAD_BDV': 'processing_failed',
            'ERR_PARSE': 'merged_cells_detected',
            'ERR_S3_ACCESS': 's3_connection_failed',
            'ERR_UNKNOWN': 'processing_failed',
        }

        # Use error code if provided, otherwise use status code
        actual_code = status_code
        if error_code and error_code in error_mapping:
            actual_code = error_mapping[error_code]
        elif status_code in status_mapping:
            actual_code = status_mapping[status_code]

        try:
            hook = self.get_postgres_hook(database="service_db")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    sql = "SELECT public.update_status_forms_file_s3(%s, %s);"
                    cur.execute(sql, (file_id, actual_code))
                    result = cur.fetchone()
                    conn.commit()
                    self.log.info(
                        f"VK Cloud file {file_id} status updated to {actual_code}"
                    )
                    return result[0] if result else False
        except Exception as e:
            self.log.error(f"Error updating VK Cloud file status {file_id}: {e}")
            return False

    def get_missing_vk_cloud_files(
        self,
        current_keys: List[str],
        bucket_names: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get files that exist in DB but are missing from S3.
        Calls get_missing_s3_forms_files() stored function.

        Args:
            current_keys: List of current S3 keys
            bucket_names: Optional list of bucket names to filter by.
                         If None, returns all missing files (backward compatible).
                         If specified, only returns missing files from these buckets.

        Returns:
            List of missing file records
        """
        missing_files = []
        try:
            hook = self.get_postgres_hook(database="service_db")
            conn = hook.get_conn()
            cur = conn.cursor()

            cur.execute(
                "SELECT * FROM public.get_missing_s3_forms_files(%s)",
                (current_keys,)
            )
            rows = cur.fetchall()

            missing_files = [
                {
                    "key": r[0],
                    "file_name": r[1],
                    "user_name": r[2],
                    "guid": r[3],
                    "bucket": r[4],
                    "table_name": r[5],
                    "status": "missing",
                }
                for r in rows
                if bucket_names is None or r[4] in bucket_names  # Filter by bucket
            ]
            conn.commit()

            if missing_files:
                self.log.info(f"Found {len(missing_files)} missing VK Cloud files")

        except Exception as e:
            if 'conn' in locals():
                conn.rollback()
            self.log.error(f"Error getting missing VK Cloud files: {e}")
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

        return missing_files

    def delete_vk_cloud_file_metadata(self, file_id: str) -> bool:
        """
        Delete VK Cloud file metadata from database.

        Args:
            file_id: Unique file identifier

        Returns:
            bool: True if successful
        """
        try:
            hook = self.get_postgres_hook(database="service_db")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    sql = "DELETE FROM public.forms_file_metadata WHERE file_id = %s;"
                    cur.execute(sql, (file_id,))
                    conn.commit()
                    return cur.rowcount > 0
        except Exception as e:
            self.log.error(f"Error deleting VK Cloud file metadata {file_id}: {e}")
            return False

    def get_vk_cloud_file_status(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current status of a VK Cloud file.

        Args:
            file_id: Unique file identifier

        Returns:
            Dict with file status info or None if not found
        """
        try:
            hook = self.get_postgres_hook(database="service_db")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    sql = """
                        SELECT
                            fm.file_id,
                            fm.file_name,
                            fm.size,
                            b.bucket_name,
                            fm.key,
                            ps.code as status_code,
                            ps.description as status_description,
                            pe.code as error_code,
                            pe.description as error_description,
                            fm.last_modified,
                            fm.updated_at
                        FROM public.forms_file_metadata fm
                        JOIN public.buckets b ON fm.bucket_id = b.bucket_id
                        LEFT JOIN public.processing_status ps
                            ON fm.file_load_status_id = ps.status_id
                        LEFT JOIN public.processing_error pe
                            ON fm.error_id = pe.error_id
                        WHERE fm.file_id = %s;
                    """
                    cur.execute(sql, (file_id,))
                    row = cur.fetchone()

                    if row:
                        return {
                            "file_id": row[0],
                            "file_name": row[1],
                            "file_size": row[2],
                            "bucket_name": row[3],
                            "file_key": row[4],
                            "status_code": row[5],
                            "status_description": row[6],
                            "error_code": row[7],
                            "error_description": row[8],
                            "last_modified": row[9],
                            "updated_at": row[10],
                        }
                    return None
        except Exception as e:
            self.log.error(f"Error getting VK Cloud file status {file_id}: {e}")
            return None

    def get_vk_cloud_file_history(
        self,
        file_id: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get status history for a VK Cloud file.

        Args:
            file_id: Unique file identifier
            limit: Maximum number of history records

        Returns:
            List of status history records
        """
        try:
            hook = self.get_postgres_hook(database="service_db")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    sql = """
                        SELECT
                            h.file_id,
                            h.file_name,
                            ps.code as status_code,
                            ps.description as status_description,
                            pe.code as error_code,
                            pe.description as error_description,
                            h.updated_at
                        FROM public.forms_file_metadata_history h
                        LEFT JOIN public.processing_status ps
                            ON h.file_load_status_id = ps.status_id
                        LEFT JOIN public.processing_error pe
                            ON h.error_id = pe.error_id
                        WHERE h.file_id = %s
                        ORDER BY h.updated_at DESC
                        LIMIT %s;
                    """
                    cur.execute(sql, (file_id, limit))
                    rows = cur.fetchall()

                    return [
                        {
                            "file_id": row[0],
                            "file_name": row[1],
                            "status_code": row[2],
                            "status_description": row[3],
                            "error_code": row[4],
                            "error_description": row[5],
                            "updated_at": row[6],
                        }
                        for row in rows
                    ]
        except Exception as e:
            self.log.error(f"Error getting VK Cloud file history {file_id}: {e}")
            return []
