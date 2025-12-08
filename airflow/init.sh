airflow connections add starrocks_default \
--conn-type 'mysql' \
--conn-host 'kube-starrocks-fe-service' \
--conn-port 9030 \
--conn-login 'root' \
--conn-password 'Q1w2e3r+'

airflow connections add airflow_db \
--conn-type 'postgres' \
--conn-host 'postgres-airflow' \
--conn-port 5432 \
--conn-login 'airflow' \
--conn-password 'Q1w2e3r+'

airflow connections add minio_default \
--conn-type 'aws' \
--conn-host 'minio-svc' \
--conn-port 9000 \
--conn-login 'minioadmin' \
--conn-password 'Q1w2e3r+' \
--conn-extra '{"endpoint_url": "http://minio-svc:9000", "region_name": "ru-central1"}'