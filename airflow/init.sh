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

# VK Cloud S3 connections for test environment buckets
airflow connections add rzdm_indefinite_s3 \
--conn-type 'aws' \
--conn-login 'INDEFINITE_ACCESS_KEY' \
--conn-password 'INDEFINITE_SECRET_KEY' \
--conn-extra '{"endpoint_url": "https://hb.ru-msk.vkcs.cloud", "region_name": "ru-msk"}'

airflow connections add rzdm_infoclinic_s3 \
--conn-type 'aws' \
--conn-login 'INFOCLINIC_ACCESS_KEY' \
--conn-password 'INFOCLINIC_SECRET_KEY' \
--conn-extra '{"endpoint_url": "https://hb.ru-msk.vkcs.cloud", "region_name": "ru-msk"}'

airflow connections add rzdm_asb_s3 \
--conn-type 'aws' \
--conn-login 'ASB_ACCESS_KEY' \
--conn-password 'ASB_SECRET_KEY' \
--conn-extra '{"endpoint_url": "https://hb.ru-msk.vkcs.cloud", "region_name": "ru-msk"}'

airflow connections add rzdm_asckz_s3 \
--conn-type 'aws' \
--conn-login 'ASCKZ_ACCESS_KEY' \
--conn-password 'ASCKZ_SECRET_KEY' \
--conn-extra '{"endpoint_url": "https://hb.ru-msk.vkcs.cloud", "region_name": "ru-msk"}'

airflow connections add rzdm_kuirzp_s3 \
--conn-type 'aws' \
--conn-login 'KUIRZP_ACCESS_KEY' \
--conn-password 'KUIRZP_SECRET_KEY' \
--conn-extra '{"endpoint_url": "https://hb.ru-msk.vkcs.cloud", "region_name": "ru-msk"}'

airflow connections add rzdm_nsi_s3 \
--conn-type 'aws' \
--conn-login 'NSI_ACCESS_KEY' \
--conn-password 'NSI_SECRET_KEY' \
--conn-extra '{"endpoint_url": "https://hb.ru-msk.vkcs.cloud", "region_name": "ru-msk"}'

airflow connections add rzdm_buinu_s3 \
--conn-type 'aws' \
--conn-login 'BUINU_ACCESS_KEY' \
--conn-password 'BUINU_SECRET_KEY' \
--conn-extra '{"endpoint_url": "https://hb.ru-msk.vkcs.cloud", "region_name": "ru-msk"}'