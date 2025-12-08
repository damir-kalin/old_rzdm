"""
VK Cloud S3 operators for monitoring files in VK Cloud Object Storage
"""
from vk_cloud_operators.vk_cloud_s3_sensor import VKCloudS3Sensor
from vk_cloud_operators.financial_forms_operator import FinancialFormsOperator

__all__ = [
    'VKCloudS3Sensor',
    'FinancialFormsOperator',
]
