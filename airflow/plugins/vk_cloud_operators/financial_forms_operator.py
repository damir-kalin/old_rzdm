"""
Financial Forms Operator - NEW VERSION
Loads Form 1 and Form 2 data into static tables: stg_buinu.form_1_assets and stg_buinu.form_2_financial
Based on new_parser.py logic (2025-12-07)
"""

from __future__ import annotations

import re
from io import BytesIO
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import numpy as np
import pandas as pd
from airflow.models import BaseOperator

from base_operators.base_hooks_operator import BaseHooksOperator
from starrocks_operators.starrocks_connection_mixin import StarRocksConnectionMixin
from starrocks_operators.starrocks_data_converter import StarRocksDataConverter
from vk_cloud_operators.vk_stream_loader import VKStreamLoader
from vk_cloud_operators.vk_cloud_file_status_mixin import VKCloudFileStatusMixin


# Pattern identifier dictionary - detects document type and year from content
PATTERN_IDENTIFIER_DICT = {
    'accounting_balance_form_1_2025': {
        'document_name': 'БУХГАЛТЕРСКИЙ БАЛАНС',
        'document_name_cell': (0, 4),
        'document_year': '2025',
        'document_year_cell': (1, 3),
        'form_type': 'form_1'
    },
    'accounting_balance_form_1_2024': {
        'document_name': 'БУХГАЛТЕРСКИЙ БАЛАНС',
        'document_name_cell': (1, 1),
        'document_year': '2024',
        'document_year_cell': (2, 2),
        'form_type': 'form_1'
    },
    'financial_results_report_form_2_2025': {
        'document_name': 'ОТЧЕТ О ФИНАНСОВЫХ РЕЗУЛЬТАТАХ',
        'document_name_cell': (0, 4),
        'document_year': '2025',
        'document_year_cell': (1, 5),
        'form_type': 'form_2'
    },
    'financial_results_report_form_2_2024': {
        'document_name': 'ОТЧЕТ О ПРИБЫЛЯХ И УБЫТКАХ',
        'document_name_cell': (1, 1),
        'document_year': '2024',
        'document_year_cell': (2, 1),
        'form_type': 'form_2'
    },
}

# Column header patterns - numbered columns (1, 2, 3, 4, 5)
COLUMN_HEADER_NAME_PATTERN_DICT = {
    'accounting_balance_form_1_2025': {0: '1', 6: '2', 7: '3', 8: '4', 9: '5'},
    'accounting_balance_form_1_2024': {0: '1', 1: '2', 2: '3', 3: '4', 6: '5'},
    'financial_results_report_form_2_2025': {0: '1', 6: '2', 7: '3', 8: '4'},
    'financial_results_report_form_2_2024': {0: '1', 1: '2', 2: '3', 3: '4'},
}

# Final row patterns
COLUMN_FINAL_ROW_PATTERN_DICT = {
    'accounting_balance_form_1_2025': {0: 'БАЛАНС', 6: '1700'},
    'accounting_balance_form_1_2024': {0: 'БАЛАНС', 1: '1700'},
    'financial_results_report_form_2_2024': {0: 'Разводненная прибыль (убыток) на акцию', 6: '2910'},
    'financial_results_report_form_2_2025': {0: 'Разводненная прибыль (убыток) на акцию', 6: '2910'},
}

# Header metadata patterns
HEADER_PATTERN_DICT = {
    'accounting_balance_form_1_2025': {
        'period': {
            'col_number': 3,
            'str_re_pattern': r'(?:За\s*)?(?:\d+\s*[-\s]*(?:й\s*)?квартал|\d+\s*месяц(?:ев)?|\d{4}\s*г)(?:\s*\d{4}\s*г?\.?)?',
            'row_count_offset': 15,
            'param_name': 'Период',
            'add_as_column': True
        },
        'document_type': {
            'col_number': 4,
            'str_re_pattern': 'БУХГАЛТЕРСКИЙ БАЛАНС',
            'row_count_offset': 16,
            'param_name': 'Тип отчета',
            'add_as_column': True
        },
        'organization_field': {
            'col_number': 0,
            'str_re_pattern': 'Организация',
            'row_count_offset': 11,
            'param_name': 'Поле организация',
            'add_as_column': False
        },
        'organization_name_value': {
            'col_number': 2,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 11,
            'param_name': 'Организация',
            'add_as_column': True
        },
        'activity_type_field': {
            'col_number': 0,
            'str_re_pattern': 'Вид экономической деятельности',
            'row_count_offset': 9,
            'param_name': 'Поле вид экономической деятельности',
            'add_as_column': False
        },
        'activity_type_value': {
            'col_number': 3,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 9,
            'param_name': 'Вид экономической деятельности',
            'add_as_column': True
        },
        'unit_of_measurement_field': {
            'col_number': 0,
            'str_re_pattern': r'Еденица измерения: тыс\.руб\/млн\.руб\.\(ненужное зачеркнуть\)',
            'row_count_offset': 6,
            'param_name': 'Поле еденица измерения',
            'add_as_column': False
        },
        'unit_of_measurement_value': {
            'col_number': 7,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 6,
            'param_name': 'Еденица измерения',
            'add_as_column': True
        },
    },
    'accounting_balance_form_1_2024': {
        'period': {
            'col_number': 1,
            'str_re_pattern': r'(^За \d{1,2}) месяцев (\d{4}) г$',
            'row_count_offset': 15,
            'param_name': 'Период',
            'add_as_column': True
        },
        'document_type': {
            'col_number': 1,
            'str_re_pattern': 'БУХГАЛТЕРСКИЙ БАЛАНС',
            'row_count_offset': 16,
            'param_name': 'Тип отчета',
            'add_as_column': True
        },
        'organization_name_value': {
            'col_number': 0,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 12,
            'param_name': 'Организация',
            'add_as_column': True
        },
        'unit_of_measurement_field': {
            'col_number': 0,
            'str_re_pattern': r'Еденица измерения: тыс\.руб\/млн\.руб\.\(ненужное зачеркнуть\)',
            'row_count_offset': 6,
            'param_name': 'Поле еденица измерения',
            'add_as_column': False
        },
        'unit_of_measurement_value': {
            'col_number': 1,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 6,
            'param_name': 'Еденица измерения',
            'add_as_column': True
        },
    },
    'financial_results_report_form_2_2025': {
        'period': {
            'col_number': 5,
            'str_re_pattern': r'(?:За\s*)?(?:\d+\s*[-\s]*(?:й\s*)?квартал|\d+\s*месяц(?:ев)?|\d{4}\s*г)(?:\s*\d{4}\s*г?\.?)?',
            'row_count_offset': 13,
            'param_name': 'Период',
            'add_as_column': True
        },
        'document_type': {
            'col_number': 4,
            'str_re_pattern': 'ОТЧЕТ О ФИНАНСОВЫХ РЕЗУЛЬТАТАХ',
            'row_count_offset': 14,
            'param_name': 'Тип отчета',
            'add_as_column': True
        },
        'organization_field': {
            'col_number': 0,
            'str_re_pattern': 'Организация',
            'row_count_offset': 9,
            'param_name': 'Поле организация',
            'add_as_column': False
        },
        'organization_name_value': {
            'col_number': 2,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 9,
            'param_name': 'Организация',
            'add_as_column': True
        },
        'unit_of_measurement_field': {
            'col_number': 0,
            'str_re_pattern': r'Единица измерения:  руб\., коп\.',
            'row_count_offset': 4,
            'param_name': 'Поле еденица измерения',
            'add_as_column': False
        },
        'unit_of_measurement_value': {
            'col_number': 6,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 4,
            'param_name': 'Еденица измерения',
            'add_as_column': True
        },
    },
    'financial_results_report_form_2_2024': {
        'period': {
            'col_number': 1,
            'str_re_pattern': r'(?:За\s*)?(?:\d+\s*[-\s]*(?:й\s*)?квартал|\d+\s*месяц(?:ев)?|\d{4}\s*г)(?:\s*\d{4}\s*г?\.?)?',
            'row_count_offset': 13,
            'param_name': 'Период',
            'add_as_column': True
        },
        'document_type': {
            'col_number': 1,
            'str_re_pattern': 'ОТЧЕТ О ФИНАНСОВЫХ РЕЗУЛЬТАТАХ',
            'row_count_offset': 14,
            'param_name': 'Тип отчета',
            'add_as_column': True
        },
        'organization_field': {
            'col_number': 0,
            'str_re_pattern': 'Организация',
            'row_count_offset': 9,
            'param_name': 'Поле организация',
            'add_as_column': False
        },
        'organization_name_value': {
            'col_number': 1,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 9,
            'param_name': 'Организация',
            'add_as_column': True
        },
        'activity_type_field': {
            'col_number': 0,
            'str_re_pattern': 'Вид экономической деятельности',
            'row_count_offset': 7,
            'param_name': 'Поле вид экономической деятельности',
            'add_as_column': False
        },
        'activity_type_value': {
            'col_number': 1,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 7,
            'param_name': 'Вид экономической деятельности',
            'add_as_column': True
        },
        'unit_of_measurement_field': {
            'col_number': 0,
            'str_re_pattern': r'Единица измерения:  руб\., коп\.',
            'row_count_offset': 4,
            'param_name': 'Поле еденица измерения',
            'add_as_column': False
        },
        'unit_of_measurement_value': {
            'col_number': 1,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()\-–—]+$',
            'row_count_offset': 4,
            'param_name': 'Еденица измерения',
            'add_as_column': True
        },
    },
}


def get_row_index_list_by_table_pattern(df: pd.DataFrame, conditions_dict: dict) -> list:
    """Find row indices matching all conditions"""
    if not conditions_dict:
        return []

    mask = pd.Series([True] * len(df))

    for column_name, expected_value in conditions_dict.items():
        if column_name not in df.columns:
            return []
        mask = mask & (df[column_name] == expected_value)

    return df[mask].index.tolist()


def find_rows_by_offset_pattern(df: pd.DataFrame, indices_list: list, conditions_dict: dict) -> list:
    """Find rows matching offset pattern from base indices"""
    matching_index_list = []

    for base_index in indices_list:
        target_index = base_index - conditions_dict['row_count_offset']

        if target_index not in df.index:
            continue

        col_number = conditions_dict['col_number']
        col_name = col_number

        if col_name not in df.columns:
            continue

        cell_value = df.loc[target_index][col_name]
        str_value = str(cell_value)
        pattern = conditions_dict['str_re_pattern']

        if re.match(pattern, str_value):
            matching_index_list.append(target_index)

    return matching_index_list


def parse_filename(raw_file_name: str) -> Tuple[str, str, str]:
    """
    Parse filename: Форма 1 6 месяцев 2025 год_--_hr_user_--_265c144f-c2e7-418d-b1d1-80e76fcfb5b7.xls
    Returns: (file_name, user_name, file_id)
    """
    if '_--_' in raw_file_name and raw_file_name.count('_--_') >= 2:
        parts = raw_file_name.split('_--_')
        file_name = parts[0].strip()
        user_name = parts[1].strip() if len(parts) > 1 else 'unknown'
        file_id_with_ext = parts[2].strip() if len(parts) > 2 else 'unknown'
        # Remove extension from file_id
        file_id = file_id_with_ext.rsplit('.', 1)[0] if '.' in file_id_with_ext else file_id_with_ext
        return file_name, user_name, file_id
    else:
        # No separator found
        file_name = raw_file_name.rsplit('.', 1)[0] if '.' in raw_file_name else raw_file_name
        return file_name, 'unknown', 'unknown'


class FinancialFormsOperator(StarRocksConnectionMixin, VKCloudFileStatusMixin, BaseHooksOperator):
    """
    Operator for processing Financial Forms (Form 1, Form 2) into static tables.
    Loads all Form 1 data to stg_buinu.form_1_assets
    Loads all Form 2 data to stg_buinu.form_2_financial
    """

    def __init__(
        self,
        s3_bucket: str,
        s3_key: str,
        file_id: str,
        starrocks_database: str = "stg_buinu",
        column_separator: str = "|",
        load_timeout: int = 600,
        enable_logging: bool = True,
        postgres_conn_id: str = "airflow_db",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_id = file_id
        self.starrocks_database = starrocks_database
        self.column_separator = column_separator
        self.load_timeout = load_timeout
        self.enable_logging = enable_logging
        self.postgres_conn_id = postgres_conn_id

        conn_id = kwargs.get("starrocks_conn_id", "starrocks_default")
        self._initialize_starrocks_connection(
            conn_id=conn_id,
            max_retries=kwargs.get("max_retries", 3),
        )
        self.stream_loader = VKStreamLoader(
            session=self.starrocks_session,
            host=self.starrocks_host,
            port=self.starrocks_port,
            user=self.starrocks_credentials[0],
            password=self.starrocks_credentials[1],
            timeout=self.load_timeout,
            logger=self.log,
        )

    def get_postgres_hook(self, database: str = "service_db"):
        """Get PostgreSQL hook for logging"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        return PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            database=database
        )

    def _get_form_type_year(self, df: pd.DataFrame) -> Tuple[str, str, Optional[str]]:
        """Detect form type, year, and raw period from document content"""
        try:
            cell_1_1 = df.loc[1, 1] if 1 in df.index and 1 in df.columns else None
            cell_0_4 = df.loc[0, 4] if 0 in df.index and 4 in df.columns else None
            cell_2_1 = df.loc[2, 1] if 2 in df.index and 1 in df.columns else None
            cell_1_3 = df.loc[1, 3] if 1 in df.index and 3 in df.columns else None
            cell_1_5 = df.loc[1, 5] if 1 in df.index and 5 in df.columns else None

            if cell_0_4 == 'БУХГАЛТЕРСКИЙ БАЛАНС' or cell_1_1 == 'БУХГАЛТЕРСКИЙ БАЛАНС':
                form_type = 'form_1'
                raw_period = next((x for x in (cell_2_1, cell_1_3) if not isinstance(x, (float, np.float64)) and pd.notna(x)), None)
                year_match = re.search(r'\d{4}', str(raw_period)) if raw_period else None
                year_period = year_match.group() if year_match else 'undefined_period'
            elif cell_0_4 == 'ОТЧЕТ О ФИНАНСОВЫХ РЕЗУЛЬТАТАХ' or cell_1_1 == 'ОТЧЕТ О ПРИБЫЛЯХ И УБЫТКАХ':
                form_type = 'form_2'
                raw_period = next((x for x in (cell_2_1, cell_1_5) if not isinstance(x, (float, np.float64)) and pd.notna(x)), None)
                year_match = re.search(r'\d{4}', str(raw_period)) if raw_period else None
                year_period = year_match.group() if year_match else 'undefined_period'
            else:
                form_type = 'undefined_form'
                year_period = 'undefined_period'
                raw_period = None

            return form_type, year_period, raw_period
        except Exception as e:
            self.log.warning(f"Error detecting form type/year: {e}")
            return 'undefined_form', 'undefined_period', None

    def _get_pattern_name_by_df(self, df: pd.DataFrame) -> str:
        """Match pattern based on document content"""
        form_type, year_period, raw_period = self._get_form_type_year(df)
        matched_patterns = []
        for pattern_name, pattern_info in PATTERN_IDENTIFIER_DICT.items():
            if (form_type == pattern_info['form_type'] and
                year_period == pattern_info['document_year']):
                matched_patterns.append(pattern_name)
        if matched_patterns:
            return matched_patterns[0]
        return 'undefined_pattern'

    def _get_data_from_header(
        self, df: pd.DataFrame, file_pattern_name: str, table_header_index_list: list
    ) -> dict:
        """Extract metadata from header sections"""
        additional_param_dict = {}
        param_dict = HEADER_PATTERN_DICT.get(file_pattern_name, {})
        for param in param_dict.keys():
            if param_dict[param]['add_as_column']:
                matched_rows = find_rows_by_offset_pattern(
                    df, table_header_index_list, param_dict[param]
                )
                col_num = param_dict[param]['col_number']
                values = df.loc[matched_rows, col_num].tolist() if matched_rows else []
                additional_param_dict[param_dict[param]['param_name']] = values
        return additional_param_dict

    def _get_merged_df(
        self, df: pd.DataFrame, file_pattern_name: str,
        table_header_index_list: list, table_last_row_index_list: list,
        additional_param_dict: dict
    ) -> pd.DataFrame:
        """Merge all tables into one DataFrame"""
        column_pattern = COLUMN_HEADER_NAME_PATTERN_DICT[file_pattern_name]
        merged_df = pd.DataFrame(columns=list(column_pattern.values()))

        for i in range(len(table_header_index_list)):
            current_df = df[table_header_index_list[i]+2:table_last_row_index_list[i]+1][
                list(column_pattern.keys())
            ].rename(columns=column_pattern)

            for k in additional_param_dict.keys():
                current_df[k] = additional_param_dict[k][i] if i < len(additional_param_dict[k]) else None

            merged_df = pd.concat([merged_df, current_df], ignore_index=True)

        return merged_df

    def _update_status(self, status_code: str, error_code: str = None):
        """Update file status in PostgreSQL if logging is enabled"""
        if self.enable_logging:
            try:
                self.update_vk_cloud_file_status(
                    file_id=self.file_id,
                    status_code=status_code,
                    error_code=error_code
                )
            except Exception as e:
                self.log.warning(f"Could not update file status: {e}")

    def execute(self, context) -> Dict[str, object]:
        try:
            self.log.info("Processing Financial Form: %s", self.s3_key)

            self._update_status('VALIDATING')

            # Get file from S3
            file_content = self._get_file_from_s3()

            # Read Excel
            df = pd.read_excel(BytesIO(file_content), header=None)

            # Detect pattern
            file_pattern_name = self._get_pattern_name_by_df(df)
            if file_pattern_name == 'undefined_pattern':
                raise ValueError("Unable to detect file pattern from document content")

            self.log.info(f"Detected pattern: {file_pattern_name}")

            # Get form type
            form_type = PATTERN_IDENTIFIER_DICT[file_pattern_name]['form_type']

            # Find table boundaries
            table_header_index_list = get_row_index_list_by_table_pattern(
                df, COLUMN_HEADER_NAME_PATTERN_DICT[file_pattern_name]
            )
            table_last_row_index_list = get_row_index_list_by_table_pattern(
                df, COLUMN_FINAL_ROW_PATTERN_DICT[file_pattern_name]
            )

            if not table_header_index_list or len(table_header_index_list) != len(table_last_row_index_list):
                raise ValueError(f"Table format does not match pattern {file_pattern_name}")

            self.log.info(f"Found {len(table_header_index_list)} tables in document")

            # Extract header metadata
            additional_param_dict = self._get_data_from_header(
                df, file_pattern_name, table_header_index_list
            )

            # Merge tables
            union_df = self._get_merged_df(
                df, file_pattern_name, table_header_index_list,
                table_last_row_index_list, additional_param_dict
            )

            # Parse filename
            raw_file_name = self.s3_key.split('/')[-1]  # Get filename from s3_key
            file_name, user_name, file_id = parse_filename(raw_file_name)

            # Rename columns based on form type
            if form_type == 'form_1':
                union_df = union_df.rename(columns={
                    '1': 'АКТИВ',
                    '2': 'Код показателя',
                    '3': 'На текущий год',
                    '4': 'На 31 декабря прошлого года',
                    '5': 'На 31 декабря позапрошлого года',
                    'Период': 'Отчетный период',
                    'Тип отчета': 'Название отчета',
                    'Еденица измерения': 'Единица измерения',
                    'Вид экономической деятельности': 'Вид экономической деятельности'
                })
                union_df['АКТИВ'] = union_df['АКТИВ'].str.strip()
                union_df['На текущий год'] = union_df['На текущий год'].astype(str).str.replace(' ', '', regex=False).str.replace(',', '.', regex=False).replace('nan', np.nan).astype(float)
                union_df['На 31 декабря прошлого года'] = union_df['На 31 декабря прошлого года'].astype(str).str.replace(' ', '', regex=False).str.replace(',', '.', regex=False).replace('nan', np.nan).astype(float)
                union_df['На 31 декабря позапрошлого года'] = union_df['На 31 декабря позапрошлого года'].astype(str).str.replace(' ', '', regex=False).str.replace(',', '.', regex=False).replace('nan', np.nan).astype(float)

                starrocks_table = 'form_1_assets'

            elif form_type == 'form_2':
                union_df = union_df.rename(columns={
                    '1': 'Показатель',
                    '2': 'Код',
                    '3': 'За текущий период',
                    '4': 'За текущий период прошлого года',
                    'Период': 'Отчетный период',
                    'Тип отчета': 'Название отчета',
                    'Еденица измерения': 'Единица измерения'
                })
                union_df['Показатель'] = union_df['Показатель'].str.strip()
                union_df['За текущий период'] = union_df['За текущий период'].astype(str).str.replace(' ', '', regex=False).str.replace(',', '.', regex=False).replace('nan', np.nan).astype(float)
                union_df['За текущий период прошлого года'] = union_df['За текущий период прошлого года'].astype(str).str.replace(' ', '', regex=False).str.replace(',', '.', regex=False).replace('nan', np.nan).astype(float)

                starrocks_table = 'form_2_financial'
            else:
                raise ValueError(f"Unknown form type: {form_type}")

            # Add metadata columns
            union_df['file_id'] = file_id
            union_df['user_name'] = user_name
            union_df['raw_file_name'] = raw_file_name
            union_df['file_name'] = file_name
            union_df['s3_placement'] = pd.Timestamp.now()

            # Reorder columns to match DDL
            if form_type == 'form_1':
                column_order = [
                    'АКТИВ',
                    'Код показателя',
                    'На текущий год',
                    'На 31 декабря прошлого года',
                    'На 31 декабря позапрошлого года',
                    'Отчетный период',
                    'Единица измерения',
                    'Название отчета',
                    'Организация',
                    'Вид экономической деятельности',
                    'file_id',
                    'user_name',
                    'raw_file_name',
                    'file_name',
                    's3_placement'
                ]
            else:  # form_2
                column_order = [
                    'Показатель',
                    'Код',
                    'За текущий период',
                    'За текущий период прошлого года',
                    'Отчетный период',
                    'Единица измерения',
                    'Название отчета',
                    'Организация',
                    'file_id',
                    'user_name',
                    'raw_file_name',
                    'file_name',
                    's3_placement'
                ]

            # Reorder DataFrame columns
            union_df = union_df[column_order]

            self.log.info(f"Final DataFrame shape: {union_df.shape}")

            self._update_status('LOADING_STAGE')

            # Load to StarRocks
            load_result = self._load_to_starrocks(union_df, starrocks_table)

            self._update_status('COMPLETED')

            rows_loaded = load_result.get('NumberLoadedRows', len(union_df))
            return {
                'success': True,
                'rows_processed': len(union_df),
                'rows_loaded': rows_loaded,
                'form_type': form_type,
                'table_name': starrocks_table,
                'starrocks_database': self.starrocks_database,
                'load_result': load_result,
            }

        except Exception as exc:
            self.log.error("Error processing financial form: %s", exc, exc_info=True)

            error_code = 'ERR_UNKNOWN'
            error_str = str(exc).lower()
            if 'pattern' in error_str or 'detect' in error_str:
                error_code = 'ERR_PATTERN_DETECTION'
            elif 'table' in error_str or 'format' in error_str:
                error_code = 'ERR_TABLE_FORMAT'
            elif 'load' in error_str or 'starrocks' in error_str:
                error_code = 'ERR_LOAD_STAGE'
            elif 's3' in error_str or 'file not found' in error_str:
                error_code = 'ERR_S3_ACCESS'
            elif 'parse' in error_str or 'excel' in error_str:
                error_code = 'ERR_PARSE'

            self._update_status('ERROR', error_code)

            return {'success': False, 'error': str(exc)}
        finally:
            self.close_starrocks_connection()

    def _load_to_starrocks(self, df: pd.DataFrame, table_name: str) -> Dict[str, object]:
        """Load DataFrame to StarRocks via stream loading"""
        columns = list(df.columns)

        converter = StarRocksDataConverter(logger=self.log)
        csv_data = converter.dataframe_to_csv(
            df,
            separator=self.column_separator,
        )

        return self.stream_loader.execute_stream_load(
            database=self.starrocks_database,
            table=table_name,
            payload=csv_data,
            columns=columns,
            column_separator=self.column_separator,
        )

    def _get_file_from_s3(self) -> bytes:
        """Fetch file from S3"""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=self.s3_conn_id)
        file_obj = hook.get_key(key=self.s3_key, bucket_name=self.s3_bucket)
        if not file_obj:
            raise FileNotFoundError(
                f"File not found: s3://{self.s3_bucket}/{self.s3_key}"
            )
        return file_obj.get()['Body'].read()
