"""
Integration module for Financial Forms with the existing StarRocks pipeline.

This operator mirrors the parsing logic from ``form_parsing_by_pattern (2).ipynb``:
we detect the Form 1 pattern from the original XLS file name, identify table
boundaries via header/final-row templates, enrich each table with header metadata,
and stream the cleaned dataset into StarRocks.
"""

from __future__ import annotations

import re
from io import BytesIO
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from airflow.models import BaseOperator

from base_operators.base_hooks_operator import BaseHooksOperator
from starrocks_operators.starrocks_connection_mixin import StarRocksConnectionMixin
from starrocks_operators.starrocks_data_converter import StarRocksDataConverter
from vk_cloud_operators.vk_table_manager import VKTableManager
from vk_cloud_operators.vk_stream_loader import VKStreamLoader


# Enhanced pattern definitions from the new notebook
FILE_NAME_PATTERNS: Dict[str, str] = {
    'accounting_balance_form_1_year': r'^Форма 1 (\d{1,2}) месяцев (\d{4}) год\.xls$',
    'accounting_balance_form_1_y': r'^Форма 1 (\d{1,2}) месяцев (\d{4}) г\.\.xls$',
    'financial_results_report_form_2_year': r'^Форма 2 (\d{1,2}) месяцев (\d{4}) год\.xls$',
    'financial_results_report_form_2_annual': r'^Форма 2 за (\d{4}) год\.xls$'
}

COLUMN_HEADER_PATTERNS: Dict[str, Dict[int, str]] = {
    'accounting_balance_form_1_year': {
        0: 'АКТИВ',
        6: 'Код\nпоказателя',
        7: 'На текущий год',
        8: 'На 31 декабря прошлого года',
        9: 'На 31 декабря позапрошлого года'
    },
    'accounting_balance_form_1_y': {
        0: 'АКТИВ',
        1: 'Код\nпоказателя',
        2: 'На текущий период',
        3: 'На 31 декабря прошлого года',
        6: 'На 31 декабря позапрошлого года'
    },
    'financial_results_report_form_2_year': {
        0: 'Наименование показателя',
        6: 'Код',
        7: 'За текущий год',
        8: 'За текущий период прошлого года'
    },
    'financial_results_report_form_2_annual': {
        0: 'Наименование показателя',
        6: 'Код',
        7: 'За текущий год',
        8: 'За текущий период прошлого года'
    }
}

FINAL_ROW_PATTERNS: Dict[str, Dict[int, str]] = {
    'accounting_balance_form_1_year': {0: 'БАЛАНС', 6: '1700'},
    'accounting_balance_form_1_y': {0: 'БАЛАНС', 1: '1700'},
    'financial_results_report_form_2_year': {
        0: 'Разводненная прибыль (убыток) на акцию',
        6: '2910'
    },
    'financial_results_report_form_2_annual': {
        0: 'Разводненная прибыль (убыток) на акцию',
        6: '2910'
    }
}

HEADER_METADATA_PATTERNS: Dict[str, Dict[str, Dict[str, object]]] = {
    'accounting_balance_form_1_year': {
        'period': {
            'col_number': 3,
            'str_re_pattern': r'(^\d{1,2}) месяцев (\d{4}) г\.$',
            'row_count_offset': 14,
            'param_name': 'Период',
            'add_as_column': True
        },
        'document_type': {
            'col_number': 4,
            'str_re_pattern': 'БУХГАЛТЕРСКИЙ БАЛАНС',
            'row_count_offset': 15,
            'param_name': 'Тип отчета',
            'add_as_column': True
        },
        'organization_field': {
            'col_number': 0,
            'str_re_pattern': 'Организация',
            'row_count_offset': 8,
            'param_name': 'Поле организация',
            'add_as_column': False
        },
        'organization_name_value': {
            'col_number': 2,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 10,
            'param_name': 'Организация',
            'add_as_column': True
        },
        'activity_type_field': {
            'col_number': 0,
            'str_re_pattern': 'Вид экономической деятельности',
            'row_count_offset': 6,
            'param_name': 'Поле вид экономической деятельности',
            'add_as_column': False
        },
        'activity_type_value': {
            'col_number': 3,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 8,
            'param_name': 'Вид экономической деятельности',
            'add_as_column': True
        },
        'unit_of_measurement_field': {
            'col_number': 0,
            'str_re_pattern': r'Еденица измерения: тыс\.руб\/млн\.руб\.\(ненужное зачеркнуть\)',
            'row_count_offset': 3,
            'param_name': 'Поле еденица измерения',
            'add_as_column': False
        },
        'unit_of_measurement_value': {
            'col_number': 7,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 5,
            'param_name': 'Еденица измерения',
            'add_as_column': True
        },
    },
    'accounting_balance_form_1_y': {
        'period': {
            'col_number': 5,
            'str_re_pattern': r'(^\d{1,2}) месяцев (\d{4}) г\.$',
            'row_count_offset': 12,
            'param_name': 'Период',
            'add_as_column': True
        },
        'document_type': {
            'col_number': 4,
            'str_re_pattern': 'БУХГАЛТЕРСКИЙ БАЛАНС',
            'row_count_offset': 13,
            'param_name': 'Тип отчета',
            'add_as_column': True
        },
        'organization_field': {
            'col_number': 0,
            'str_re_pattern': 'Организация',
            'row_count_offset': 8,
            'param_name': 'Поле организация',
            'add_as_column': False
        },
        'organization_name_value': {
            'col_number': 2,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 8,
            'param_name': 'Организация',
            'add_as_column': True
        },
        'activity_type_field': {
            'col_number': 0,
            'str_re_pattern': 'Вид экономической деятельности',
            'row_count_offset': 6,
            'param_name': 'Поле вид экономической деятельности',
            'add_as_column': False
        },
        'activity_type_value': {
            'col_number': 4,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 6,
            'param_name': 'Вид экономической деятельности',
            'add_as_column': True
        },
        'unit_of_measurement_field': {
            'col_number': 0,
            'str_re_pattern': r'Единица измерения:  руб\., коп\.',
            'row_count_offset': 3,
            'param_name': 'Поле еденица измерения',
            'add_as_column': False
        },
        'unit_of_measurement_value': {
            'col_number': 6,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 3,
            'param_name': 'Еденица измерения',
            'add_as_column': True
        },
    },
    'financial_results_report_form_2_year': {
        'period': {
            'col_number': 5,
            'str_re_pattern': r'(^\d{1,2}) месяцев (\d{4}) г\.$',
            'row_count_offset': 12,
            'param_name': 'Период',
            'add_as_column': True
        },
        'document_type': {
            'col_number': 4,
            'str_re_pattern': 'ОТЧЕТ О ФИНАНСОВЫХ РЕЗУЛЬТАТАХ',
            'row_count_offset': 13,
            'param_name': 'Тип отчета',
            'add_as_column': True
        },
        'organization_field': {
            'col_number': 0,
            'str_re_pattern': 'Организация',
            'row_count_offset': 8,
            'param_name': 'Поле организация',
            'add_as_column': False
        },
        'organization_name_value': {
            'col_number': 2,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 8,
            'param_name': 'Организация',
            'add_as_column': True
        },
        'activity_type_field': {
            'col_number': 0,
            'str_re_pattern': 'Вид экономической деятельности',
            'row_count_offset': 6,
            'param_name': 'Поле вид экономической деятельности',
            'add_as_column': False
        },
        'activity_type_value': {
            'col_number': 4,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 6,
            'param_name': 'Вид экономической деятельности',
            'add_as_column': True
        },
        'unit_of_measurement_field': {
            'col_number': 0,
            'str_re_pattern': r'Единица измерения:  руб\., коп\.',
            'row_count_offset': 3,
            'param_name': 'Поле еденица измерения',
            'add_as_column': False
        },
        'unit_of_measurement_value': {
            'col_number': 6,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 3,
            'param_name': 'Еденица измерения',
            'add_as_column': True
        },
    },
    'financial_results_report_form_2_annual': {
        'period': {
            'col_number': 5,
            'str_re_pattern': r'(^12) месяцев (\d{4}) г\.$',
            'row_count_offset': 12,
            'param_name': 'Период',
            'add_as_column': True
        },
        'document_type': {
            'col_number': 4,
            'str_re_pattern': 'ОТЧЕТ О ФИНАНСОВЫХ РЕЗУЛЬТАТАХ',
            'row_count_offset': 13,
            'param_name': 'Тип отчета',
            'add_as_column': True
        },
        'organization_field': {
            'col_number': 0,
            'str_re_pattern': 'Организация',
            'row_count_offset': 8,
            'param_name': 'Поле организация',
            'add_as_column': False
        },
        'organization_name_value': {
            'col_number': 2,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 8,
            'param_name': 'Организация',
            'add_as_column': True
        },
        'activity_type_field': {
            'col_number': 0,
            'str_re_pattern': 'Вид экономической деятельности',
            'row_count_offset': 6,
            'param_name': 'Поле вид экономической деятельности',
            'add_as_column': False
        },
        'activity_type_value': {
            'col_number': 4,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 6,
            'param_name': 'Вид экономической деятельности',
            'add_as_column': True
        },
        'unit_of_measurement_field': {
            'col_number': 0,
            'str_re_pattern': r'Единица измерения:  руб\., коп\.',
            'row_count_offset': 3,
            'param_name': 'Поле еденица измерения',
            'add_as_column': False
        },
        'unit_of_measurement_value': {
            'col_number': 6,
            'str_re_pattern': r'^[a-zA-Zа-яА-ЯёЁ0-9\s\"«»„"".,()-–—]+$',
            'row_count_offset': 3,
            'param_name': 'Еденица измерения',
            'add_as_column': True
        },
    },
}

# Advanced data transformation mappings for quarterly/yearly normalization
COLUMN_VALUE_ATTRIBUTES: Dict[str, Dict[str, Dict[str, object]]] = {
    'accounting_balance_form_1_year': {
        'На текущий год': {
            'value_type': 'Нарастающий итог по кварталам с начала года',
            'year_offset': 0,
            'data_type': 'Оперативный',
            'discreteness': 'Квартал',
            'metric_type': 'Факт'
        },
        'На 31 декабря прошлого года': {
            'value_type': 'Итог',
            'year_offset': 1,
            'data_type': 'Утвержденный',
            'discreteness': 'Год',
            'metric_type': 'Факт'
        },
        'На 31 декабря позапрошлого года': {
            'value_type': 'Итог',
            'year_offset': 2,
            'data_type': 'Утвержденный',
            'discreteness': 'Год',
            'metric_type': 'Факт'
        }
    },
    'financial_results_report_form_2_year': {
        'За текущий год': {
            'value_type': 'Нарастающий итог по кварталам с начала года',
            'year_offset': 0,
            'data_type': 'Оперативный',
            'discreteness': 'Квартал',
            'metric_type': 'Факт'
        },
        'За текущий период прошлого года': {
            'value_type': 'Нарастающий итог по кварталам с начала года',
            'year_offset': 1,
            'data_type': 'Оперативный',
            'discreteness': 'Квартал',
            'metric_type': 'Факт'
        },
    },
    'financial_results_report_form_2_annual': {
        'За текущий год': {
            'value_type': 'Нарастающий итог по кварталам с начала года',
            'year_offset': 0,
            'data_type': 'Утвержденный',
            'discreteness': 'Год',
            'metric_type': 'Факт'
        },
        'За текущий период прошлого года': {
            'value_type': 'Нарастающий итог по кварталам с начала года',
            'year_offset': 1,
            'data_type': 'Утвержденный',
            'discreteness': 'Год',
            'metric_type': 'Факт'
        },
    },
}

# Column mapping overrides for different form patterns
COLUMN_SOURCE_OVERRIDES: Dict[str, List[int]] = {}
COLUMN_RENAME_OVERRIDES: Dict[str, List[str]] = {}


def get_row_index_list_by_table_pattern(df, conditions_dict):
    """Find row indices matching table pattern conditions."""
    if not conditions_dict:
        return []

    mask = pd.Series([True] * len(df))

    for column_name, expected_value in conditions_dict.items():
        if column_name not in df.columns:
            return []
        mask = mask & (df[column_name] == expected_value)

    return df[mask].index.tolist()


def find_rows_by_offset_pattern(df, indices_list, conditions_dict):
    """Find rows matching offset pattern from base indices."""
    matching_index_list = []

    for base_index in indices_list:
        target_index = base_index - conditions_dict['row_count_offset']

        if target_index not in df.index:
            continue

        col_number = conditions_dict['col_number']

        if col_number not in df.columns:
            continue

        cell_value = df.loc[target_index][col_number]
        str_value = str(cell_value)
        pattern = conditions_dict['str_re_pattern']

        if re.match(pattern, str_value):
            matching_index_list.append(target_index)

    return matching_index_list


def get_pattern_name_by_file_name(file_name, pattern_dict):
    """Determine pattern name from filename."""
    matched_pattern_list = []
    for k in pattern_dict.keys():
        if re.match(pattern_dict[k], file_name):
            matched_pattern_list.append(k)

    if len(matched_pattern_list) != 0:
        return matched_pattern_list[0]
    else:
        return 'unknown_pattern'


def get_data_from_header(df, header_pattern_dict, file_pattern_name, table_header_index_list):
    """Extract additional parameters from headers."""
    additional_param_dict = dict()
    param_dict = header_pattern_dict[file_pattern_name]

    for param in param_dict.keys():
        if param_dict[param]['add_as_column']:
            param_name = param_dict[param]['param_name']
            col_number = param_dict[param]['col_number']

            matching_indices = find_rows_by_offset_pattern(
                df, table_header_index_list, param_dict[param]
            )

            values = []
            for idx in matching_indices:
                if idx is not None and col_number in df.columns:
                    values.append(df.loc[idx][col_number])
                else:
                    values.append(None)

            additional_param_dict[param_name] = values

    return additional_param_dict


def get_merged_df(df, column_header_name_pattern_dict, file_pattern_name,
                  table_header_index_list, table_last_row_index_list, additional_param_dict):
    """Merge all tables into one dataframe."""
    merged_df = pd.DataFrame(columns=column_header_name_pattern_dict[file_pattern_name].values())

    for i in range(len(table_header_index_list)):
        current_df = df[table_header_index_list[i]+2:table_last_row_index_list[i]+1][
            column_header_name_pattern_dict[file_pattern_name].keys()
        ].rename(columns=column_header_name_pattern_dict[file_pattern_name])

        for k in additional_param_dict.keys():
            current_df[k] = additional_param_dict[k][i] if i < len(additional_param_dict[k]) else None

        merged_df = pd.concat([merged_df, current_df])

    return merged_df


def transform_financial_data(union_df: pd.DataFrame, file_pattern_name: str) -> pd.DataFrame:
    """Transform financial data with quarterly and yearly normalization."""
    # Extract quarter and year from period for applicable patterns
    if file_pattern_name in ('accounting_balance_form_1_year', 'financial_results_report_form_2_year', 'financial_results_report_form_2_annual'):
        # Extract months and convert to quarter, handling None values
        months_extracted = union_df['Период'].str.extract(r'(\d+)', expand=False)
        union_df['Квартал'] = pd.to_numeric(months_extracted, errors='coerce').fillna(0).astype(int) // 3
        union_df['Год'] = union_df['Период'].str.extract(r'(\d{4})', expand=False)

    if file_pattern_name not in COLUMN_VALUE_ATTRIBUTES:
        return union_df

    # Transform data structure for each value column
    value_columns = COLUMN_VALUE_ATTRIBUTES[file_pattern_name]

    if file_pattern_name == 'accounting_balance_form_1_year':
        final_df = pd.DataFrame(columns=('АКТИВ','Код\nпоказателя', 'Значение'))
        base_columns = ['АКТИВ', 'Код\nпоказателя']
    elif file_pattern_name in ('financial_results_report_form_2_year', 'financial_results_report_form_2_annual'):
        final_df = pd.DataFrame(columns=('Наименование показателя', 'Код', 'Значение'))
        base_columns = ['Наименование показателя', 'Код']
    else:
        return union_df

    metadata_columns = [
        'Период', 'Тип отчета', 'Организация',
        'Вид экономической деятельности', 'Еденица измерения',
        'Квартал', 'Год'
    ]

    for col_name, attributes in value_columns.items():
        cur_df = union_df[base_columns + [col_name] + metadata_columns].copy()
        cur_df = cur_df.rename(columns={col_name: 'Значение'})

        # Apply year offset
        year_offset = attributes['year_offset']
        cur_df['Год'] = (pd.to_numeric(cur_df['Год'], errors='coerce').fillna(0).astype(int) - year_offset).astype(str)
        cur_df['Тип значения'] = attributes['value_type']

        # Apply dynamic metadata from attributes dictionary
        cur_df[['Тип данных', 'Метрика', 'Дискретность']] = [
            attributes['data_type'],
            attributes['metric_type'],
            attributes['discreteness']
        ]

        # Set quarter to 4 for non-current year data
        if col_name != list(value_columns.keys())[0]:  # Not the first column (current period)
            cur_df['Квартал'] = 4

        final_df = pd.concat([final_df, cur_df], ignore_index=True)

    return final_df


def _get_row_indices_by_pattern(df: pd.DataFrame, pattern: Dict[int, str]) -> List[int]:
    indices: List[int] = []
    for idx in df.index:
        row_matches = True
        for col_idx, expected_value in pattern.items():
            if col_idx not in df.columns:
                row_matches = False
                break
            if df.at[idx, col_idx] != expected_value:
                row_matches = False
                break
        if row_matches:
            indices.append(idx)
    return indices


def _find_rows_by_offset_pattern(
    df: pd.DataFrame,
    header_indices: List[int],
    config: Dict[str, object],
) -> List[Optional[int]]:
    matches: List[Optional[int]] = []
    for base_idx in header_indices:
        target_idx = base_idx - int(config['row_count_offset'])
        if target_idx not in df.index or config['col_number'] not in df.columns:
            matches.append(None)
            continue
        cell_value = df.at[target_idx, config['col_number']]
        str_value = '' if cell_value is None or (isinstance(cell_value, float) and np.isnan(cell_value)) else str(cell_value)
        if re.match(config['str_re_pattern'], str_value):
            matches.append(target_idx)
        else:
            matches.append(None)
    return matches


class FinancialFormsOperator(StarRocksConnectionMixin, BaseHooksOperator):
    """
    Specialized operator for processing financial forms (Form 1 and Form 2).
    """

    def __init__(
        self,
        s3_bucket: str,
        s3_key: str,
        starrocks_table: str,
        file_id: str,
        starrocks_database: str = "stage",
        column_separator: str = "|",
        load_timeout: int = 600,
        header_row: int = 7,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.starrocks_table = starrocks_table
        self.file_id = file_id
        self.starrocks_database = starrocks_database
        self.column_separator = column_separator
        self.load_timeout = load_timeout
        self.header_row = header_row  # kept for backward compatibility

        conn_id = kwargs.get("starrocks_conn_id", "starrocks_default")
        self._initialize_starrocks_connection(
            conn_id=conn_id,
            max_retries=kwargs.get("max_retries", 3),
        )
        self.table_manager = VKTableManager(
            mysql_conn_id=conn_id,
            database=self.starrocks_database,
            logger=self.log,
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

    def _determine_form_pattern(self, file_name: str) -> str:
        for pattern_name, regex in FILE_NAME_PATTERNS.items():
            if re.match(regex, file_name):
                return pattern_name
        raise ValueError(
            f"File {file_name} does not match supported form patterns"
        )

    def _extract_additional_parameters(
        self,
        df: pd.DataFrame,
        header_indices: List[int],
        pattern_name: str,
    ) -> Dict[str, List[Optional[str]]]:
        params_config = HEADER_METADATA_PATTERNS.get(pattern_name, {})
        additional_params: Dict[str, List[Optional[str]]] = {}

        for config in params_config.values():
            if not config.get('add_as_column'):
                continue

            rows = _find_rows_by_offset_pattern(df, header_indices, config)
            values: List[Optional[str]] = []
            for row_idx in rows:
                if row_idx is None or config['col_number'] not in df.columns:
                    values.append(None)
                else:
                    values.append(df.at[row_idx, config['col_number']])
            additional_params[config['param_name']] = values

        return additional_params

    def _build_form_dataframe(
        self,
        df: pd.DataFrame,
        pattern_name: str,
    ) -> pd.DataFrame:
        column_pattern = COLUMN_HEADER_PATTERNS.get(pattern_name)
        final_row_pattern = FINAL_ROW_PATTERNS.get(pattern_name)
        if not column_pattern or not final_row_pattern:
            raise ValueError(f"No configuration found for pattern {pattern_name}")

        header_indices = _get_row_indices_by_pattern(df, column_pattern)
        final_indices = _get_row_indices_by_pattern(df, final_row_pattern)
        if not header_indices or len(header_indices) != len(final_indices):
            raise ValueError(
                f"Unable to detect table boundaries for pattern {pattern_name}"
            )

        metadata = self._extract_additional_parameters(
            df,
            header_indices,
            pattern_name,
        )

        union_df = pd.DataFrame(columns=column_pattern.values())
        src_columns = COLUMN_SOURCE_OVERRIDES.get(
            pattern_name,
            list(column_pattern.keys()),
        )
        dst_columns = list(column_pattern.values())
        rename_columns = COLUMN_RENAME_OVERRIDES.get(pattern_name, dst_columns)

        for i, header_idx in enumerate(header_indices):
            end_idx = final_indices[i]
            current_df = df.iloc[header_idx + 2:end_idx + 1, src_columns].copy()
            current_df.columns = rename_columns

            for meta_name, meta_values in metadata.items():
                value = meta_values[i] if i < len(meta_values) else None
                current_df[meta_name] = value

            union_df = pd.concat([union_df, current_df], ignore_index=True)

        return union_df

    def clean_financial_data(self, df: pd.DataFrame) -> pd.DataFrame:
        clean_columns: List[str] = []
        seen: Dict[str, int] = {}
        for idx, col in enumerate(df.columns):
            col_str = "" if col is None else str(col)
            if not col_str or col_str.startswith('Unnamed'):
                base_name = f'column_{idx + 1}'
                seen[base_name] = seen.get(base_name, 0) + 1
                clean_columns.append(
                    base_name if seen[base_name] == 1 else f"{base_name}_{seen[base_name]}"
                )
                continue

            normalized = re.sub(r'[\r\n]+', ' ', col_str)
            normalized = re.sub(r'\s+', ' ', normalized).strip()
            if not normalized:
                normalized = f'column_{idx + 1}'
            normalized = normalized[:128]
            seen[normalized] = seen.get(normalized, 0) + 1
            if seen[normalized] > 1:
                normalized = f"{normalized}_{seen[normalized]}"
            clean_columns.append(normalized)

        df.columns = clean_columns
        df = df.dropna(how='all')
        df = df.loc[:, df.notna().any()]

        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    series = df[col].astype(str).str.replace(' ', '').str.replace(',', '.')
                    numeric_series = pd.to_numeric(series, errors='coerce')
                    if numeric_series.notna().sum() > len(df) * 0.5:
                        df[col] = numeric_series
                except Exception:
                    continue

        df['form_type'] = 'financial'
        df['file_id'] = self.file_id
        df['source_file'] = self.s3_key
        df['processed_at'] = pd.Timestamp.now()

        return df

    def validate_financial_data(self, df: pd.DataFrame) -> Dict[str, object]:
        validation = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'data_completeness': {},
            'data_types': {},
            'quality_issues': [],
        }

        for col in df.columns:
            null_pct = round(df[col].isnull().mean() * 100, 2)
            validation['data_completeness'][col] = null_pct
            validation['data_types'][col] = str(df[col].dtype)
            if null_pct > 95:
                validation['quality_issues'].append(
                    f"Column '{col}' is mostly empty ({null_pct}%)"
                )

        duplicate_count = df.duplicated().sum()
        if duplicate_count:
            validation['quality_issues'].append(
                f"Found {duplicate_count} duplicate rows"
            )

        return validation

    def execute(self, context) -> Dict[str, object]:
        try:
            self.log.info("Processing financial form with enhanced logic: %s", self.s3_key)
            file_content = self._get_file_from_s3()

            # Read Excel file without headers using enhanced logic
            df = pd.read_excel(BytesIO(file_content), header=None, engine='xlrd')

            file_name = self.s3_key.split('/')[-1]
            file_pattern_name = get_pattern_name_by_file_name(file_name, FILE_NAME_PATTERNS)

            if file_pattern_name == 'unknown_pattern':
                raise ValueError(f"File {file_name} does not match supported patterns")

            self.log.info("Detected pattern: %s", file_pattern_name)

            # Validate table format using enhanced logic
            table_header_index_list = get_row_index_list_by_table_pattern(
                df, COLUMN_HEADER_PATTERNS[file_pattern_name]
            )
            table_last_row_index_list = get_row_index_list_by_table_pattern(
                df, FINAL_ROW_PATTERNS[file_pattern_name]
            )

            if not table_header_index_list or len(table_header_index_list) != len(table_last_row_index_list):
                raise ValueError(f"Table format validation failed for pattern {file_pattern_name}")

            self.log.info("Found %d tables in file", len(table_header_index_list))

            # Extract header metadata using enhanced logic
            if file_pattern_name in HEADER_METADATA_PATTERNS:
                additional_param_dict = get_data_from_header(
                    df, HEADER_METADATA_PATTERNS, file_pattern_name, table_header_index_list
                )
            else:
                additional_param_dict = {}

            # Merge all tables using enhanced logic
            parsed_df = get_merged_df(
                df, COLUMN_HEADER_PATTERNS, file_pattern_name,
                table_header_index_list, table_last_row_index_list, additional_param_dict
            )

            # Apply advanced transformation (quarterly/yearly normalization)
            transformed_df = transform_financial_data(parsed_df, file_pattern_name)

            # Add metadata
            transformed_df['file_pattern'] = file_pattern_name
            transformed_df['form_type'] = 'financial_enhanced'
            transformed_df['file_id'] = self.file_id
            transformed_df['source_file'] = self.s3_key
            transformed_df['processed_at'] = pd.Timestamp.now()

            # Clean and load data
            cleaned_df = self.clean_financial_data(transformed_df)
            load_result = self._load_to_starrocks(cleaned_df)
            validation = self.validate_financial_data(cleaned_df)

            rows_loaded = load_result.get('NumberLoadedRows', len(cleaned_df))
            return {
                'success': True,
                'rows_processed': len(cleaned_df),
                'rows_loaded': rows_loaded,
                'columns': list(cleaned_df.columns),
                'form_metadata': {
                    'pattern_name': file_pattern_name,
                    'tables_found': len(table_header_index_list),
                    'header_metadata': additional_param_dict
                },
                'validation_results': validation,
                'table_name': self.starrocks_table,
                'starrocks_database': self.starrocks_database,
                'load_result': load_result,
            }
        except Exception as exc:
            self.log.error("Error processing financial form: %s", exc)
            return {'success': False, 'error': str(exc)}
        finally:
            self.close_starrocks_connection()

    def _load_to_starrocks(self, df: pd.DataFrame) -> Dict[str, object]:
        columns = list(df.columns)
        self.table_manager.create_table_if_not_exists(
            table_name=self.starrocks_table,
            columns=columns,
        )
        converter = StarRocksDataConverter(logger=self.log)
        csv_data = converter.dataframe_to_csv(
            df,
            separator=self.column_separator,
        )
        return self.stream_loader.execute_stream_load(
            database=self.starrocks_database,
            table=self.starrocks_table,
            payload=csv_data,
            columns=columns,
            column_separator=self.column_separator,
        )

    def _get_file_from_s3(self) -> bytes:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=self.s3_conn_id)
        file_obj = hook.get_key(key=self.s3_key, bucket_name=self.s3_bucket)
        if not file_obj:
            raise FileNotFoundError(
                f"File not found: s3://{self.s3_bucket}/{self.s3_key}"
            )
        return file_obj.get()['Body'].read()
