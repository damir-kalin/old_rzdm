{% macro mysql__rename_relation(from_relation, to_relation) -%}
  {%- set from_full = from_relation.render() -%}
  {%- set to_full = to_relation.render() -%}
  -- For StarRocks: use CREATE TABLE ... AS SELECT instead of RENAME TABLE
  -- StarRocks doesn't support RENAME TABLE, so we copy data and drop temp table
  -- APPEND-ONLY: Если целевая таблица существует, данные уже добавлены через post_hook
  -- Просто удаляем временную таблицу, не перезаписывая целевую
  {%- set target_relation = adapter.get_relation(
        database=to_relation.database,
        schema=to_relation.schema,
        identifier=to_relation.identifier
  ) -%}
  {%- if target_relation is none -%}
    -- Целевая таблица не существует: создаем из временной
    CREATE TABLE {{ to_full }} AS SELECT * FROM {{ from_full }};
    DROP TABLE IF EXISTS {{ from_full }};
  {%- else -%}
    -- Целевая таблица существует: данные уже добавлены через post_hook (append-only)
    -- Просто удаляем временную таблицу, не перезаписывая целевую
    DROP TABLE IF EXISTS {{ from_full }};
  {%- endif -%}
{%- endmacro %}

