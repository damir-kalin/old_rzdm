{%- macro mysql__get_create_table_as_sql(temporary, relation, sql) -%}
  {%- set final_relation = relation.replace_path(identifier=relation.identifier.replace('__dbt_tmp', '')) -%}
  {%- set relation_check = adapter.get_relation(
        database=final_relation.database,
        schema=final_relation.schema,
        identifier=final_relation.identifier
  ) -%}
  {%- if relation_check is none -%}
    -- Таблица не существует: создаем
    CREATE TABLE {{ final_relation.render() }} AS{{ ' ' }}{{ sql }}
  {%- else -%}
    -- Таблица уже существует: пропускаем CREATE TABLE (append-only режим)
    -- Данные будут добавлены через post_hook
    SELECT 1 as skip_create;
  {%- endif -%}
{%- endmacro -%}

