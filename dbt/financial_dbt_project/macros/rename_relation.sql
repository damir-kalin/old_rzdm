{% macro mysql__rename_relation(from_relation, to_relation) -%}
  {%- set from_full = from_relation.render() -%}
  {%- set to_full = to_relation.render() -%}
  {%- if from_relation.type == 'table' -%}
    -- For StarRocks: use CREATE TABLE ... AS SELECT instead of RENAME TABLE
    -- StarRocks doesn't support RENAME TABLE, so we copy data and drop temp table
    DROP TABLE IF EXISTS {{ to_full }};
    CREATE TABLE {{ to_full }} AS SELECT * FROM {{ from_full }};
    DROP TABLE {{ from_full }};
  {%- elif from_relation.type == 'view' -%}
    DROP VIEW IF EXISTS {{ to_full }};
    CREATE VIEW {{ to_full }} AS SELECT * FROM {{ from_full }};
    DROP VIEW {{ from_full }};
  {%- else -%}
    -- Fallback: try ALTER TABLE RENAME (StarRocks syntax)
    ALTER TABLE {{ from_full }} RENAME {{ to_full }};
  {%- endif -%}
{%- endmacro %}

