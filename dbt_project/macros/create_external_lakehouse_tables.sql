{#
  Registers the Delta Lake tables the Spark streaming jobs write
  (file-path based, no catalog entry of their own -- see
  streaming_job/stream_to_bronze.py and stream_to_silver.py) as external
  tables Spark SQL (and therefore dbt's source() calls) can query by
  name instead of by path.

  Run automatically via dbt_project.yml's on-run-start hook. Idempotent
  (CREATE TABLE IF NOT EXISTS), so safe to re-run on every invocation.
#}
{% macro create_external_lakehouse_tables() %}
  {% set statements = [
    "CREATE TABLE IF NOT EXISTS bronze_clickstream_events USING DELTA LOCATION '" ~ var('bronze_path') ~ "'",
    "CREATE TABLE IF NOT EXISTS silver_clickstream_events USING DELTA LOCATION '" ~ var('silver_path') ~ "'",
  ] %}
  {% if execute %}
    {% for statement in statements %}
      {% do run_query(statement) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
