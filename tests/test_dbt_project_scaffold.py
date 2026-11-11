"""Sanity checks for the dbt project scaffold (dbt_project/).

These don't require dbt itself (not installed in CI -- see ci.yml) and
don't talk to Spark/Delta; they just parse the YAML/SQL config dbt
would read and check it's internally consistent, so a typo in the
scaffold (a source table dbt models reference but never declares, a
missing profile key) fails fast in CI instead of only surfacing the
next time someone actually runs `dbt run`.
"""
from pathlib import Path

import yaml

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent / "dbt_project"


def load_yaml(relative_path):
    return yaml.safe_load((DBT_PROJECT_DIR / relative_path).read_text())


def test_dbt_project_yml_has_expected_shape():
    config = load_yaml("dbt_project.yml")

    assert config["name"] == "realtime_streaming_lakehouse"
    assert config["profile"] == "realtime_streaming_lakehouse"
    assert "models" in config["model-paths"]
    assert "macros" in config["macro-paths"]
    assert config["vars"]["bronze_path"]
    assert config["vars"]["silver_path"]
    assert config["on-run-start"] == ["{{ create_external_lakehouse_tables() }}"]

    layer_config = config["models"]["realtime_streaming_lakehouse"]
    for layer, expected_materialization in (
        ("bronze", "view"),
        ("silver", "view"),
        ("gold", "table"),
    ):
        assert layer_config[layer]["+materialized"] == expected_materialization
        assert layer_config[layer]["+schema"] == layer


def test_profiles_example_matches_project_profile_name():
    project_config = load_yaml("dbt_project.yml")
    profiles = load_yaml("profiles.yml.example")

    profile_name = project_config["profile"]
    assert profile_name in profiles

    dev_output = profiles[profile_name]["outputs"]["dev"]
    assert profiles[profile_name]["target"] == "dev"
    assert dev_output["type"] == "spark"
    assert dev_output["method"] == "session"


def test_sources_yml_declares_bronze_and_silver_tables():
    sources_config = load_yaml("models/sources.yml")

    lakehouse_sources = [s for s in sources_config["sources"] if s["name"] == "lakehouse"]
    assert len(lakehouse_sources) == 1

    table_names = {t["name"] for t in lakehouse_sources[0]["tables"]}
    assert table_names == {"bronze_clickstream_events", "silver_clickstream_events"}


def test_staging_models_select_from_declared_sources():
    bronze_sql = (DBT_PROJECT_DIR / "models/bronze/stg_clickstream_events.sql").read_text()
    silver_sql = (DBT_PROJECT_DIR / "models/silver/stg_clickstream_events_silver.sql").read_text()

    assert "source('lakehouse', 'bronze_clickstream_events')" in bronze_sql
    assert "source('lakehouse', 'silver_clickstream_events')" in silver_sql
    # Silver staging carries the ingestion_ts column the silver Delta
    # table adds; bronze has no such column yet.
    assert "ingestion_ts" in silver_sql
    assert "ingestion_ts" not in bronze_sql


def test_create_external_tables_macro_references_both_paths():
    macro_sql = (
        DBT_PROJECT_DIR / "macros/create_external_lakehouse_tables.sql"
    ).read_text()

    assert "var('bronze_path')" in macro_sql
    assert "var('silver_path')" in macro_sql
    assert "bronze_clickstream_events" in macro_sql
    assert "silver_clickstream_events" in macro_sql
