"""Integration tests for the parquet_direct materialization against Snowflake and Amazon S3.

They need a Snowflake account and an S3 bucket behind an external volume, so they are skipped unless the
environment variables in REQUIRED are set. See ../README.md. Each session uses a fresh schema and S3 prefix
and removes both at the end.
"""

import io
import json
import os
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import snowflake.connector
from dbt.cli.main import dbtRunner

PROJECT = Path(__file__).resolve().parent.parent
REQUIRED = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "PARQUET_DIRECT_EXTERNAL_VOLUME",
    "PARQUET_DIRECT_CATALOG_NONE",
    "PARQUET_DIRECT_CATALOG_HIVE",
    "PARQUET_DIRECT_STORAGE_URL",
]
pytestmark = pytest.mark.skipif(
    any(not os.environ.get(name) for name in REQUIRED),
    reason="needs Snowflake and S3; see integration_tests_snowflake/README.md",
)


def parquet(ids, id_type=pa.int32()):
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": pa.array(ids, id_type), "message": [f"m{i}" for i in ids]}), buffer)
    return buffer.getvalue()


def inferred_parquet(row_id, flag, **additional_columns):
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": [row_id], "flag": [flag], "Extra": ["value"], **additional_columns}), buffer)
    return buffer.getvalue()


class IntegrationEnvironment:
    def __init__(self):
        run_id = uuid.uuid4().hex[:8]
        self.schema = f"PARQUET_DIRECT_INTEGRATION_TEST_{run_id}".upper()
        self.base = f"{os.environ.get('PARQUET_DIRECT_BASE_PREFIX', 'integration_test').strip('/')}/{run_id}"
        storage = urlparse(os.environ["PARQUET_DIRECT_STORAGE_URL"])  # the external volume's STORAGE_BASE_URL
        self.bucket = storage.netloc
        self.root = storage.path.strip("/")
        self.s3 = boto3.client("s3")
        os.environ["PARQUET_DIRECT_SCHEMA"] = self.schema
        self.vars = {
            "external_volume": os.environ["PARQUET_DIRECT_EXTERNAL_VOLUME"],
            "catalog_none": os.environ["PARQUET_DIRECT_CATALOG_NONE"],
            "catalog_hive": os.environ["PARQUET_DIRECT_CATALOG_HIVE"],
            "base_prefix": self.base,
        }
        self.connection = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "snowflake"),
            client_store_temporary_credential=True,
            password=os.environ.get("SNOWFLAKE_PASSWORD") or None,
            private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH") or None,
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
        )

    def key(self, path):
        return "/".join(part for part in [self.root, self.base, path] if part)

    def put(self, path, body):
        self.s3.put_object(Bucket=self.bucket, Key=self.key(path), Body=body)

    def sql(self, query):
        with self.connection.cursor(snowflake.connector.DictCursor) as cursor:
            return cursor.execute(query).fetchall()

    def table(self, name):
        return f"{os.environ['SNOWFLAKE_DATABASE']}.{self.schema}.{name}"

    def count(self, name):
        return self.sql(f"select count(*) as n from {self.table(name)}")[0]["N"]

    def dbt(self, *args, **overrides):
        command = [*args, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
        if args[0] not in ("deps",):
            command += ["--vars", json.dumps({**self.vars, **overrides})]
        result = dbtRunner().invoke(command)
        messages = [str(r.message) for r in getattr(result.result, "results", None) or []]
        if result.exception:
            messages.append(str(result.exception))
        return result.success, "\n".join(messages)

    def cleanup(self):
        self.sql(f"drop schema if exists {os.environ['SNOWFLAKE_DATABASE']}.{self.schema} cascade")
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.key("") + "/"):
            keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if keys:
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})
        self.connection.close()


@pytest.fixture(scope="module")
def environment():
    environment = IntegrationEnvironment()
    environment.sql(f"create schema if not exists {os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}")
    ok, output = environment.dbt("deps")
    assert ok, output
    yield environment
    environment.cleanup()


def test_create_then_rerun_is_unchanged(environment):
    environment.put("create_and_rerun/d=2026-11-15/a.parquet", parquet([1, 2]))
    ok, output = environment.dbt("run", "-s", "manual_table", "--full-refresh", data_directory="create_and_rerun")
    assert ok, output
    assert output.startswith("created"), output
    assert environment.count("manual_table") == 2

    ok, output = environment.dbt("run", "-s", "manual_table", data_directory="create_and_rerun")
    assert ok, output
    assert output.startswith("unchanged"), output


def test_alterable_change_is_applied(environment):
    ok, output = environment.dbt("run", "-s", "manual_table", data_directory="create_and_rerun", table_comment="v2")
    assert ok, output
    assert output.startswith("altered"), output
    shown = environment.sql(f"show iceberg tables like 'MANUAL_TABLE' in schema {os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}")
    assert shown[0]["comment"] == "v2"


def test_recreation_required_change_stops_until_full_refresh(environment):
    ok, output = environment.dbt("run", "-s", "manual_table", data_directory="create_and_rerun", table_comment="v2", id_type="long")
    assert not ok
    assert "ALTER cannot apply" in output and "ID type" in output, output

    ok, output = environment.dbt("run", "-s", "manual_table", "--full-refresh", data_directory="create_and_rerun", table_comment="v2", id_type="long")
    assert ok, output
    columns = {row["name"]: row["type"] for row in environment.sql(f"desc iceberg table {environment.table('manual_table')}")}
    assert columns["ID"] == "NUMBER(19,0)"
    assert environment.count("manual_table") == 2


def test_full_refresh_false_blocks_recreation(environment):
    ok, output = environment.dbt("run", "-s", "manual_table", "--full-refresh", data_directory="create_and_rerun", table_comment="v2", full_refresh_setting=False)
    assert not ok
    assert "full_refresh: false" in output, output


def test_refresh_paths_register_only_listed_paths(environment):
    environment.put("refresh_paths/d=1/a.parquet", parquet([1]))
    environment.put("refresh_paths/d=2/b.parquet", parquet([2, 3]))
    common = {"data_directory": "refresh_paths", "refresh_on_create": False}
    ok, output = environment.dbt("run", "-s", "manual_table", "--full-refresh", refresh_paths=["d=1/"], **common)
    assert ok, output
    assert environment.count("manual_table") == 1

    ok, output = environment.dbt("run", "-s", "manual_table", refresh_paths=["d=2/"], **common)
    assert ok, output
    assert environment.count("manual_table") == 3

    environment.put("refresh_paths/d=3/c.parquet", parquet([4]))
    ok, output = environment.dbt("run", "-s", "manual_table", refresh_paths=["no-such-dir/"], **common)
    assert ok, output
    # Snowflake's documentation says a path that does not exist refreshes the whole base location; on 2026-11-20 it
    # registered nothing. R08 follows the documentation, so this records the observed behavior, not a guarantee.
    assert environment.count("manual_table") == 3, "a refresh of a path that does not exist registered the other paths' files"


def test_infers_columns_from_matched_file_on_create_and_full_refresh(environment):
    environment.put("inferred/d=1/old.parquet", inferred_parquet(1, 1))
    environment.put("inferred/d=2/a.parquet", inferred_parquet(2, True))
    environment.put("inferred/d=2/z.parquet", inferred_parquet(3, False, only_in_z=["value"]))

    leftover = "__TMP_DBT_PARQUET_DIRECT__INFERRED_TABLE"
    environment.sql(f"""
        create iceberg table {environment.table(leftover)} (id long)
        external_volume = '{environment.vars["external_volume"]}'
        catalog = '{environment.vars["catalog_none"]}'
        base_location = '{environment.base}/inferred/'
        refresh_on_create = false
    """)
    common = {
        "inference_test_enabled": True,
        "refresh_paths": ["d=2/"],
    }
    table = environment.table("inferred_table")
    ok, output = environment.dbt("run", "-s", "inferred_table", inference_pattern="d=2/a[.]parquet", **common)
    assert ok and output.startswith("created"), output
    assert inference_probe_names(environment) == set(), "the next inference replaces and drops a leftover probe"
    columns = {row["name"]: row["type"] for row in environment.sql(f"desc iceberg table {table}")}
    assert set(columns) == {"ID", "FLAG", "EXTRA", "D"}, columns
    assert columns["FLAG"] == "BOOLEAN"
    assert environment.sql(f"select id, flag, d from {table} order by id") == [
        {"ID": 2, "FLAG": True, "D": "2"},
        {"ID": 3, "FLAG": False, "D": "2"},
    ]

    ok, output = environment.dbt("run", "-s", "inferred_table", inference_pattern="d=2/missing[.]parquet", **common)
    assert ok and output.startswith("unchanged"), output

    ok, output = environment.dbt("run", "-s", "inferred_table", "--full-refresh", inference_pattern="d=2/z[.]parquet", **common)
    assert ok and output.startswith("replaced"), output
    columns = {row["name"] for row in environment.sql(f"desc iceberg table {table}")}
    assert "ONLY_IN_Z" in columns
    assert environment.count("inferred_table") == 2
    assert inference_probe_names(environment) == set()


def inference_probe_names(environment):
    schema = f"{os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}"
    return {row["name"] for row in environment.sql(f"show tables in schema {schema}") if row["name"].startswith("__TMP_DBT_PARQUET_DIRECT__")}


def test_inference_fails_when_no_file_matches(environment):
    environment.put("inferred/d=2/a.parquet", inferred_parquet(2, True))
    ok, output = environment.dbt(
        "run", "-s", "inferred_table", inference_test_enabled=True,
        inference_test_alias="inferred_without_match", inference_pattern="d=2/missing[.]parquet",
    )
    assert not ok and "091574" in output, output
    schema = f"{os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}"
    assert not environment.sql(f"show objects like 'INFERRED_WITHOUT_MATCH' in schema {schema}")
    assert inference_probe_names(environment) == set()


def test_infers_columns_for_an_alias_at_the_qualified_name_limit(environment):
    qualifier = f"{os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}."
    model_alias = "a" * (255 - len(qualifier))
    environment.put("inferred/d=4/long_alias.parquet", inferred_parquet(4, True))
    ok, output = environment.dbt(
        "run", "-s", "inferred_table", inference_test_enabled=True,
        inference_test_alias=model_alias, inference_pattern="d=4/long_alias[.]parquet", refresh_paths=["d=4/"],
    )
    assert ok and output.startswith("created"), output
    table = environment.table(model_alias)
    columns = {row["name"]: row["type"] for row in environment.sql(f"desc iceberg table {table}")}
    assert set(columns) == {"ID", "FLAG", "EXTRA", "D"}, columns
    assert columns["FLAG"] == "BOOLEAN"
    assert environment.count(model_alias) == 1
    assert inference_probe_names(environment) == set()


def test_skipped_file_fails_the_run(environment):
    environment.put("skipped_file/d=1/good.parquet", parquet([1]))
    environment.put("skipped_file/d=1/broken.parquet", b"not parquet")
    common = {"data_directory": "skipped_file", "refresh_on_create": False, "refresh_paths": ["d=1/"]}
    ok, output = environment.dbt("run", "-s", "manual_table", "--full-refresh", **common)
    assert not ok
    assert "skipped file(s)" in output and "broken.parquet" in output, output
    assert environment.count("manual_table") == 1

    ok, output = environment.dbt("run", "-s", "manual_table", on_skipped="warn", **common)
    assert ok, output


def test_generic_test_reports_skipped_file(environment):
    ok, output = environment.dbt("test", "-s", "manual_table", "--exclude", "tag:ignore_check", data_directory="skipped_file", refresh_on_create=False, refresh_paths=["d=1/"])
    assert not ok, output


def test_ignore_patterns_agree_between_run_and_test(environment):
    # The model run evaluates the pattern in Python and the generic test in Snowflake.
    common = {"data_directory": "skipped_file", "refresh_on_create": False, "refresh_paths": ["d=1/"]}
    ok, output = environment.dbt("run", "-s", "manual_table", ignore_file_patterns=["/broken[.]parquet$"], **common)
    assert ok, output
    ok, output = environment.dbt("test", "-s", "tag:ignore_check", **common)
    assert ok, output


def test_run_operation_reports_skipped_file(environment):
    ok, output = environment.dbt(
        "run-operation", "parquet_direct_check_registration", "--args", "{model: manual_table, lookback_hours: 1}",
        data_directory="skipped_file", refresh_on_create=False, refresh_paths=["d=1/"],
    )
    assert not ok
    assert "broken.parquet" in output, output


def test_type_conflict_fails_the_refresh(environment):
    environment.put("type_conflict/d=1/int.parquet", parquet([1]))
    common = {"data_directory": "type_conflict", "refresh_paths": ["d=2/"]}
    ok, output = environment.dbt("run", "-s", "manual_table", "--full-refresh", **common)
    assert ok, output
    environment.put("type_conflict/d=2/string.parquet", parquet(["x"], pa.string()))
    ok, output = environment.dbt("run", "-s", "manual_table", **common)
    assert not ok
    assert "incompatible" in output.lower(), output


def flags(ids, values, value_type):
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": pa.array(ids, pa.int64()), "is_active": pa.array(values, value_type)}), buffer)
    return buffer.getvalue()


@pytest.mark.parametrize("fail_view", [False, True], ids=["runs-once", "retries-view"])
def test_previous_version_keeps_files_readable_after_a_type_change(environment, capsys, fail_view):
    alias = "versioned_table_retry" if fail_view else "versioned_table"
    schema = f"{os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}"
    view = environment.table(alias)
    integer_directory = f"{alias}_integer"
    boolean_directory = f"{alias}_boolean"
    string_directory = f"{alias}_string"

    def run(*args, **options):
        return environment.dbt("run", "-s", "versioned_table", *args, versioned_alias=alias, **options)

    environment.put(f"{integer_directory}/d=1/integer.parquet", flags([1, 2], [0, 1], pa.int64()))
    ok, output = run("--full-refresh", versioned_directory=integer_directory)
    assert ok, output

    environment.put(f"{boolean_directory}/d=2/boolean.parquet", flags([3, 4], [True, False], pa.bool_()))
    previous = {"table": f"{alias}_before_boolean"}
    converted = {**previous, "columns": {"is_active": "case is_active when 0 then false when 1 then true end"}}
    common = {"is_active_type": "boolean", "versioned_directory": boolean_directory}
    public_table = f"show iceberg tables like '{alias.upper()}' in schema {schema}"
    current = f"show iceberg tables like '{alias.upper()}__CURRENT' in schema {schema}"
    archived_integer = f"show iceberg tables like '{previous['table'].upper()}' in schema {schema}"
    ok, output = run(previous_versions=[converted], is_active_type="boolean", versioned_directory=integer_directory)
    assert not ok and "R08" in output and "set refresh_paths" in output, output
    assert environment.sql(public_table) and not environment.sql(current) and not environment.sql(archived_integer)

    ok, output = run(previous_versions=[previous], **common)
    assert not ok and "R09" in output and "IS_ACTIVE" in output, output
    assert environment.sql(public_table) and not environment.sql(current) and not environment.sql(archived_integer)

    capsys.readouterr()
    ok, output = run(previous_versions=[converted], on_configuration_change="fail", **common)
    assert ok, "creating the current table builds the view whatever on_configuration_change says: " + output
    assert "Renamed" in capsys.readouterr().out
    assert environment.sql(archived_integer)

    ok, output = run(previous_versions=[converted], on_configuration_change="fail", **common)
    assert ok and output.startswith("unchanged"), "the stored view definition must read back as unchanged: " + output
    rows = environment.sql(f"select id, is_active, d from {view} order by id")
    assert [(row["ID"], row["IS_ACTIVE"], row["D"]) for row in rows] == [(1, False, "1"), (2, True, "1"), (3, True, "2"), (4, False, "2")]
    assert environment.sql(f"show views like '{alias.upper()}' in schema {schema}")
    described = {row["name"]: row["comment"] for row in environment.sql(f"desc view {view}")}
    assert described["IS_ACTIVE"] == "whether the customer is active", described

    ok, output = environment.dbt("test", "-s", "versioned_table", previous_versions=[converted], versioned_alias=alias, **common)
    assert ok, output

    environment.sql(f"create masking policy if not exists {schema}.flag_mask as (value boolean) returns boolean -> value")
    environment.sql(f"alter view {view} modify column is_active set masking policy {schema}.flag_mask")
    environment.sql(f"grant select on view {view} to role public")
    policies = f"select policy_name from table({os.environ['SNOWFLAKE_DATABASE']}.information_schema.policy_references(ref_entity_name => '{view}', ref_entity_domain => 'view'))"
    inverted = {**previous, "columns": {"is_active": "is_active = 0"}}
    ok, output = run(previous_versions=[inverted], on_configuration_change="fail", **common)
    assert not ok and "the combined view differs from its declaration" in output, output
    assert [row["POLICY_NAME"] for row in environment.sql(policies)] == ["FLAG_MASK"], "fail leaves the view as it is"
    ok, output = run(previous_versions=[inverted], on_configuration_change="continue", **common)
    assert ok and output.startswith("altered"), "continue still re-creates the view: " + output
    rows = environment.sql(f"select id, is_active from {view} where d = '1' order by id")
    assert [(row["ID"], row["IS_ACTIVE"]) for row in rows] == [(1, True), (2, False)], rows
    assert environment.sql(policies) == [], "re-creating drops a policy that the model does not declare"
    grants = f"show grants on view {view}"
    assert ("SELECT", "PUBLIC") in [(row["privilege"], row["grantee_name"]) for row in environment.sql(grants)], "re-creating the view keeps its grants"
    ok, output = run(previous_versions=[inverted], on_configuration_change="fail", **common)
    assert ok and output.startswith("unchanged"), "the re-created view must read back as unchanged: " + output
    ok, output = environment.dbt("run-operation", "parquet_direct_check_registration", "--args", "{model: versioned_table}", previous_versions=[inverted], versioned_alias=alias, **common)
    assert ok, output

    environment.put(f"{string_directory}/d=3/string.parquet", flags([5, 6], ["true", "false"], pa.string()))
    stringified = {"table": f"{alias}_before_string", "columns": {"is_active": "iff(is_active, 'true', 'false')"}}
    second = {
        "is_active_type": "varchar",
        "versioned_directory": string_directory,
        "refresh_paths": ["d=3/"],
        "previous_versions": [inverted, stringified],
    }
    capsys.readouterr()
    if fail_view:
        ok, output = run(is_active_masking_policy=f"{schema}.flag_mask", **second)
        assert not ok and "003530" in output, output
        assert "Renamed" in capsys.readouterr().out
        assert environment.sql(current), "the view must fail after the current table is created"
        ok, output = run(on_configuration_change="continue", **second)
        assert ok, "a continue rerun finishes the view of the failed type change: " + output
        assert "Renamed" not in capsys.readouterr().out
    else:
        ok, output = run(**second)
        assert ok, output
        assert "Renamed" in capsys.readouterr().out

    assert environment.sql(f"show iceberg tables like '{stringified['table'].upper()}' in schema {schema}")
    rows = environment.sql(f"select id, is_active, d from {view} order by id")
    assert [(row["ID"], row["IS_ACTIVE"], row["D"]) for row in rows] == [
        (1, "true", "1"), (2, "false", "1"), (3, "true", "2"), (4, "false", "2"), (5, "true", "3"), (6, "false", "3"),
    ], rows
    types = {row["name"]: row["type"] for row in environment.sql(f"desc view {view}")}
    assert types["IS_ACTIVE"].startswith("VARCHAR"), types
    assert ("SELECT", "PUBLIC") in [(row["privilege"], row["grantee_name"]) for row in environment.sql(grants)], "re-creating the view keeps its grants"
    ok, output = run(on_configuration_change="fail", **second)
    assert ok and output.startswith("unchanged"), "the re-created view must read back as unchanged: " + output


def test_type_changes_share_a_base_location(environment):
    common = {"versioned_alias": "shared_versions", "versioned_directory": "shared_versions"}
    view = environment.table("shared_versions")
    environment.put("shared_versions/d=1/integer.parquet", flags([1, 2], [0, 1], pa.int64()))
    ok, output = environment.dbt("run", "-s", "versioned_table", **common)
    assert ok, output

    environment.put("shared_versions/d=2/boolean.parquet", flags([3, 4], [True, False], pa.bool_()))
    first = {"table": "shared_versions_before_boolean", "columns": {"is_active": "is_active = 1"}}
    ok, output = environment.dbt(
        "run", "-s", "versioned_table", is_active_type="boolean", previous_versions=[first], refresh_paths=["d=2/"], **common,
    )
    assert ok, output
    rows = environment.sql(f"select id, is_active from {view} order by id")
    assert [(row["ID"], row["IS_ACTIVE"]) for row in rows] == [(1, False), (2, True), (3, True), (4, False)]

    environment.put("shared_versions/d=3/string.parquet", flags([5, 6], ["true", "false"], pa.string()))
    first["columns"] = {"is_active": "iff(is_active = 1, 'true', 'false')"}
    second = {"table": "shared_versions_before_string", "columns": {"is_active": "iff(is_active, 'true', 'false')"}}
    ok, output = environment.dbt(
        "run", "-s", "versioned_table", is_active_type="varchar", previous_versions=[first, second], refresh_paths=["d=3/"], **common,
    )
    assert ok, output
    rows = environment.sql(f"select id, is_active from {view} order by id")
    assert [(row["ID"], row["IS_ACTIVE"]) for row in rows] == [
        (1, "false"), (2, "true"), (3, "true"), (4, "false"), (5, "true"), (6, "false"),
    ]
    for table, expected_ids in [
        (first["table"], [1, 2]), (second["table"], [3, 4]), ("shared_versions__current", [5, 6]),
    ]:
        assert [row["ID"] for row in environment.sql(f"select id from {environment.table(table)} order by id")] == expected_ids


def test_invalid_config_fails_before_any_sql(environment):
    ok, output = environment.dbt("run", "-s", "guarded_table", guarded_table_enabled=True, guarded_base_location="no-trailing-slash")
    assert not ok
    assert "S03" in output, output
    assert not environment.sql(f"show objects like 'GUARDED_TABLE' in schema {os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}")


def test_other_object_is_never_replaced(environment):
    environment.sql(f"create view {environment.table('occupied_table')} as select 1 as id")
    ok, output = environment.dbt("run", "-s", "guarded_table", "--full-refresh", guarded_table_enabled=True, guarded_alias="occupied_table")
    assert not ok
    assert "R03" in output, output
    assert environment.sql(f"show views like 'OCCUPIED_TABLE' in schema {os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}")


def test_hive_partition_column_is_inferred(environment):
    environment.put("hive/d=2026-11-15/a.parquet", parquet([1, 2]))
    ok, output = environment.dbt("run", "-s", "hive_table", "--full-refresh")
    assert ok, output
    kinds = {row["name"]: row["kind"] for row in environment.sql(f"desc iceberg table {environment.table('hive_table')}")}
    assert kinds.get("D") == "PARTITION", kinds

    ok, output = environment.dbt("run", "-s", "hive_table")
    assert ok, output
    assert output.startswith("unchanged"), output


def test_pattern_inference_is_rejected_for_hive_catalog(environment):
    ok, output = environment.dbt("run", "-s", "hive_table", "--full-refresh", inference_pattern="d=2026-11-15/a[.]parquet")
    assert not ok and "R16" in output, output


def test_governance_settings_round_trip(environment):
    schema = f"{os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}"
    environment.sql(f"create tag if not exists {schema}.governance_tag")
    environment.sql(f"create masking policy if not exists {schema}.message_mask as (value string) returns string -> value")
    environment.sql(f"create row access policy if not exists {schema}.allow_all as (value int) returns boolean -> true")
    environment.put("governed/a.parquet", parquet([1, 2]))
    names = {
        "governed_table_enabled": True,
        "tag_name": f"{schema}.governance_tag",
        "masking_policy_name": f"{schema}.message_mask",
        "row_access_policy_name": f"{schema}.allow_all",
    }
    ok, output = environment.dbt("run", "-s", "governed_table", "--full-refresh", **names)
    assert ok, output
    assert environment.count("governed_table") == 2

    ok, output = environment.dbt("run", "-s", "governed_table", **names)
    assert ok, output
    assert output.startswith("unchanged"), "declared tags, policies, and constraints must read back as unchanged: " + output

    ok, output = environment.dbt("run", "-s", "governed_table", tag_value="v2", **names)
    assert ok, output
    assert output.startswith("altered"), output
    ok, output = environment.dbt("run", "-s", "governed_table", tag_value="v2", **names)
    assert ok, output
    assert output.startswith("unchanged"), output


@pytest.mark.skipif(not os.environ.get("PARQUET_DIRECT_AUTO_REFRESH"), reason="needs S3 event notifications; set PARQUET_DIRECT_AUTO_REFRESH=1")
def test_auto_refresh_backfills_existing_files(environment):
    environment.put("auto/a.parquet", parquet([1, 2]))
    ok, output = environment.dbt("run", "-s", "auto_refresh_table", "--full-refresh", auto_refresh_table_enabled=True)
    assert ok, output
    deadline = time.time() + 600
    while environment.count("auto_refresh_table") < 2 and time.time() < deadline:
        time.sleep(30)
    assert environment.count("auto_refresh_table") == 2

    environment.put("auto/broken.parquet", b"not parquet")
    time.sleep(120)
    ok, output = environment.dbt("run", "-s", "auto_refresh_table", auto_refresh_table_enabled=True)
    assert not ok
    assert "broken.parquet" in output, output


    environment.put("auto_versioned_integer/d=1/integer.parquet", flags([1], [1], pa.int64()))
    ok, output = environment.dbt(
        "run", "-s", "versioned_table", versioned_alias="auto_versioned_table",
        versioned_directory="auto_versioned_integer", versioned_auto_refresh=True,
    )
    assert ok, output
    source = environment.table("auto_versioned_table").upper()
    status = environment.sql(f"select system$parquet_direct_auto_refresh_status('{source}') as status")[0]["STATUS"]
    assert isinstance(json.loads(status), dict), status

    environment.put("auto_versioned_boolean/d=2/boolean.parquet", flags([2], [False], pa.bool_()))
    previous = {"table": "auto_versioned_before_boolean", "columns": {"is_active": "is_active = 1"}}
    ok, output = environment.dbt(
        "run", "-s", "versioned_table", versioned_alias="auto_versioned_table",
        versioned_directory="auto_versioned_boolean", is_active_type="boolean", previous_versions=[previous],
    )
    assert ok, output
    archived = environment.table(previous["table"]).upper()
    status = environment.sql(f"select system$parquet_direct_auto_refresh_status('{archived}') as status")[0]["STATUS"]
    assert "not enabled" in status.lower() or "disabled" in status.lower(), status
    rows = environment.sql(f"select id, is_active from {source} order by id")
    assert [(row["ID"], row["IS_ACTIVE"]) for row in rows] == [(1, True), (2, False)]
