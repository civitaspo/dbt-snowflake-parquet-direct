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


def test_previous_version_keeps_files_readable_after_a_type_change(environment):
    schema = f"{os.environ['SNOWFLAKE_DATABASE']}.{environment.schema}"
    environment.put("versioned_integer/d=1/integer.parquet", flags([1, 2], [0, 1], pa.int64()))
    ok, output = environment.dbt("run", "-s", "versioned_table", "--full-refresh")
    assert ok, output

    environment.put("versioned_boolean/d=2/boolean.parquet", flags([3, 4], [True, False], pa.bool_()))
    previous = {"table": "versioned_table_before_boolean"}
    converted = {**previous, "columns": {"is_active": "case is_active when 0 then false when 1 then true end"}}
    common = {"is_active_type": "boolean", "versioned_directory": "versioned_boolean"}
    ok, output = environment.dbt("run", "-s", "versioned_table", previous_versions=[converted], is_active_type="boolean")
    assert not ok and "R08" in output and "fix them before running it" in output, "a rename that cannot help must not be suggested: " + output
    assert environment.sql(f"show iceberg tables like 'VERSIONED_TABLE' in schema {schema}"), "nothing is renamed or created before the checks pass"

    ok, output = environment.dbt("run", "-s", "versioned_table", previous_versions=[converted], **common)
    assert not ok
    rename = f"alter iceberg table {environment.table('versioned_table')} rename to {environment.table('"VERSIONED_TABLE_BEFORE_BOOLEAN"')}"
    assert "R06" in output and ("rename the table that holds its files first: " + rename).lower() in output.lower(), output
    environment.sql(rename)

    ok, output = environment.dbt("run", "-s", "versioned_table", previous_versions=[previous], **common)
    assert not ok
    assert "R09" in output and "IS_ACTIVE" in output, output
    assert not environment.sql(f"show objects like 'VERSIONED_TABLE__CURRENT' in schema {schema}")

    ok, output = environment.dbt("run", "-s", "versioned_table", previous_versions=[converted], on_configuration_change="fail", **common)
    assert ok, "creating the current table builds the view whatever on_configuration_change says: " + output
    ok, output = environment.dbt("run", "-s", "versioned_table", previous_versions=[converted], on_configuration_change="fail", **common)
    assert ok and output.startswith("unchanged"), "the stored view definition must read back as unchanged: " + output
    rows = environment.sql(f"select id, is_active, d from {environment.table('versioned_table')} order by id")
    assert [(row["ID"], row["IS_ACTIVE"], row["D"]) for row in rows] == [(1, False, "1"), (2, True, "1"), (3, True, "2"), (4, False, "2")]
    assert environment.sql(f"show views like 'VERSIONED_TABLE' in schema {schema}")
    described = {row["name"]: row["comment"] for row in environment.sql(f"desc view {environment.table('versioned_table')}")}
    assert described["IS_ACTIVE"] == "whether the customer is active", described

    ok, output = environment.dbt("test", "-s", "versioned_table", previous_versions=[converted], **common)
    assert ok, output

    environment.sql(f"create masking policy if not exists {schema}.flag_mask as (value boolean) returns boolean -> value")
    environment.sql(f"alter view {environment.table('versioned_table')} modify column is_active set masking policy {schema}.flag_mask")
    environment.sql(f"grant select on view {environment.table('versioned_table')} to role public")
    policies = f"select policy_name from table({os.environ['SNOWFLAKE_DATABASE']}.information_schema.policy_references(ref_entity_name => '{environment.table('versioned_table')}', ref_entity_domain => 'view'))"
    inverted = {**previous, "columns": {"is_active": "is_active = 0"}}
    ok, output = environment.dbt("run", "-s", "versioned_table", previous_versions=[inverted], on_configuration_change="fail", **common)
    assert not ok and "the combined view differs from its declaration" in output, output
    assert [row["POLICY_NAME"] for row in environment.sql(policies)] == ["FLAG_MASK"], "fail leaves the view as it is"
    ok, output = environment.dbt("run", "-s", "versioned_table", previous_versions=[inverted], on_configuration_change="continue", **common)
    assert ok and output.startswith("altered"), "continue still re-creates the view: " + output
    rows = environment.sql(f"select id, is_active from {environment.table('versioned_table')} where d = '1' order by id")
    assert [(row["ID"], row["IS_ACTIVE"]) for row in rows] == [(1, True), (2, False)], rows
    assert environment.sql(policies) == [], "re-creating drops a policy that the model does not declare"
    grants = f"show grants on view {environment.table('versioned_table')}"
    assert ("SELECT", "PUBLIC") in [(row["privilege"], row["grantee_name"]) for row in environment.sql(grants)], "re-creating the view keeps its grants"
    ok, output = environment.dbt("run", "-s", "versioned_table", previous_versions=[inverted], on_configuration_change="fail", **common)
    assert ok and output.startswith("unchanged"), "the re-created view must read back as unchanged: " + output
    ok, output = environment.dbt("run-operation", "parquet_direct_check_registration", "--args", "{model: versioned_table}", previous_versions=[inverted], **common)
    assert ok, output

    environment.put("versioned_string/d=3/string.parquet", flags([5, 6], ["true", "false"], pa.string()))
    stringified = {"table": "versioned_table_before_string", "columns": {"is_active": "iff(is_active, 'true', 'false')"}}
    second = {"is_active_type": "varchar", "versioned_directory": "versioned_string", "refresh_paths": ["d=3/"], "previous_versions": [inverted, stringified]}
    current = f"show iceberg tables like 'VERSIONED_TABLE__CURRENT' in schema {schema}"
    ok, output = environment.dbt("run", "-s", "versioned_table", **second)
    assert not ok and "R15" in output and "fix them before running it" in output, output
    assert environment.sql(current), "the current table must not be renamed or replaced before the checks pass"

    ok, output = environment.dbt("run", "-s", "versioned_table", refresh_on_create=False, **second)
    assert not ok
    rename = f"alter iceberg table {environment.table('versioned_table__current')} rename to {environment.table('"VERSIONED_TABLE_BEFORE_STRING"')}"
    assert "R06" in output and ("rename the table that holds its files first: " + rename).lower() in output.lower(), output
    environment.sql(rename)
    ok, output = environment.dbt("run", "-s", "versioned_table", refresh_on_create=False, is_active_masking_policy=f"{schema}.flag_mask", **second)
    assert not ok and environment.sql(current), "a boolean masking policy on the varchar column fails the view after the current table exists: " + output
    ok, output = environment.dbt("run", "-s", "versioned_table", refresh_on_create=False, on_configuration_change="continue", **second)
    assert ok, "a continue rerun finishes the view of the failed type change: " + output
    rows = environment.sql(f"select id, is_active, d from {environment.table('versioned_table')} order by id")
    assert [(row["ID"], row["IS_ACTIVE"], row["D"]) for row in rows] == [
        (1, "true", "1"), (2, "false", "1"), (3, "true", "2"), (4, "false", "2"), (5, "true", "3"), (6, "false", "3"),
    ], rows
    types = {row["name"]: row["type"] for row in environment.sql(f"desc view {environment.table('versioned_table')}")}
    assert types["IS_ACTIVE"].startswith("VARCHAR"), types
    assert ("SELECT", "PUBLIC") in [(row["privilege"], row["grantee_name"]) for row in environment.sql(grants)], "re-creating the view keeps its grants"
    ok, output = environment.dbt("run", "-s", "versioned_table", refresh_on_create=False, on_configuration_change="fail", **second)
    assert ok and output.startswith("unchanged"), "the re-created view must read back as unchanged: " + output


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
