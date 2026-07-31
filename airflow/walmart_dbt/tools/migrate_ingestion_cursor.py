"""One-time, idempotent Lakeflow Connect cursor migration.

Only the orders and order_items definitions are changed. Other pipeline
objects, primary keys, notifications, and scheduling are preserved.
"""

from __future__ import annotations

import argparse

from databricks.sdk import WorkspaceClient


PIPELINE_ID = "ce9fc2cb-0885-423b-9f23-3637d69e78af"
SOURCE_TABLES = {"orders", "order_items"}
NEW_CURSOR = "change_version"


def migrate(apply: bool) -> None:
    workspace = WorkspaceClient()
    pipeline = workspace.pipelines.get(PIPELINE_ID)
    spec = pipeline.spec
    found: set[str] = set()
    changes: list[str] = []

    for obj in spec.ingestion_definition.objects:
        table = obj.table
        if table.source_table not in SOURCE_TABLES:
            continue
        found.add(table.source_table)
        config = table.table_configuration.query_based_connector_config
        previous = list(config.cursor_columns)
        if previous != [NEW_CURSOR]:
            changes.append(
                f"{table.source_table}: {previous!r} -> {[NEW_CURSOR]!r}"
            )
            config.cursor_columns = [NEW_CURSOR]

    if found != SOURCE_TABLES:
        raise RuntimeError(
            f"Expected {sorted(SOURCE_TABLES)}, found {sorted(found)}"
        )

    if not changes:
        print("No change required; cursor configuration is already current.")
        return

    print(*changes, sep="\n")
    if not apply:
        print("Dry run only. Pass --apply to update the pipeline.")
        return

    workspace.pipelines.update(
        PIPELINE_ID,
        name=spec.name,
        catalog=spec.catalog,
        schema=spec.schema,
        channel=spec.channel,
        ingestion_definition=spec.ingestion_definition,
        notifications=spec.notifications,
        expected_last_modified=pipeline.last_modified,
    )
    print("Pipeline cursor configuration updated.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    arguments = parser.parse_args()
    migrate(apply=arguments.apply)
