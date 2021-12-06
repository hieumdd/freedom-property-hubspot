from datetime import datetime

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()


def get_latest_data(dataset: str, table: str) -> datetime:
    return [
        dict(i.items())
        for i in BQ_CLIENT.query(
            f"SELECT max(hs_lastmodifieddate) as max_date FROM {dataset}.{table}"
        ).result()
    ][0]["max_date"]


def load(dataset: str, table: str, rows: list[dict]) -> int:
    output_rows = (
        BQ_CLIENT.load_table_from_json(
            rows,
            f"{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                schema=[
                    {"name": "id", "type": "INTEGER"},
                    {
                        "name": "properties",
                        "type": "RECORD",
                        "mode": "REPEATED",
                        "fields": [
                            {"name": "key", "type": "STRING"},
                            {"name": "value", "type": "STRING"},
                        ],
                    },
                    {"name": "hs_lastmodifieddate", "type": "TIMESTAMP"},
                    {"name": "createdAt", "type": "TIMESTAMP"},
                    {"name": "updatedAt", "type": "TIMESTAMP"},
                    {"name": "archived", "type": "BOOLEAN"},
                ],
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        )
        .result()
        .output_rows
    )
    _update(dataset, table)
    return output_rows


def _update(dataset: str, table: str) -> None:
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {dataset}.{table} AS
    SELECT * EXCEPT (row_num)
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() over (
                    PARTITION BY id
                    ORDER BY hs_lastmodifieddate DESC
                ) AS row_num
            FROM
                {dataset}.{table}
        ) WHERE row_num = 1
    """
    ).result()
