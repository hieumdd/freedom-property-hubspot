from typing import Optional, Any
from datetime import datetime, timezone

import requests

from libs.hubspot import get
from libs.bigquery import get_latest_data, load

DATASET = "HubSpot"
TABLE = "Deals"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def get_date_range(
    dataset: str,
    table: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> tuple[datetime, datetime]:
    if start and end:
        return (
            datetime.strptime(start, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc),
            datetime.strptime(end, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc),
        )
    else:
        return get_latest_data(dataset, table), datetime.utcnow()


def transform_properties(properties: dict) -> list[dict]:
    return [{"key": key, "value": value} for key, value in properties.items()]


def transform(rows: list[dict]) -> list[dict]:
    return [
        {
            "id": row["id"],
            "properties": transform_properties(row["properties"]),
            "hs_lastmodifieddate": row["properties"]["hs_lastmodifieddate"],
            "createdAt": row["createdAt"],
            "updatedAt": row["updatedAt"],
            "archived": row["archived"],
        }
        for row in rows
    ]


def run(dataset: str, table: str, request_data: dict) -> dict:
    start: Optional[str] = request_data.get("start")
    end: Optional[str] = request_data.get("end")
    with requests.Session() as session:
        data = get(
            session,
            *get_date_range(dataset, table, start, end),
        )
    response: dict[str, Any] = {
        "start": start,
        "end": end,
        "num_processed": len(data),
    }
    if len(data) > 0:
        response = {
            **response,
            "output_rows": load(dataset, table, transform(data)),
        }
    return response
