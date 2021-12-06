from unittest.mock import Mock

import pytest


from main import main


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        ("2021-12-06", "2021-12-07"),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_pipeline(start, end):
    data = {
        "start": start,
        "end": end,
    }
    res = main(Mock(get_json=Mock(return_value=data), args=data))
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["num_processed"] == res["output_rows"]
