from datetime import datetime

import pytest

import new_source_contract as c

AS_OF = datetime(2025, 1, 10, 12, 0)
GOOD = dict(source_key="K1", source_version="1", event_time="2025-01-10T08:00:00Z", quantity="3",
            unit_price="2.5", currency="USD", description="Mug", category="apparel",
            sales_channel="online", order_priority="high")


def check(**over):
    return c.validate_record({**GOOD, **over}, AS_OF)


def test_valid_record_is_normalized():
    rec = check().record
    assert rec["category"] == "Apparel" and rec["sales_channel"] == "Online"
    assert rec["return_status"] == "Not Returned" and rec["discount"] is None


def test_offset_timestamps_converted_to_utc():
    assert check(event_time="2025-01-10T09:30:00+02:00").record["event_time"] == datetime(2025, 1, 10, 7, 30)


@pytest.mark.parametrize("over,reason", [
    ({"source_key": ""}, "missing_required:source_key"),
    ({"quantity": "x"}, "invalid_number:quantity"),
    ({"quantity": "1.5"}, "invalid_number:quantity"),
    ({"unit_price": "nan"}, "invalid_number:unit_price"),
    ({"source_version": "0"}, "invalid_number:source_version"),
    ({"event_time": "yesterday"}, "invalid_timestamp"),
    ({"currency": "EUR"}, "unsupported_currency"),
    ({"category": "Toys"}, "invalid_value:category"),
    ({"event_time": "2024-12-01T00:00:00Z"}, "late_arrival"),
    ({"event_time": "2025-03-01T00:00:00Z"}, "future_event_time"),
])
def test_rejections(over, reason):
    result = check(**over)
    assert result.record is None and result.reasons == [reason]


def test_non_positive_values_are_accepted_for_dbt_to_exclude():
    assert check(quantity="0", unit_price="0").record is not None
