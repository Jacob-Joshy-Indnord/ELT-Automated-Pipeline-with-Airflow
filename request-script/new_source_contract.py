"""Provisional contract for the second sales feed.

Every value here is a documented ASSUMPTION (see docs/new_source_contract.md)
until the source owner approves the real contract. Reconcile this module and
the tests with that contract before production promotion.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

CONTRACT_STATUS = "provisional"

# Input columns of the provisional CSV drop (format is NOT a vendor commitment).
REQUIRED_FIELDS = (
    "source_key", "source_version", "event_time", "quantity", "unit_price",
    "currency", "description", "category", "sales_channel", "order_priority",
)
OPTIONAL_FIELDS = (
    "discount", "shipping_cost", "country", "payment_method",
    "return_status", "customer_id", "invoice_no", "stock_code",
)
ALL_FIELDS = REQUIRED_FIELDS + OPTIONAL_FIELDS

# Allowed dimension values mirror the legacy dataset (case-insensitive match,
# canonical spelling stored so legacy report groupings stay consistent).
ALLOWED_VALUES = {
    "category": ("Apparel", "Electronics", "Accessories", "Stationery", "Furniture"),
    "sales_channel": ("In-store", "Online"),
    "order_priority": ("High", "Medium", "Low"),
    "return_status": ("Not Returned", "Returned"),
}
DEFAULT_RETURN_STATUS = "Not Returned"  # missing-field policy

# Currency: single currency assumed, so conversion is the identity.
ALLOWED_CURRENCIES = ("USD",)

# Timestamps: ISO-8601; naive values are taken as UTC; offsets are converted
# to UTC. Legacy invoice_date is stored naive, assumed UTC as well.
SOURCE_TIMEZONE = timezone.utc

# Late data: a record whose event_time is older than this before the batch
# as-of instant is quarantined (reason `late_arrival`).
MAX_LATE_DAYS = 7
# Future-dated records are rejected beyond this tolerance.
MAX_FUTURE_MINUTES = 5
# A batch with zero records is an error unless explicitly allowed.
ALLOW_EMPTY_BATCH = False
# dbt test threshold: fail when rejected/received exceeds this for a batch.
MAX_REJECT_RATE = 0.2

REJECT_REASONS = (
    "missing_required:{field}", "invalid_number:{field}", "invalid_timestamp",
    "invalid_value:{field}", "unsupported_currency", "late_arrival",
    "future_event_time", "duplicate_in_batch",
)


@dataclass
class Validated:
    record: dict | None = None
    reasons: list[str] = field(default_factory=list)


def _blank(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value)) or str(value).strip() == ""


def _number(value, field_name, integer=False):
    try:
        number = float(str(value).strip())
        if not math.isfinite(number):
            raise ValueError
        if integer:
            if number != int(number):
                raise ValueError
            return int(number)
        return number
    except ValueError:
        raise ValueError(f"invalid_number:{field_name}") from None


def parse_event_time(value) -> datetime:
    """Parse ISO-8601 to a naive UTC datetime (matches legacy column type)."""
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid_timestamp") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SOURCE_TIMEZONE)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def validate_record(raw: dict, as_of: datetime) -> Validated:
    """Validate one raw row. `as_of` is a naive UTC datetime. First failure wins."""
    try:
        for name in REQUIRED_FIELDS:
            if _blank(raw.get(name)):
                raise ValueError(f"missing_required:{name}")
        rec: dict = {"source_key": str(raw["source_key"]).strip()}
        rec["source_version"] = _number(raw["source_version"], "source_version", integer=True)
        if rec["source_version"] < 1:
            raise ValueError("invalid_number:source_version")
        rec["event_time"] = parse_event_time(raw["event_time"])
        # Positive-value rules are enforced in dbt (stg_new_source_sales), not here.
        rec["quantity"] = _number(raw["quantity"], "quantity", integer=True)
        rec["unit_price"] = _number(raw["unit_price"], "unit_price")
        for name in ("discount", "shipping_cost"):
            rec[name] = None if _blank(raw.get(name)) else _number(raw[name], name)
        rec["customer_id"] = None if _blank(raw.get("customer_id")) else _number(raw["customer_id"], "customer_id")
        rec["currency"] = str(raw["currency"]).strip().upper()
        if rec["currency"] not in ALLOWED_CURRENCIES:
            raise ValueError("unsupported_currency")
        rec["description"] = str(raw["description"]).strip()
        for name, allowed in ALLOWED_VALUES.items():
            value = raw.get(name)
            if _blank(value):
                if name in REQUIRED_FIELDS:
                    raise ValueError(f"missing_required:{name}")
                rec[name] = DEFAULT_RETURN_STATUS
                continue
            match = {a.lower(): a for a in allowed}.get(str(value).strip().lower())
            if match is None:
                raise ValueError(f"invalid_value:{name}")
            rec[name] = match
        for name in ("country", "payment_method", "invoice_no", "stock_code"):
            rec[name] = None if _blank(raw.get(name)) else str(raw[name]).strip()
        if rec["event_time"] < as_of - timedelta(days=MAX_LATE_DAYS):
            raise ValueError("late_arrival")
        if rec["event_time"] > as_of + timedelta(minutes=MAX_FUTURE_MINUTES):
            raise ValueError("future_event_time")
        return Validated(record=rec)
    except ValueError as exc:
        return Validated(reasons=[str(exc)])
