"""Pick up the daily Shopify order CSV.

One file is delivered per UTC business day D, covering D's orders. Its
location is configurable because the delivery path and naming rule have not
been fixed yet:

    SHOPIFY_INBOX_DIR     directory holding the deliveries
                          (default /opt/airflow/data/shopify)
    SHOPIFY_FILE_PATTERN  str.format pattern for the file name, given the
                          business date as ``date``
                          (default shopify_orders_{date:%Y-%m-%d}.csv)

Unlike load_data.py, nothing is read at import time, so a missing Shopify file
cannot break importing or running the existing online-sales loader. A missing
file is returned as ``None``; reporting its absence belongs to the new_source
freshness check. The reader keeps every row and every value as delivered text:
date windows and type rules belong to the new_source validation.
"""
import io
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_INBOX_DIR = "/opt/airflow/data/shopify"
DEFAULT_FILE_PATTERN = "shopify_orders_{date:%Y-%m-%d}.csv"


@dataclass(frozen=True)
class ShopifyBatch:
    """The raw Shopify delivery for one business date."""

    business_date: date
    path: Path
    content: bytes  # exact file bytes, the input to the content-hash batch id
    rows: pd.DataFrame  # every delivered row, all columns as str


def business_date_for_run(run_at: datetime) -> date:
    """Return business date D for a run: the UTC calendar day before ``run_at``."""
    if run_at.tzinfo is None:
        raise ValueError("run_at must be timezone-aware")
    return run_at.astimezone(timezone.utc).date() - timedelta(days=1)


def shopify_file_path(business_date: date, inbox_dir: Union[str, Path, None] = None) -> Path:
    """Return the one path the Shopify file for ``business_date`` is expected at."""
    inbox = Path(inbox_dir or os.environ.get("SHOPIFY_INBOX_DIR", DEFAULT_INBOX_DIR))
    pattern = os.environ.get("SHOPIFY_FILE_PATTERN", DEFAULT_FILE_PATTERN)
    return inbox / pattern.format(date=business_date)


def read_shopify_batch(
    business_date: date, inbox_dir: Union[str, Path, None] = None
) -> Optional[ShopifyBatch]:
    """Read the Shopify CSV for ``business_date``, or return None if it is absent."""
    path = shopify_file_path(business_date, inbox_dir)
    if not path.is_file():
        log.warning("No Shopify file for %s at %s", business_date.isoformat(), path)
        return None

    content = path.read_bytes()
    try:
        rows = pd.read_csv(
            io.BytesIO(content),
            dtype=str,
            keep_default_na=False,  # keep blanks as "", let validation decide
            encoding="utf-8-sig",  # Shopify exports may carry a BOM
        )
    except pd.errors.EmptyDataError:
        rows = pd.DataFrame(dtype=str)
    log.info("Read %d Shopify rows for %s from %s", len(rows), business_date.isoformat(), path)
    return ShopifyBatch(business_date=business_date, path=path, content=content, rows=rows)
