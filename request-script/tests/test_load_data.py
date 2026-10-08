"""Tests for the "Read source CSV" requirement (task-FB23WIQaNYY).

load_data.py runs `df = pd.read_csv(r'data/raw/online_sales_dataset.csv')`
at module level (line 52), ahead of the try/except in main() (lines 72-79).
Because the path is relative, whether this succeeds depends entirely on the
process's current working directory at import time.
"""
import os

import pandas as pd
import pytest

from conftest import REPO_ROOT


def test_import_csv_populates_module_level_df(isolated_import):
    """Scenario: Import CSV.

    WHEN the relative CSV is accessible from the current working directory
    THEN importing the module parses it into module-level `df` before
    `main` is ever called.
    """
    os.chdir(REPO_ROOT)

    load_data = isolated_import()

    assert isinstance(load_data.df, pd.DataFrame)
    assert not load_data.df.empty
    assert list(load_data.df.columns) == [
        "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
        "UnitPrice", "CustomerID", "Country", "Discount", "PaymentMethod",
        "ShippingCost", "Category", "SalesChannel", "ReturnStatus",
        "ShipmentProvider", "WarehouseLocation", "OrderPriority",
    ]


def test_import_fails_when_csv_is_not_reachable_from_cwd(isolated_import, tmp_path):
    """Scenario: Import failure.

    WHEN the relative CSV is inaccessible at module import (e.g. the
    process working directory isn't the repo root, as happens whenever the
    script is launched from elsewhere, such as Airflow's DAG folder)
    THEN the import itself raises, and it does so outside of any
    try/except in main() -- main() never even gets a chance to run.
    """
    os.chdir(tmp_path)

    with pytest.raises(FileNotFoundError):
        isolated_import()

    # The failed import must not leave a half-initialized module behind,
    # and it must not have been swallowed by main()'s handler (which prints
    # "An error occurred during execution" and would still leave `main`
    # importable).
    assert "load_data" not in __import__("sys").modules


def test_import_failure_is_not_caught_by_mains_error_handler(isolated_import, tmp_path, capsys):
    """main()'s try/except (load_data.py:72-79) only wraps calls made from
    inside main() itself. A failure during import can't reach it, so the
    "An error occurred during execution" message must never be printed for
    an import-time failure.
    """
    os.chdir(tmp_path)

    with pytest.raises(FileNotFoundError):
        isolated_import()

    captured = capsys.readouterr()
    assert "An error occurred during execution" not in captured.out
