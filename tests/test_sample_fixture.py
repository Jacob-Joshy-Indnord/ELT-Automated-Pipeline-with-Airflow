import os
import sys
from datetime import date

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "request-script"))
import load_new_source as ns  # noqa: E402


def test_sample_fixture_split():
    recs = ns.read_csv(os.path.join(ROOT, "data", "new_source", "sample_2026-01-10.csv"))
    acc, rej = ns.split_batch(recs, date(2026, 1, 10))
    # qty 0 is accepted at landing (excluded later in staging); missing qty and the duplicate are rejected
    assert len(acc) == 4
    assert sorted(r["row_number"] for r in rej) == [5, 6]
