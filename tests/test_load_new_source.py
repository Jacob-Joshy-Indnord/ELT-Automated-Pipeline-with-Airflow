import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "request-script"))
import load_new_source as ns  # noqa: E402

RUN = date(2026, 1, 10)


def rec(key="A1", version="1", **kw):
    r = {"source_key": key, "version": version, "event_time": "2026-01-10 08:00", "quantity": "2", "unit_price": "3.5"}
    r.update(kw)
    return r


class FakeCursor:
    """Emulates the landing tables' key semantics so replay behaviour can be asserted."""
    def __init__(self, db):
        self.db = db

    def execute(self, sql, params=None):
        if self.db.fail_on and self.db.fail_on in sql:
            raise RuntimeError("db down")
        if sql.lstrip().startswith("INSERT INTO landing_new.new_sales_source"):
            self.db.accepted[(params[0], params[1])] = params
        elif sql.lstrip().startswith("DELETE FROM landing_new.new_sales_rejects"):
            self.db.rejects = {k: v for k, v in self.db.rejects.items() if k[0] != params[0]}
        elif sql.lstrip().startswith("INSERT INTO landing_new.new_sales_rejects"):
            self.db.rejects[(params[0], params[1])] = params
        elif sql.lstrip().startswith("INSERT INTO landing_new.load_batches"):
            self.db.batches[params[0]] = params


class FakeConn:
    def __init__(self, fail_on=None):
        self.accepted, self.rejects, self.batches = {}, {}, {}
        self.fail_on, self.rolled_back = fail_on, False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

    def rollback(self):
        self.rolled_back = True


def test_valid_row_accepted():
    acc, rej = ns.split_batch([rec()], RUN)
    assert len(acc) == 1 and not rej and acc[0]["quantity"] == 2 and acc[0]["unit_price"] == 3.5


@pytest.mark.parametrize("bad,reason", [
    (rec(source_key=""), "missing required"),
    (rec(quantity=""), "missing required"),
    (rec(unit_price="abc"), "malformed number"),
    (rec(quantity="1.5"), "malformed number"),
    (rec(event_time="yesterday"), "malformed event_time"),
    (rec(event_time="2026-01-05 00:00"), "late event"),
    (rec(event_time="2026-02-01 00:00"), "after run date"),
    (rec(discount="x"), "malformed number in discount"),
])
def test_invalid_rows_rejected_with_reason(bad, reason):
    acc, rej = ns.split_batch([bad], RUN)
    assert not acc and reason in rej[0]["reject_reason"]


def test_duplicate_in_batch_rejected_and_traceable():
    acc, rej = ns.split_batch([rec(), rec()], RUN)
    assert len(acc) == 1 and rej[0]["row_number"] == 2 and "duplicate" in rej[0]["reject_reason"]


def test_replay_same_batch_twice_does_not_duplicate():
    conn = FakeConn()
    records = [rec("A1"), rec("A2"), rec("", "1"), rec("A1")]
    first = ns.load_batch(conn, records, RUN)
    second = ns.load_batch(conn, records, RUN)
    assert first == second and first["accepted"] == 2 and first["rejected"] == 2
    assert len(conn.accepted) == 2 and len(conn.rejects) == 2
    assert all(k[0] == "new-source-2026-01-10" for k in conn.rejects)
    assert len(conn.batches) == 1


def test_new_version_is_new_key_version():
    conn = FakeConn()
    ns.load_batch(conn, [rec("A1", "1"), rec("A1", "2")], RUN)
    assert set(conn.accepted) == {("A1", 1), ("A1", 2)}


def test_load_failure_is_raised_and_rolled_back():
    conn = FakeConn(fail_on="INSERT INTO landing_new.new_sales_source")
    with pytest.raises(RuntimeError):
        ns.load_batch(conn, [rec()], RUN)
    assert conn.rolled_back


def test_missing_file_fails_run(tmp_path):
    with pytest.raises(FileNotFoundError):
        ns.run(RUN, base_dir=str(tmp_path))


def test_empty_batch_recorded_as_zero_rows():
    conn = FakeConn()
    out = ns.load_batch(conn, [], RUN)
    assert out["received"] == 0 and conn.batches["new-source-2026-01-10"][2] == 0
