import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "request-script"))
import new_source_loader as nsl  # noqa: E402

HEADER = "source_key,source_version,event_time,description,quantity,unit_price,discount,shipping_cost\n"


def batch(*rows):
    return (HEADER + "\n".join(rows) + "\n").encode()


VALID = "A1,1,2025-01-02T10:00:00,Mug,3,2.5,0.5,1.0"


def test_valid_row_accepted():
    accepted, rejects = nsl.parse_batch(batch(VALID))
    assert rejects == []
    assert accepted[0]["quantity"] == 3 and accepted[0]["unit_price"] == 2.5


def test_missing_required_and_malformed_rejected_with_reason():
    accepted, rejects = nsl.parse_batch(batch(
        ",1,2025-01-02T10:00:00,Mug,3,2.5,,",
        "A2,1,not-a-date,Mug,3,2.5,,",
        "A3,1,2025-01-02T10:00:00,Mug,x,2.5,,",
        "A4,1,2025-01-02T10:00:00,Mug,3,,,",
    ))
    assert accepted == []
    assert [r[0] for r in rejects] == [1, 2, 3, 4]
    assert "source_key" in rejects[0][1] and "event_time" in rejects[1][1]
    assert "quantity" in rejects[2][1] and "unit_price" in rejects[3][1]


def test_duplicate_in_batch_rejected():
    accepted, rejects = nsl.parse_batch(batch(VALID, VALID))
    assert len(accepted) == 1 and "duplicate" in rejects[0][1]


def test_same_key_new_version_is_distinct():
    accepted, _ = nsl.parse_batch(batch(VALID, VALID.replace("A1,1", "A1,2")))
    assert len(accepted) == 2


def test_empty_batch_is_valid_and_zero_rows():
    assert nsl.parse_batch(batch()) == ([], [])


def test_missing_header_column_fails_batch():
    with pytest.raises(ValueError):
        nsl.parse_batch(b"source_key,quantity\nA,1\n")


def test_timezone_normalised_to_utc():
    accepted, _ = nsl.parse_batch(batch("A1,1,2025-01-02T10:00:00+02:00,Mug,1,1,,"))
    assert accepted[0]["event_time"].hour == 8 and accepted[0]["event_time"].tzinfo is None


def test_batch_id_deterministic_for_replay():
    assert nsl.batch_id_for(b"x") == nsl.batch_id_for(b"x") != nsl.batch_id_for(b"y")


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        if self.conn.fail:
            raise RuntimeError("db down")
        self.conn.statements.append(sql)


class FakeConn:
    def __init__(self, fail=False):
        self.fail, self.statements, self.commits, self.rollbacks = fail, [], 0, 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_load_uses_do_nothing_upsert_and_commits_once():
    conn = FakeConn()
    result = nsl.load_batch(conn, batch(VALID, "B,1,bad,,1,1,,"))
    assert result["accepted"] == 1 and result["rejected"] == 1
    assert any("ON CONFLICT (source_key, source_version) DO NOTHING" in s for s in conn.statements)
    assert conn.commits == 1


def test_load_failure_raises_and_rolls_back():
    conn = FakeConn(fail=True)
    with pytest.raises(RuntimeError):
        nsl.load_batch(conn, batch(VALID))
    assert conn.rollbacks == 1 and conn.commits == 0


def test_main_raises_on_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        nsl.main(str(tmp_path / "nope.csv"))
