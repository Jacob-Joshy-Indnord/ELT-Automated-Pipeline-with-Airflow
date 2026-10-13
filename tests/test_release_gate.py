import copy
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "request-script"))
import release_gate  # noqa: E402


def approved():
    c = copy.deepcopy(release_gate.load_contract())
    c["status"] = "approved"
    a = c["approval"]
    a.update(status="approved", source_owner="Owner", approved_on="2026-11-01",
             implementation_reconciled=True, sdlc_approvers=["Reviewer"])
    a["approved_items"] = {k: True for k in a["approved_items"]}
    return c


def test_provisional_contract_blocks_promotion():
    assert release_gate.gate_problems(release_gate.load_contract())


def test_fully_approved_contract_passes():
    assert release_gate.gate_problems(approved()) == []


def test_single_unapproved_item_blocks():
    c = approved()
    c["approval"]["approved_items"]["currency"] = False
    assert any("currency" in p for p in release_gate.gate_problems(c))


def test_unreconciled_blocks():
    c = approved()
    c["approval"]["implementation_reconciled"] = False
    assert release_gate.gate_problems(c)
