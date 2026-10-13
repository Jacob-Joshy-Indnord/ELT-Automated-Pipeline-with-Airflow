"""Promotion gate: refuses production promotion until the source contract is approved.

Usage: python release_gate.py [path/to/new_source_contract.json]   (exit 1 = blocked)
"""
import json
import os
import sys

DEFAULT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "new_source_contract.json")


def load_contract(path=DEFAULT_PATH):
    with open(path) as f:
        return json.load(f)


def gate_problems(contract):
    """Return the list of reasons promotion is blocked (empty list = may promote)."""
    problems = []
    approval = contract.get("approval", {})
    if contract.get("status") != "approved":
        problems.append("contract status is not 'approved'")
    if approval.get("status") != "approved":
        problems.append("source owner approval status is not 'approved'")
    if not approval.get("source_owner"):
        problems.append("source owner not named")
    if not approval.get("approved_on"):
        problems.append("approval date missing")
    for item, ok in approval.get("approved_items", {}).items():
        if not ok:
            problems.append(f"contract item not approved: {item}")
    if not approval.get("implementation_reconciled"):
        problems.append("implementation and tests not reconciled with approved contract")
    if not approval.get("sdlc_approvers"):
        problems.append("no named SDLC approvers recorded")
    return problems


def main(argv):
    problems = gate_problems(load_contract(*argv[1:2]))
    for p in problems:
        print(f"BLOCKED: {p}")
    if not problems:
        print("Promotion gate passed")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
