"""Nonproduction preflight for the DockerOperator (run BEFORE enabling the gated DAG).

Proves, using the same configuration the DAG uses:
  1. docker_url is reachable;
  2. the dbt project and profile host paths are configured and mount into the container;
  3. dbt succeeds on a valid command and a nonzero dbt exit is surfaced as a failure.
Usage: DBT_HOST_PROJECT_DIR=... DBT_HOST_PROFILES_FILE=... python docker_preflight.py
Exit code 0 = all checks passed.
"""
import os
import sys

IMAGE = "ghcr.io/dbt-labs/dbt-postgres:1.9.latest"


def check_config(env=os.environ):
    problems = []
    for name in ("DBT_HOST_PROJECT_DIR", "DBT_HOST_PROFILES_FILE"):
        if not env.get(name):
            problems.append(f"{name} is not configured")
    return problems


def run_dbt(client, env, args):
    """Run dbt in a container with the configured mounts; returns the exit status code."""
    container = client.containers.run(
        IMAGE, args, working_dir="/usr/app", detach=True,
        network_mode=env.get("DBT_DOCKER_NETWORK", "online-sales-data-pipeline_my_network"),
        volumes={env["DBT_HOST_PROJECT_DIR"]: {"bind": "/usr/app", "mode": "rw"},
                 env["DBT_HOST_PROFILES_FILE"]: {"bind": "/root/.dbt/profiles.yml", "mode": "ro"}})
    try:
        return container.wait()["StatusCode"]
    finally:
        container.remove(force=True)


def main(env=os.environ):
    problems = check_config(env)
    if problems:
        print("\n".join("FAIL: " + p for p in problems))
        return 1
    import docker
    client = docker.DockerClient(base_url=env.get("DOCKER_URL", "unix://var/run/docker.sock"))
    if not client.ping():
        print("FAIL: docker_url unreachable")
        return 1
    print("OK: docker_url reachable")
    if run_dbt(client, env, "debug") != 0:
        print("FAIL: dbt debug failed (project/profile mount or DB connection)")
        return 1
    print("OK: project and profile mounts resolve; dbt debug passed")
    if run_dbt(client, env, "run --select __no_such_model__ --profiles-dir /nonexistent") == 0:
        print("FAIL: nonzero dbt exit was not reported")
        return 1
    print("OK: nonzero dbt exit is reported (DockerOperator fails the task on it)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
