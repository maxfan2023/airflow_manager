#!/usr/bin/env python3
"""Sync scope trigger roles to DAG Run:<dag_id>.can_create permissions."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Set

from sqlalchemy import select

from airflow.models.dag import DagModel, DagTag
from airflow.providers.fab.auth_manager.models import Role
from airflow.utils.session import create_session

DEFAULT_DAG_TAG_TO_SCOPE = {
    "GDTET_GLOBAL_DAG": "global",
    "GDTET_US_DAG": "us",
}

DAG_RUN_RESOURCE_PREFIX = "DAG Run:"
ACTION_CAN_CREATE = "can_create"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync AF_TRIGGER_SCOPE_<SCOPE> with DAG Run:<dag_id>.can_create "
            "permissions derived from DAG classification tags."
        )
    )
    parser.add_argument(
        "--scopes",
        default="global,us",
        help="Comma-separated scopes to manage (default: global,us).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print changes without applying them.",
    )
    return parser.parse_args()


def normalize_scopes(scopes_csv: str) -> Set[str]:
    scopes: Set[str] = set()
    for item in scopes_csv.split(","):
        scope = item.strip().lower()
        if scope:
            scopes.add(scope)
    scopes.add("global")
    return scopes


def scope_role_name(scope: str) -> str:
    return f"AF_TRIGGER_SCOPE_{scope.upper()}"


def load_tag_scope_map() -> Dict[str, str]:
    raw = os.environ.get("AIRFLOW_RBAC_DAG_TAG_SCOPE_MAP", "").strip()
    if not raw:
        return dict(DEFAULT_DAG_TAG_TO_SCOPE)

    parsed: Dict[str, str] = {}
    for item in raw.split(","):
        entry = item.strip()
        if not entry or "=" not in entry:
            continue
        tag, scope = entry.split("=", 1)
        tag = tag.strip()
        scope = scope.strip().lower()
        if tag and scope:
            parsed[tag] = scope

    return parsed if parsed else dict(DEFAULT_DAG_TAG_TO_SCOPE)


def resolve_dags_root() -> Path:
    env_root = os.environ.get("AIRFLOW_RBAC_DAGS_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    cfg_root = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if cfg_root:
        return Path(cfg_root).expanduser().resolve()

    airflow_home = os.environ.get("AIRFLOW_HOME")
    if airflow_home:
        return (Path(airflow_home).expanduser().resolve() / "dags").resolve()

    return (Path.cwd() / "dags").resolve()


def is_under_root(fileloc: str, dags_root: Path) -> bool:
    try:
        Path(fileloc).resolve().relative_to(dags_root)
    except Exception:
        return False
    return True


def classify_dags_by_scope(scopes: Set[str], tag_to_scope: Dict[str, str], dags_root: Path) -> Dict[str, Set[str]]:
    tags_by_dag: Dict[str, Set[str]] = defaultdict(set)
    dag_fileloc: Dict[str, str] = {}

    with create_session() as session:
        for dag_id, fileloc in session.execute(select(DagModel.dag_id, DagModel.fileloc)):
            if dag_id and fileloc:
                dag_fileloc[dag_id] = fileloc

        for dag_id, tag_name in session.execute(select(DagTag.dag_id, DagTag.name)):
            if dag_id and tag_name:
                tags_by_dag[dag_id].add(tag_name)

    scoped_dags: Dict[str, Set[str]] = {scope: set() for scope in scopes}
    for dag_id, fileloc in dag_fileloc.items():
        if not is_under_root(fileloc, dags_root):
            continue

        tags = tags_by_dag.get(dag_id, set())
        matched_scopes = {tag_to_scope[tag] for tag in tags if tag in tag_to_scope}
        if len(matched_scopes) != 1:
            continue

        scope = next(iter(matched_scopes))
        if scope in scopes:
            scoped_dags[scope].add(dag_id)

    return scoped_dags


def get_existing_scoped_permissions(scopes: Set[str]) -> Dict[str, Set[str]]:
    role_to_scope = {scope_role_name(scope): scope for scope in scopes}
    managed_roles = set(role_to_scope)
    existing: Dict[str, Set[str]] = {scope: set() for scope in scopes}

    with create_session() as session:
        roles = (
            session.execute(select(Role).where(Role.name.in_(managed_roles)))
            .unique()
            .scalars()
            .all()
        )

    for role in roles:
        scope = role_to_scope.get(role.name)
        if not scope:
            continue

        for permission in role.permissions:
            action = getattr(getattr(permission, "action", None), "name", "")
            resource = getattr(getattr(permission, "resource", None), "name", "")
            if action != ACTION_CAN_CREATE:
                continue
            if not resource.startswith(DAG_RUN_RESOURCE_PREFIX):
                continue

            dag_id = resource[len(DAG_RUN_RESOURCE_PREFIX) :]
            if dag_id:
                existing[scope].add(dag_id)

    return existing


def run_airflow_cli(args: Iterable[str], dry_run: bool) -> None:
    cmd = ["airflow", *args]
    if dry_run:
        print(f"[DRY-RUN] {shlex.join(cmd)}")
        return
    subprocess.run(cmd, check=True)


def sync_permissions(
    desired: Dict[str, Set[str]],
    existing: Dict[str, Set[str]],
    dry_run: bool,
) -> tuple[int, int]:
    added = 0
    removed = 0

    for scope in sorted(desired):
        role = scope_role_name(scope)
        target_dags = desired.get(scope, set())
        current_dags = existing.get(scope, set())

        for dag_id in sorted(target_dags - current_dags):
            run_airflow_cli(
                ["roles", "add-perms", role, "-a", ACTION_CAN_CREATE, "-r", f"{DAG_RUN_RESOURCE_PREFIX}{dag_id}"],
                dry_run=dry_run,
            )
            added += 1

        for dag_id in sorted(current_dags - target_dags):
            run_airflow_cli(
                ["roles", "del-perms", role, "-a", ACTION_CAN_CREATE, "-r", f"{DAG_RUN_RESOURCE_PREFIX}{dag_id}"],
                dry_run=dry_run,
            )
            removed += 1

    return added, removed


def main() -> int:
    args = parse_args()
    scopes = normalize_scopes(args.scopes)
    tag_to_scope = load_tag_scope_map()
    dags_root = resolve_dags_root()

    desired = classify_dags_by_scope(scopes=scopes, tag_to_scope=tag_to_scope, dags_root=dags_root)
    existing = get_existing_scoped_permissions(scopes=scopes)
    added, removed = sync_permissions(desired=desired, existing=existing, dry_run=args.dry_run)

    print(
        "Scoped DAG Run permission sync complete: "
        f"scopes={sorted(scopes)}, dags_root={dags_root}, "
        f"add={added}, remove={removed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
