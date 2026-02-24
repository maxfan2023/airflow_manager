"""Folder-based DAG scope policy for Airflow FAB RBAC.

Place this file at AIRFLOW_HOME/airflow_local_settings.py.
The scheduler imports dag_policy() for every DAG and injects DAG-level ACLs.

Folder convention:
  dags/us/...      -> AF_TRIGGER_SCOPE_US
  dags/global/...  -> AF_TRIGGER_SCOPE_GLOBAL
  dags/mx/...      -> AF_TRIGGER_SCOPE_MX
  dags/cn/...      -> AF_TRIGGER_SCOPE_CN

Compatibility alias:
  dags/globle/...  -> treated as global
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Set

from airflow.exceptions import AirflowClusterPolicyViolation

LOG = logging.getLogger(__name__)

# Scope -> trigger role mapping. Extend this dict to support new regions.
SCOPE_TO_TRIGGER_ROLE = {
    "global": "AF_TRIGGER_SCOPE_GLOBAL",
    "us": "AF_TRIGGER_SCOPE_US",
    "mx": "AF_TRIGGER_SCOPE_MX",
    "cn": "AF_TRIGGER_SCOPE_CN",
}

# Optional folder aliases. Keep "globle" for compatibility with existing naming.
SCOPE_ALIASES = {
    "global": "global",
    "globle": "global",
    "us": "us",
    "mx": "mx",
    "cn": "cn",
}

MANAGED_TRIGGER_ROLES = set(SCOPE_TO_TRIGGER_ROLE.values())
TRIGGER_DAG_PERMISSIONS = {"can_edit"}


def _truthy(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_dags_root() -> Path:
    # Priority:
    # 1) AIRFLOW_RBAC_DAGS_ROOT
    # 2) AIRFLOW__CORE__DAGS_FOLDER
    # 3) <this_file_dir>/dags
    env_root = os.environ.get("AIRFLOW_RBAC_DAGS_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    cfg_root = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if cfg_root:
        return Path(cfg_root).expanduser().resolve()

    return (Path(__file__).resolve().parent / "dags").resolve()


def _default_scope() -> str:
    # Unknown/unclassified DAGs fall back to this scope when strict mode is off.
    configured = os.environ.get("AIRFLOW_RBAC_DEFAULT_SCOPE", "global").strip().lower()
    return SCOPE_ALIASES.get(configured, configured)


def _strict_scope_enforcement() -> bool:
    return _truthy(os.environ.get("AIRFLOW_RBAC_SCOPE_STRICT"), default=False)


def _normalize_permissions(perms: object) -> Set[str]:
    if perms is None:
        return set()
    if isinstance(perms, str):
        return {perms}
    if isinstance(perms, Iterable):
        return {str(p) for p in perms}
    return set()


def _scope_from_file(fileloc: str) -> Optional[str]:
    dags_root = _resolve_dags_root()
    dag_file = Path(fileloc).resolve()

    try:
        relative = dag_file.relative_to(dags_root)
    except ValueError:
        # Not under configured dags root (for example packaged example DAGs).
        # Leave access_control unchanged.
        return None

    strict = _strict_scope_enforcement()
    default_scope = _default_scope()

    # Expect folder layout: dags/<scope>/<dag_file.py>
    if len(relative.parts) < 2:
        if strict:
            raise AirflowClusterPolicyViolation(
                f"DAG must be in scope folder under {dags_root}; got {dag_file}"
            )
        LOG.warning(
            "DAG file %s is not in dags/<scope>/... layout; fallback scope=%s",
            dag_file,
            default_scope,
        )
        return default_scope

    raw_scope = relative.parts[0].strip().lower()
    scope = SCOPE_ALIASES.get(raw_scope, raw_scope)

    if scope in SCOPE_TO_TRIGGER_ROLE:
        return scope

    if strict:
        raise AirflowClusterPolicyViolation(
            f"Unsupported DAG scope folder '{raw_scope}' for DAG file {dag_file}"
        )

    LOG.warning(
        "Unsupported DAG scope folder '%s' for %s; fallback scope=%s",
        raw_scope,
        dag_file,
        default_scope,
    )
    return default_scope


def _merge_managed_acl(existing_acl: object, target_role: str) -> Dict[str, Set[str]]:
    merged: Dict[str, Set[str]] = {}

    if isinstance(existing_acl, dict):
        for role_name, perms in existing_acl.items():
            if role_name in MANAGED_TRIGGER_ROLES:
                continue
            merged[str(role_name)] = _normalize_permissions(perms)

    # Exactly one managed trigger role per DAG to avoid accidental cross-scope trigger.
    merged[target_role] = set(TRIGGER_DAG_PERMISSIONS)
    return merged


def dag_policy(dag) -> None:
    """Inject DAG-level ACL from folder scope for trigger role control."""

    scope = _scope_from_file(dag.fileloc)
    if scope is None:
        return

    target_role = SCOPE_TO_TRIGGER_ROLE.get(scope)
    if not target_role:
        raise AirflowClusterPolicyViolation(
            f"No trigger role mapping configured for scope '{scope}'"
        )

    dag.access_control = _merge_managed_acl(getattr(dag, "access_control", None), target_role)
