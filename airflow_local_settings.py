"""Tag-based DAG scope policy for Airflow FAB RBAC.

Place this file at AIRFLOW_HOME/airflow_local_settings.py.
The scheduler imports dag_policy() for every DAG and injects DAG-level ACLs.

Tag convention (default):
  GDTET_US_DAG      -> AF_TRIGGER_SCOPE_US
  GDTET_GLOBAL_DAG  -> AF_TRIGGER_SCOPE_GLOBAL

Future extension:
  Add more entries to DEFAULT_DAG_TAG_TO_SCOPE
  or configure AIRFLOW_RBAC_DAG_TAG_SCOPE_MAP, e.g.
  "GDTET_US_DAG=us,GDTET_GLOBAL_DAG=global,GDTET_MX_DAG=mx"
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
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

DEFAULT_DAG_TAG_TO_SCOPE = {
    "GDTET_GLOBAL_DAG": "global",
    "GDTET_US_DAG": "us",
}


def _load_dag_tag_to_scope() -> Dict[str, str]:
    """Load the classification-tag mapping from env var with safe fallback.

    Expected format:
      AIRFLOW_RBAC_DAG_TAG_SCOPE_MAP="TAG_A=scope_a,TAG_B=scope_b"
    """
    raw = os.environ.get("AIRFLOW_RBAC_DAG_TAG_SCOPE_MAP", "").strip()
    if not raw:
        return dict(DEFAULT_DAG_TAG_TO_SCOPE)

    parsed: Dict[str, str] = {}
    for entry in raw.split(","):
        item = entry.strip()
        if not item:
            continue
        if "=" not in item:
            LOG.warning(
                "Ignore invalid AIRFLOW_RBAC_DAG_TAG_SCOPE_MAP item (missing '='): %s",
                item,
            )
            continue
        tag, scope = item.split("=", 1)
        normalized_tag = tag.strip()
        normalized_scope = scope.strip().lower()
        if not normalized_tag or not normalized_scope:
            LOG.warning(
                "Ignore invalid AIRFLOW_RBAC_DAG_TAG_SCOPE_MAP item (empty tag/scope): %s",
                item,
            )
            continue
        parsed[normalized_tag] = normalized_scope

    if not parsed:
        LOG.warning(
            "AIRFLOW_RBAC_DAG_TAG_SCOPE_MAP is set but no valid mapping parsed; "
            "fallback to defaults."
        )
        return dict(DEFAULT_DAG_TAG_TO_SCOPE)

    return parsed


DAG_TAG_TO_SCOPE = _load_dag_tag_to_scope()
MANAGED_TRIGGER_ROLES = set(SCOPE_TO_TRIGGER_ROLE.values())
RESOURCE_DAG = "DAGs"
RESOURCE_DAG_RUN = "DAG Runs"
ACTION_CAN_EDIT = "can_edit"
ACTION_CAN_CREATE = "can_create"

# Keep one scope role per DAG with:
# - DAGs.can_edit (required by trigger endpoint DAG-level PUT check)
# - DAG Runs.can_create (required by trigger endpoint RUN check)
#
# Airflow's sync_perm_for_dag periodically reconciles DAG ACLs and revokes stale
# DAG-level permissions. If DAG Runs.can_create is not present in dag.access_control,
# per-DAG trigger grants can disappear within seconds.
TRIGGER_DAG_RESOURCE_ACTIONS = {
    RESOURCE_DAG: {ACTION_CAN_EDIT},
    RESOURCE_DAG_RUN: {ACTION_CAN_CREATE},
}


def _resolve_dags_root() -> Path:
    """Resolve the managed DAG root used by this policy.

    Only DAGs under this root will be managed by tag-based RBAC. This avoids
    changing ACLs of non-project DAGs (for example bundled sample DAGs).
    """
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


def _normalize_permissions(perms: object) -> Set[str]:
    """Normalize ACL action values into a set of strings.

    Airflow/FAB ACL values can appear as string/list/set/tuple depending on
    caller style and version. This helper keeps downstream logic simple.
    """
    if perms is None:
        return set()
    if isinstance(perms, str):
        return {perms}
    if isinstance(perms, Iterable):
        return {str(p) for p in perms}
    return set()


def _is_dag_under_managed_root(fileloc: str) -> bool:
    """Return True when DAG file location belongs to the managed DAG root."""
    dags_root = _resolve_dags_root()
    dag_file = Path(fileloc).resolve()

    try:
        dag_file.relative_to(dags_root)
    except ValueError:
        # Not under configured dags root (for example packaged example DAGs).
        return False
    return True


def _extract_dag_tags(dag) -> Set[str]:
    """Extract and normalize DAG tags from different runtime representations."""
    raw_tags = getattr(dag, "tags", None)
    if raw_tags is None:
        return set()
    if isinstance(raw_tags, str):
        normalized = raw_tags.strip()
        return {normalized} if normalized else set()
    if isinstance(raw_tags, Iterable):
        tags: Set[str] = set()
        for tag in raw_tags:
            normalized = str(tag).strip()
            if normalized:
                tags.add(normalized)
        return tags
    return set()


def _scope_from_tags(dag) -> str:
    """Resolve one and only one scope from DAG tags.

    Rules:
    - exactly one classification scope matched -> valid
    - multiple classification scopes matched   -> invalid (conflict)
    - zero classification scopes matched       -> invalid (missing tag)
    """
    tags = _extract_dag_tags(dag)

    matched_scopes: Set[str] = set()
    matched_tags: Set[str] = set()
    for tag, scope in DAG_TAG_TO_SCOPE.items():
        if tag in tags:
            matched_tags.add(tag)
            matched_scopes.add(scope)

    if len(matched_scopes) == 1:
        return next(iter(matched_scopes))

    if len(matched_scopes) > 1:
        raise AirflowClusterPolicyViolation(
            "DAG "
            f"'{getattr(dag, 'dag_id', '<unknown>')}' has conflicting classification tags "
            f"{sorted(matched_tags)}; expected exactly one classification tag."
        )

    raise AirflowClusterPolicyViolation(
        "DAG "
        f"'{getattr(dag, 'dag_id', '<unknown>')}' is missing required classification tag. "
        f"Expected one of {sorted(DAG_TAG_TO_SCOPE.keys())}, got tags={sorted(tags)}"
    )


def _normalize_resource_action_map(perms: Any) -> Dict[str, Set[str]]:
    # Support both old-style ACL sets and new-style resource->actions dict.
    # Old style (set/list of actions) is interpreted as DAG resource actions.
    if isinstance(perms, dict):
        normalized: Dict[str, Set[str]] = {}
        for resource_name, actions in perms.items():
            resource = str(resource_name).strip()
            if not resource:
                continue
            normalized_actions = _normalize_permissions(actions)
            if normalized_actions:
                normalized[resource] = normalized_actions
        return normalized

    normalized_actions = _normalize_permissions(perms)
    if not normalized_actions:
        return {}
    return {RESOURCE_DAG: normalized_actions}


def _merge_managed_acl(existing_acl: object, target_role: str) -> Dict[str, Dict[str, Set[str]]]:
    """Merge existing ACL while enforcing exactly one managed trigger role.

    Important behavior:
    - Keep non-managed roles untouched to avoid breaking business ACLs.
    - Replace all managed scope roles with exactly one target role derived
      from classification tag.
    - Add both DAG and DAG Runs resource actions for target role so Airflow's
      periodic permission reconciliation will not revoke trigger grants.
    """
    merged: Dict[str, Dict[str, Set[str]]] = {}

    if isinstance(existing_acl, dict):
        for role_name, perms in existing_acl.items():
            role = str(role_name)
            if role in MANAGED_TRIGGER_ROLES:
                continue
            normalized_perms = _normalize_resource_action_map(perms)
            if normalized_perms:
                merged[role] = normalized_perms

    # Exactly one managed trigger role per DAG to avoid accidental cross-scope trigger.
    merged[target_role] = {
        resource_name: set(actions)
        for resource_name, actions in TRIGGER_DAG_RESOURCE_ACTIONS.items()
    }
    return merged


def dag_policy(dag) -> None:
    """Airflow cluster policy hook executed for every parsed DAG.

    This function translates classification tags into deterministic DAG ACLs.
    Any DAG that is missing/invalid tags raises AirflowClusterPolicyViolation,
    which keeps it out of normal DAG listing/trigger flow.
    """

    if not _is_dag_under_managed_root(dag.fileloc):
        return

    scope = _scope_from_tags(dag)
    target_role = SCOPE_TO_TRIGGER_ROLE.get(scope)
    if not target_role:
        raise AirflowClusterPolicyViolation(
            f"No trigger role mapping configured for scope '{scope}'"
        )

    dag.access_control = _merge_managed_acl(getattr(dag, "access_control", None), target_role)
