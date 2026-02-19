# Airflow Regional RBAC Playbook (Airflow 3.1.6 + FAB)

## 1) 目标与角色模型

本方案实现以下 Airflow 自定义角色：

- `AF_RERUN_ALL_NO_TRIGGER`
  - 允许清理/重跑失败任务（clear/retry）
  - 不允许 trigger 新的 DagRun
- `AF_TRIGGER_SCOPE_US`
  - 允许 trigger `US` 作用域 DAG
- `AF_TRIGGER_SCOPE_NONUS`
  - 允许 trigger 非受限作用域 DAG
- 未来扩展（按需）:
  - `AF_TRIGGER_SCOPE_MX`
  - `AF_TRIGGER_SCOPE_CN`

组合后对应你的 4 类账户：

- normal user: `Viewer` + `AF_RERUN_ALL_NO_TRIGGER`
- US users: `Viewer` + `AF_TRIGGER_SCOPE_US`
- non-US privileged: `Viewer` + `AF_TRIGGER_SCOPE_NONUS`
- US privileged: `Viewer` + `AF_TRIGGER_SCOPE_US` + `AF_TRIGGER_SCOPE_NONUS`

## 2) 可执行清单

在 RHEL VM 的 Airflow 工程目录（例如 `/home/max/development/airflow`）执行：

```bash
cd /home/max/development/airflow
chmod +x rbac/*.sh
```

先 dry-run：

```bash
./rbac/apply_region_rbac.sh \
  --env dev \
  --config /home/max/development/airflow/conf/airflow-manager-dev.conf \
  --restricted-scopes us \
  --dag-scope-file /home/max/development/airflow/rbac/dag_scope_overrides.csv.example \
  --dry-run
```

确认无误后正式执行：

```bash
./rbac/apply_region_rbac.sh \
  --env dev \
  --config /home/max/development/airflow/conf/airflow-manager-dev.conf \
  --restricted-scopes us \
  --dag-scope-file /home/max/development/airflow/rbac/dag_scope_overrides.csv.example
```

创建本地测试用户（可选）：

```bash
./rbac/create_test_users.sh \
  --env dev \
  --config /home/max/development/airflow/conf/airflow-manager-dev.conf \
  --password 'ChangeMe_123!'
```

核验：

```bash
airflow roles list -o plain
airflow roles list -p -o plain | grep -E 'AF_RERUN_ALL_NO_TRIGGER|AF_TRIGGER_SCOPE_'
airflow users list -o plain
```

## 3) DAG 作用域设计（面向未来 MX/CN）

推荐 DAG ID 命名规则：

`<scope>__<domain>__<pipeline>`

示例：

- `us__sales__daily_close`
- `global__finance__etl`
- `mx__ops__reporting`
- `cn__risk__intraday`

脚本规则：

- `--restricted-scopes us` 时：
  - `us__*` -> `AF_TRIGGER_SCOPE_US`
  - 其它 -> `AF_TRIGGER_SCOPE_NONUS`
- 将来需要 MX/CN 限制时，只需重跑：
  - `--restricted-scopes us,mx,cn`
  - 脚本会自动创建并使用 `AF_TRIGGER_SCOPE_MX`、`AF_TRIGGER_SCOPE_CN`
- 脚本每次执行都会先清理受管触发角色里的旧 `DAG:* can_edit`，再按当前规则回填，避免历史权限残留。

若现有 DAG ID 不符合前缀规则，请维护 `dag_scope_overrides.csv`（`dag_id,scope`）。

## 4) LDAP / AD Group 设计

建议先建“能力组”（可复用、可组合）：

- `AD_AF_VIEWER` -> `Viewer`
- `AD_AF_RERUN_ALL_NO_TRIGGER` -> `AF_RERUN_ALL_NO_TRIGGER`
- `AD_AF_TRIGGER_SCOPE_NONUS` -> `AF_TRIGGER_SCOPE_NONUS`
- `AD_AF_TRIGGER_SCOPE_US` -> `AF_TRIGGER_SCOPE_US`
- 预留：
  - `AD_AF_TRIGGER_SCOPE_MX` -> `AF_TRIGGER_SCOPE_MX`
  - `AD_AF_TRIGGER_SCOPE_CN` -> `AF_TRIGGER_SCOPE_CN`

可选再建“画像组”（给运维/HR 更直观）：

- `AD_AF_PROFILE_NORMAL`
- `AD_AF_PROFILE_US_USER`
- `AD_AF_PROFILE_NONUS_PRIV`
- `AD_AF_PROFILE_US_PRIV`

> 画像组可通过 AD 嵌套组实现；若你们 LDAP 查询不展开嵌套组，则直接把用户加到能力组。

Airflow `webserver_config.py` 示例：

```python
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_LDAP_GROUP_FIELD = "memberOf"
AUTH_ROLES_MAPPING = {
    "CN=AD_AF_VIEWER,OU=Groups,DC=corp,DC=example,DC=com": ["Viewer"],
    "CN=AD_AF_RERUN_ALL_NO_TRIGGER,OU=Groups,DC=corp,DC=example,DC=com": ["AF_RERUN_ALL_NO_TRIGGER"],
    "CN=AD_AF_TRIGGER_SCOPE_NONUS,OU=Groups,DC=corp,DC=example,DC=com": ["AF_TRIGGER_SCOPE_NONUS"],
    "CN=AD_AF_TRIGGER_SCOPE_US,OU=Groups,DC=corp,DC=example,DC=com": ["AF_TRIGGER_SCOPE_US"],
}
```

## 5) 重要边界

`AF_RERUN_ALL_NO_TRIGGER` 依赖 `DAGs.can_edit`，这会同时允许 pause/unpause 等 DAG 编辑动作。  
如果你要求“只能 rerun，完全不能 pause/其它编辑”，需要做自定义 Auth Manager 逻辑，标准 RBAC 无法彻底拆开。

## 6) 参考

- [Airflow FAB Access Control](https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/access-control.html)
- [Airflow CLI: roles](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#roles)
- [Flask AppBuilder Security (LDAP)](https://flask-appbuilder.readthedocs.io/en/latest/security.html)
