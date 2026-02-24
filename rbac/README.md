# Airflow Folder-Based RBAC Playbook (Airflow 3.1.6 + FAB)

## 1) 目标

用固定 custom roles + DAG 文件夹策略实现触发权限隔离：

- `dags/us/*` 只能由 `AF_TRIGGER_SCOPE_US` trigger
- `dags/global/*` 只能由 `AF_TRIGGER_SCOPE_GLOBAL` trigger
- 未来 `dags/mx/*`、`dags/cn/*` 直接扩展

不再依赖“扫描现有 DAG 再逐条授权”，避免每次 DAG 变更都手工维护权限。

## 2) 角色模型

- `AF_RERUN_ALL_NO_TRIGGER`
  - 用于 rerun 失败任务（clear/retry）
  - 不含 `DAG Runs.can_create`
- `AF_TRIGGER_SCOPE_GLOBAL`
- `AF_TRIGGER_SCOPE_US`
- 预留：`AF_TRIGGER_SCOPE_MX`、`AF_TRIGGER_SCOPE_CN`

用户组合：

- normal user: `Viewer` + `AF_RERUN_ALL_NO_TRIGGER`
- US users: `Viewer` + `AF_TRIGGER_SCOPE_US`
- non-US privileged: `Viewer` + `AF_TRIGGER_SCOPE_GLOBAL`
- US privileged: `Viewer` + `AF_TRIGGER_SCOPE_US` + `AF_TRIGGER_SCOPE_GLOBAL`

## 3) 核心实现

- `rbac/apply_region_rbac.sh`
  - 只初始化角色和基础权限
  - 不再写 `DAG:<dag_id>` 授权
- `airflow_local_settings.py`
  - 在 `dag_policy` 中按 DAG 文件路径自动设置 `dag.access_control`
  - 每个 DAG 只保留一个受管 trigger role（global/us/mx/cn）

## 4) 部署步骤

在 Airflow 工程目录执行：

```bash
cd /home/max/development/airflow
chmod +x rbac/*.sh
```

### Step A: 初始化角色

```bash
./rbac/apply_region_rbac.sh \
  --env dev \
  --config /home/max/development/airflow/conf/airflow-manager-dev.conf \
  --scopes global,us,mx,cn
```

### Step B: 安装 policy 文件

确保 `AIRFLOW_HOME/airflow_local_settings.py` 存在并使用本仓库版本。  
如果你当前就是在 `AIRFLOW_HOME` 目录维护代码，不需要复制。  
如果不是，请执行类似：

```bash
cp /path/to/this-repo/airflow_local_settings.py /home/max/development/airflow/airflow_local_settings.py
```

如果你的启动环境没有把 `AIRFLOW_HOME` 放进 `PYTHONPATH`，请在 env 配置增加：

```bash
export PYTHONPATH="${AIRFLOW_HOME}:${PYTHONPATH:-}"
```

本仓库最新 `airflow-manager.sh` / `celery-worker-manager.sh` / `rbac/*.sh` 已在加载 config 后自动补齐该变量；  
如果你线上仍在用旧脚本，请手工加上这行，避免 DAG policy 不生效。

### Step C: 调整 DAG 目录

```text
dags/
  us/
  global/
  mx/
  cn/
```

`global` 是标准拼写。为了兼容旧目录，policy 也接受 `globle` 并按 `global` 处理。

### Step D: 重启并同步权限

```bash
./airflow-manager.sh dev restart all-services
airflow sync-perm --include-dags
```

## 5) 本地测试用户（可选）

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

## 6) LDAP / AD Group 建议

能力组：

- `AD_AF_VIEWER` -> `Viewer`
- `AD_AF_RERUN_ALL_NO_TRIGGER` -> `AF_RERUN_ALL_NO_TRIGGER`
- `AD_AF_TRIGGER_SCOPE_GLOBAL` -> `AF_TRIGGER_SCOPE_GLOBAL`
- `AD_AF_TRIGGER_SCOPE_US` -> `AF_TRIGGER_SCOPE_US`
- `AD_AF_TRIGGER_SCOPE_MX` -> `AF_TRIGGER_SCOPE_MX`
- `AD_AF_TRIGGER_SCOPE_CN` -> `AF_TRIGGER_SCOPE_CN`

示例 `AUTH_ROLES_MAPPING`：

```python
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_LDAP_GROUP_FIELD = "memberOf"
AUTH_ROLES_MAPPING = {
    "CN=AD_AF_VIEWER,OU=Groups,DC=corp,DC=example,DC=com": ["Viewer"],
    "CN=AD_AF_RERUN_ALL_NO_TRIGGER,OU=Groups,DC=corp,DC=example,DC=com": ["AF_RERUN_ALL_NO_TRIGGER"],
    "CN=AD_AF_TRIGGER_SCOPE_GLOBAL,OU=Groups,DC=corp,DC=example,DC=com": ["AF_TRIGGER_SCOPE_GLOBAL"],
    "CN=AD_AF_TRIGGER_SCOPE_US,OU=Groups,DC=corp,DC=example,DC=com": ["AF_TRIGGER_SCOPE_US"],
}
```

## 7) 重要边界

`AF_RERUN_ALL_NO_TRIGGER` 依赖 `DAGs.can_edit`，会附带 pause/unpause 等编辑能力。
如果要“只能 rerun、不能 pause”，需要自定义 Auth Manager 逻辑。
