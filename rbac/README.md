# Airflow Tag-Based RBAC Playbook (Airflow 3.1.6 + FAB)

## 1) 目标

使用固定 custom roles + DAG `tags` 策略实现触发权限隔离，不再依赖 DAG 文件夹位置。

分类规则（作用于 `dags` 根目录及其子目录中的 DAG）：

- 包含 `GDTET_US_DAG` -> 该 DAG 归类为 `us_dag`，仅 `AF_TRIGGER_SCOPE_US` 可触发
- 包含 `GDTET_GLOBAL_DAG` -> 该 DAG 归类为 `global_dag`，仅 `AF_TRIGGER_SCOPE_GLOBAL` 可触发
- 同时包含多个分类 tag（例如同时有 US+GLOBAL）-> 非法 DAG
- 两个分类 tag 都不包含 -> 非法 DAG

非法 DAG 会在解析阶段触发 `AirflowClusterPolicyViolation`，因此不能触发，也不会在 Web UI 的 DAG 列表中展示。

## 2) 角色模型

- `AF_RERUN_ALL_NO_TRIGGER`
  - 用于 rerun 失败任务（clear/retry）
  - 不含 `DAG Runs.can_create`
- `AF_TRIGGER_SCOPE_GLOBAL`
- `AF_TRIGGER_SCOPE_US`
- 预留：`AF_TRIGGER_SCOPE_MX`、`AF_TRIGGER_SCOPE_CN`
- `rbac/sync_scope_dag_run_perms.py`
  - 基于 DAG 分类 tag 同步 `DAG Run:<dag_id>.can_create` 到 `AF_TRIGGER_SCOPE_*`
  - 作为 `sync-perm` 之后的最后一步执行

用户组合：

- normal user: `Viewer` + `AF_RERUN_ALL_NO_TRIGGER`
- US users: `Viewer` + `AF_RERUN_ALL_NO_TRIGGER` + `AF_TRIGGER_SCOPE_US`
- non-US privileged: `Viewer` + `AF_RERUN_ALL_NO_TRIGGER` + `AF_TRIGGER_SCOPE_GLOBAL`
- US privileged: `Viewer` + `AF_RERUN_ALL_NO_TRIGGER` + `AF_TRIGGER_SCOPE_US` + `AF_TRIGGER_SCOPE_GLOBAL`

注意：不要手工给 `AF_TRIGGER_SCOPE_*` 增加全局 `DAG Runs.can_create`，否则会绕过 scope trigger 隔离。

## 3) 核心实现

- `rbac/apply_region_rbac.sh`
  - 初始化角色和基础权限
  - 清理旧的全局 `DAG Runs.can_create`（防止 scope 触发隔离被绕过）
  - 最后一步按 tag 同步 `DAG Run:<dag_id>.can_create`
  - 不再写 `DAG:<dag_id>` 授权
- `airflow_local_settings.py`
  - 在 `dag_policy` 中基于 DAG `tags` 自动设置 `dag.access_control`
  - 每个 DAG 只保留一个受管 trigger role
  - 自动注入：
    - `DAG:<dag_id>.can_edit`
    - `DAG Run:<dag_id>.can_create`（通过 `DAG Runs: can_create` 资源映射）
  - 默认分类 tag 映射：
    - `GDTET_US_DAG=us`
    - `GDTET_GLOBAL_DAG=global`
  - 未来扩展可用环境变量 `AIRFLOW_RBAC_DAG_TAG_SCOPE_MAP`，例如：
    - `GDTET_US_DAG=us,GDTET_GLOBAL_DAG=global,GDTET_MX_DAG=mx`

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

### Step C: 给 DAG 打分类标签

每个 DAG 必须且只能命中一个分类 tag（默认 US/GLOBAL 二选一）：

```python
with DAG(
    ...,
    tags=["your-business-tag", "GDTET_GLOBAL_DAG"],
):
    ...
```

或：

```python
with DAG(
    ...,
    tags=["another-tag", "GDTET_US_DAG"],
):
    ...
```

### Step D: 重启并同步权限

```bash
./airflow-manager.sh dev restart all-services
airflow sync-perm --include-dags
python rbac/sync_scope_dag_run_perms.py --scopes global,us,mx,cn
```

说明：`sync-perm` 可能清理 `DAG Run:<dag_id>.can_create`，因此需要紧跟一次 `sync_scope_dag_run_perms.py`。
如果你使用了本仓库最新 `airflow_local_settings.py`（已在 `dag_policy` 中声明 `DAG Runs: can_create`），
调度器的 DAG 权限同步会自动保持按 DAG 的 trigger 授权，不会在几秒后被回收。

## 5) 生成 5 个测试 DAG（你要求的场景）

```bash
./rbac/generate_tag_rbac_test_dags.sh
```

默认基础目录：`${AIRFLOW_HOME}/dags`（若未设置 `AIRFLOW_HOME`，则使用仓库的 `dags/`）

会生成 5 个文件：

1. `camp_us/rbac_tag_test_1_camp_us.py`：`source="camp-us"` -> `GDTET_US_DAG` -> `us_dag`
2. `mdm/rbac_tag_test_2_mdm.py`：`source="mdm"` -> `GDTET_US_DAG` -> `us_dag`
3. `global/rbac_tag_test_3_abc.py`：`source="abc"` -> `GDTET_GLOBAL_DAG` -> `global_dag`
4. `global/rbac_tag_test_4_hub.py`：`source="hub"` -> `GDTET_GLOBAL_DAG` -> `global_dag`
5. `rbac_tag_test_5_no_source.py`：无 `source` 变量 -> 不打分类 tag -> 非法 DAG（不可触发、UI 不展示）

说明：脚本中 `source` 到 tag 的规则为：

- `camp-us` / `ucm` / `norkom` / `mdm` -> `GDTET_US_DAG`
- 其他 `source` -> `GDTET_GLOBAL_DAG`
- 无 `source` 变量 -> 不打 tag

## 6) 本地测试用户（可选）

```bash
./rbac/create_test_users.sh \
  --env dev \
  --config /home/max/development/airflow/conf/airflow-manager-dev.conf \
  --password-file /home/max/development/airflow/.secrets/rbac_test_user_password.txt
```

如需启用严格隔离模式（触发用户不带跨 DAG rerun）：

```bash
./rbac/create_test_users.sh \
  --env dev \
  --config /home/max/development/airflow/conf/airflow-manager-dev.conf \
  --password-file /home/max/development/airflow/.secrets/rbac_test_user_password.txt \
  --strict-trigger-isolation
```

核验：

```bash
airflow roles list -o plain
airflow roles list -p -o plain | grep -E 'AF_RERUN_ALL_NO_TRIGGER|AF_TRIGGER_SCOPE_'
airflow users list -o plain
```

## 7) 回滚（删除 custom roles + 测试用户）

默认删除以下对象（存在才删）：

- users: `rbac_normal`、`rbac_us_user`、`rbac_nonus_priv`、`rbac_us_priv`
- roles: `AF_RERUN_ALL_NO_TRIGGER`、`AF_TRIGGER_SCOPE_GLOBAL`、`AF_TRIGGER_SCOPE_NONUS`（兼容旧命名）、`AF_TRIGGER_SCOPE_US`、`AF_TRIGGER_SCOPE_MX`、`AF_TRIGGER_SCOPE_CN`

```bash
./rbac/teardown_region_rbac.sh \
  --env dev \
  --config /home/max/development/airflow/conf/airflow-manager-dev.conf
```

如果你还想同时禁用策略文件（把 `airflow_local_settings.py` 改名为 `.bak.<timestamp>`）：

```bash
./rbac/teardown_region_rbac.sh \
  --env dev \
  --config /home/max/development/airflow/conf/airflow-manager-dev.conf \
  --disable-policy
```

## 8) LDAP / AD Group 建议

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

## 9) 重要边界

`AF_RERUN_ALL_NO_TRIGGER` 依赖 `DAGs.can_edit`，会附带 pause/unpause 等编辑能力。  
通过 `DAG Run:<dag_id>.can_create` 的按 DAG 授权，`AF_RERUN_ALL_NO_TRIGGER` 可以和 `AF_TRIGGER_SCOPE_*` 组合使用。  
但如果额外给 `AF_TRIGGER_SCOPE_*` 赋了全局 `DAG Runs.can_create`，仍会导致跨 scope trigger。  
如果要“只能 rerun、不能 pause”，需要自定义 Auth Manager 逻辑。
