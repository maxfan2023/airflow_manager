#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    printf 'ERROR: missing required command: %s\n' "$name" >&2
    exit 1
  fi
}

AIRFLOW_VERSION="${AIRFLOW_VERSION:-3.1.6}"
PYTHON_VERSION="${PYTHON_VERSION:-3.12.12}"
IFS='.' read -r PYTHON_MAJOR PYTHON_MINOR _ <<< "${PYTHON_VERSION}"
if [[ -z "${PYTHON_MAJOR}" || -z "${PYTHON_MINOR}" ]]; then
  echo "ERROR: invalid PYTHON_VERSION=${PYTHON_VERSION}. Expected format like 3.12.12" >&2
  exit 1
fi
PYTHON_SERIES="${PYTHON_MAJOR}.${PYTHON_MINOR}"
PYTHON_TAG="${PYTHON_MAJOR}${PYTHON_MINOR}"
AIRFLOW_EXTRAS="${AIRFLOW_EXTRAS:-celery,postgres,redis,ldap,kerberos,apache-hdfs,apache-hive,apache-spark}"
CONSTRAINTS_URL="${CONSTRAINTS_URL:-https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_SERIES}.txt}"

CONDA_IMAGE="${CONDA_IMAGE:-condaforge/mambaforge:latest}"
MANYLINUX_IMAGE="${MANYLINUX_IMAGE:-quay.io/pypa/manylinux_2_28_x86_64}"
RHEL_TEST_IMAGE="${RHEL_TEST_IMAGE:-rockylinux:8}"
DO_RHEL8_TEST="${DO_RHEL8_TEST:-1}"
RUN_CONDA_STAGE="${RUN_CONDA_STAGE:-1}"
RUN_PIP_STAGE="${RUN_PIP_STAGE:-1}"

BUNDLE_DIR="${1:-$PWD/offline_bundle_airflow_${AIRFLOW_VERSION}_py${PYTHON_VERSION}_linux_x86_64}"
BUNDLE_DIR="$(cd "$(dirname "$BUNDLE_DIR")" && pwd)/$(basename "$BUNDLE_DIR")"

CONDA_DIR="${BUNDLE_DIR}/conda"
PIP_DIR="${BUNDLE_DIR}/pip"
TEST_DIR="${BUNDLE_DIR}/test"

require_cmd docker

mkdir -p "${CONDA_DIR}/pkgs/linux-64" "${CONDA_DIR}/pkgs/noarch" "${PIP_DIR}/wheels" "${PIP_DIR}/sdists" "${TEST_DIR}"

cat >"${CONDA_DIR}/conda-runtime-specs.txt" <<EOF
python=${PYTHON_VERSION}
pip
setuptools
wheel
openssl
ca-certificates
krb5
openldap
cyrus-sasl
libpq
libffi
libxml2
libxslt
zlib
bzip2
xz
sqlite
gcc_linux-64
gxx_linux-64
binutils_linux-64
make
libgcc-ng
libstdcxx-ng
python-ldap
python-gssapi
pykerberos
EOF

cat >"${PIP_DIR}/requirements-airflow.txt" <<EOF
apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}
apache-airflow-providers-celery
apache-airflow-providers-fab
graphviz
flower>=2.0.0
EOF

cat >"${CONDA_DIR}/install_conda_offline.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if ! command -v conda >/dev/null 2>&1; then
  echo "ERROR: conda command not found in PATH." >&2
  exit 1
fi

TARGET_ENV="${1:-airflow312}"
CONDA_BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_EXPLICIT="${CONDA_BUNDLE_DIR}/explicit-linux-64.txt"
INSTALL_TOOLCHAIN="${INSTALL_TOOLCHAIN:-1}"
TOOLCHAIN_PKGS=(
  gcc_linux-64
  gxx_linux-64
  binutils_linux-64
  make
  libgcc-ng
  libstdcxx-ng
)

if [[ ! -f "${SRC_EXPLICIT}" ]]; then
  echo "ERROR: explicit-linux-64.txt not found in ${CONDA_BUNDLE_DIR}" >&2
  exit 1
fi

TMP_EXPLICIT="$(mktemp)"
trap 'rm -f "${TMP_EXPLICIT}"' EXIT

sed \
  -e "s#https://conda.anaconda.org/conda-forge#file://${CONDA_BUNDLE_DIR}/pkgs#g" \
  -e "s#http://conda.anaconda.org/conda-forge#file://${CONDA_BUNDLE_DIR}/pkgs#g" \
  "${SRC_EXPLICIT}" >"${TMP_EXPLICIT}"

CONDA_SUBDIR=linux-64 conda create -y --offline -n "${TARGET_ENV}" --file "${TMP_EXPLICIT}"
if [[ "${INSTALL_TOOLCHAIN}" == "1" ]]; then
  missing_pkgs=()
  for pkg in "${TOOLCHAIN_PKGS[@]}"; do
    if ! conda list -n "${TARGET_ENV}" | awk '{print $1}' | grep -qx "${pkg}"; then
      missing_pkgs+=("${pkg}")
    fi
  done

  if [[ "${#missing_pkgs[@]}" -gt 0 ]]; then
    cat >&2 <<EOM
ERROR: compiler toolchain packages are missing in env ${TARGET_ENV}:
  ${missing_pkgs[*]}
This offline bundle's explicit-linux-64.txt does not include the full toolchain.
Rebuild bundle with updated build script, or install online:
  conda install -n ${TARGET_ENV} -c conda-forge gcc_linux-64 gxx_linux-64 binutils_linux-64 make libgcc-ng libstdcxx-ng
EOM
    exit 1
  fi

  echo "Compiler toolchain is present in env: ${TARGET_ENV}"
else
  echo "Compiler toolchain installation skipped (INSTALL_TOOLCHAIN=${INSTALL_TOOLCHAIN})."
fi

echo "Conda offline env created: ${TARGET_ENV}"
EOF
chmod +x "${CONDA_DIR}/install_conda_offline.sh"

cat >"${PIP_DIR}/install_pip_offline.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail

PIP_BUNDLE_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
REQ_FILE="\${PIP_BUNDLE_DIR}/requirements-airflow.txt"
CONSTRAINT_FILE="\${PIP_BUNDLE_DIR}/constraints-${PYTHON_SERIES}.txt"

if [[ ! -f "\${REQ_FILE}" ]]; then
  echo "ERROR: missing \${REQ_FILE}" >&2
  exit 1
fi

if [[ ! -f "\${CONSTRAINT_FILE}" ]]; then
  echo "ERROR: missing \${CONSTRAINT_FILE}" >&2
  exit 1
fi

python -m pip install \
  --no-index \
  --find-links "\${PIP_BUNDLE_DIR}/wheels" \
  --constraint "\${CONSTRAINT_FILE}" \
  --requirement "\${REQ_FILE}"

python -m pip check
echo "Pip offline install completed."
EOF
chmod +x "${PIP_DIR}/install_pip_offline.sh"

if [[ "${RUN_CONDA_STAGE}" == "1" ]]; then
  log "Stage 1/3: resolve and export conda runtime packages (linux-64/noarch) ..."
  docker run --rm -i --platform linux/amd64 \
    -v "${BUNDLE_DIR}:/bundle" \
    "${CONDA_IMAGE}" \
    bash -s <<'EOF'
set -euo pipefail

if ! command -v mamba >/dev/null 2>&1; then
  conda install -y -n base -c conda-forge mamba
fi

mamba create -y -p /tmp/airflow-conda-export \
  --override-channels -c conda-forge \
  --file /bundle/conda/conda-runtime-specs.txt

conda list --explicit -p /tmp/airflow-conda-export >/bundle/conda/explicit-linux-64.txt

while IFS= read -r url; do
  case "${url}" in
    http://*|https://*)
      pkg_name="$(basename "${url}")"
      subdir="linux-64"
      if [[ "${url}" == */noarch/* ]]; then
        subdir="noarch"
      fi
      src_file="/opt/conda/pkgs/${pkg_name}"
      if [[ ! -f "${src_file}" ]]; then
        src_file="$(find /opt/conda/pkgs -maxdepth 1 -name "${pkg_name}" -print -quit || true)"
      fi
      if [[ -z "${src_file}" || ! -f "${src_file}" ]]; then
        echo "ERROR: package not found in conda cache: ${pkg_name}" >&2
        exit 1
      fi
      cp -f "${src_file}" "/bundle/conda/pkgs/${subdir}/${pkg_name}"
      ;;
  esac
done </bundle/conda/explicit-linux-64.txt

echo "conda export complete"
EOF
else
  log "Stage 1/3 skipped (RUN_CONDA_STAGE=${RUN_CONDA_STAGE})"
fi

if [[ "${RUN_PIP_STAGE}" == "1" ]]; then
  log "Stage 2/3: download pip deps and build missing wheels in manylinux_2_28_x86_64 ..."
  docker run --rm -i --platform linux/amd64 \
    -v "${BUNDLE_DIR}:/bundle" \
    "${MANYLINUX_IMAGE}" \
    bash -s -- "${PYTHON_TAG}" "${PYTHON_SERIES}" "${CONSTRAINTS_URL}" <<'EOF'
set -euo pipefail
PYTHON_TAG="$1"
PYTHON_SERIES="$2"
CONSTRAINTS_URL="$3"

install_system_build_deps() {
  local pkgs="curl findutils gcc gcc-c++ make libffi-devel openssl-devel openldap-devel cyrus-sasl-devel krb5-devel postgresql-devel"
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y ${pkgs}
    dnf clean all
  elif command -v yum >/dev/null 2>&1; then
    yum install -y ${pkgs}
    yum clean all
  fi
}

install_java17() {
  local java_pkg=""
  if command -v dnf >/dev/null 2>&1; then
    for p in java-17-openjdk-devel java-17-openjdk java-17-openjdk-headless; do
      if dnf install -y "${p}"; then
        java_pkg="${p}"
        break
      fi
    done
    dnf clean all
  elif command -v yum >/dev/null 2>&1; then
    for p in java-17-openjdk-devel java-17-openjdk java-17-openjdk-headless; do
      if yum install -y "${p}"; then
        java_pkg="${p}"
        break
      fi
    done
    yum clean all
  fi

  if [[ -z "${java_pkg}" ]]; then
    echo "ERROR: failed to install Java 17 package (tried java-17-openjdk*)." >&2
    exit 1
  fi

  if ! command -v java >/dev/null 2>&1 || ! command -v javac >/dev/null 2>&1; then
    echo "ERROR: java/javac not found after installing ${java_pkg}" >&2
    exit 1
  fi

  export JAVA_HOME
  JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
  export PATH="${JAVA_HOME}/bin:${PATH}"

  local java_version_line java_major
  java_version_line="$(java -version 2>&1 | head -n1)"
  java_major="$(echo "${java_version_line}" | sed -E 's/.*version "([0-9]+).*/\1/')"
  if [[ "${java_major}" != "17" ]]; then
    echo "ERROR: expected Java 17, but got: ${java_version_line}" >&2
    exit 1
  fi

  echo "Using Java for wheel builds: ${java_version_line}"
}

install_system_build_deps
install_java17

PYBIN="/opt/python/cp${PYTHON_TAG}-cp${PYTHON_TAG}/bin/python"
if [[ ! -x "${PYBIN}" ]]; then
  PYBIN="$(find /opt/python -maxdepth 3 -type f -path "*/cp${PYTHON_TAG}*/bin/python" -print -quit || true)"
fi
if [[ -z "${PYBIN}" || ! -x "${PYBIN}" ]]; then
  echo "ERROR: Python cp${PYTHON_TAG} not found in manylinux image" >&2
  exit 1
fi
PIPBIN="${PYBIN%/python}/pip"

"${PIPBIN}" install -U pip setuptools wheel build
CONSTRAINT_FILE="/bundle/pip/constraints-${PYTHON_SERIES}.txt"
curl -fsSL "${CONSTRAINTS_URL}" -o "${CONSTRAINT_FILE}"

mkdir -p /bundle/pip/raw /bundle/pip/wheels /bundle/pip/sdists
"${PIPBIN}" download \
  --dest /bundle/pip/raw \
  --constraint "${CONSTRAINT_FILE}" \
  --requirement /bundle/pip/requirements-airflow.txt

find /bundle/pip/raw -maxdepth 1 -type f -name '*.whl' -exec cp -f {} /bundle/pip/wheels/ \;
find /bundle/pip/raw -maxdepth 1 -type f \( -name '*.tar.gz' -o -name '*.zip' \) -exec cp -f {} /bundle/pip/sdists/ \;

if compgen -G "/bundle/pip/sdists/*" >/dev/null; then
  "${PIPBIN}" wheel \
    --wheel-dir /bundle/pip/wheels \
    --constraint "${CONSTRAINT_FILE}" \
    /bundle/pip/sdists/*
fi

"${PYBIN}" -m venv /tmp/pip-offline-check
source /tmp/pip-offline-check/bin/activate
pip install -U pip
pip install --no-index --find-links /bundle/pip/wheels \
  --constraint "${CONSTRAINT_FILE}" \
  --requirement /bundle/pip/requirements-airflow.txt
python -c "import airflow; print('airflow version:', airflow.__version__)"
pip check

echo "pip export complete"
EOF
else
  log "Stage 2/3 skipped (RUN_PIP_STAGE=${RUN_PIP_STAGE})"
fi

if [[ "${DO_RHEL8_TEST}" == "1" ]]; then
  log "Stage 3/3: validate offline conda+pip installation in Rocky Linux 8 container ..."
  docker run --rm -i --platform linux/amd64 \
    -v "${BUNDLE_DIR}:/bundle" \
    "${RHEL_TEST_IMAGE}" \
    bash -s -- "${PYTHON_SERIES}" <<'EOF'
set -euo pipefail
PYTHON_SERIES="$1"

if command -v dnf >/dev/null 2>&1; then
  dnf install -y bzip2 ca-certificates curl findutils which
  dnf clean all
elif command -v yum >/dev/null 2>&1; then
  yum install -y bzip2 ca-certificates curl findutils which
  yum clean all
fi

curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p /opt/miniconda
source /opt/miniconda/etc/profile.d/conda.sh

bash /bundle/conda/install_conda_offline.sh airflow_offline_test
conda activate airflow_offline_test

python -m pip install --no-index --find-links /bundle/pip/wheels \
  --constraint "/bundle/pip/constraints-${PYTHON_SERIES}.txt" \
  --requirement /bundle/pip/requirements-airflow.txt

python - <<'PY'
import importlib
mods = [
    "airflow",
    "airflow.providers.celery.executors.celery_executor",
    "airflow.providers.postgres.hooks.postgres",
    "airflow.providers.redis.hooks.redis",
    "airflow.providers.apache.spark.operators.spark_submit",
    "airflow.providers.apache.hive.hooks.hive",
    "airflow.providers.apache.hdfs.hooks.webhdfs",
    "ldap",
    "gssapi",
    "kerberos",
]
for m in mods:
    importlib.import_module(m)
print("offline import checks passed")
PY

python -c "import ssl; print('openssl:', ssl.OPENSSL_VERSION)"
airflow version
python -m pip check
EOF
fi

cat >"${BUNDLE_DIR}/README-OFFLINE-INSTALL.txt" <<EOF
Offline bundle created: ${BUNDLE_DIR}

1) Copy this folder to your RHEL8 x86_64 server.
2) Activate your anaconda base first, then run:
   bash ./conda/install_conda_offline.sh airflow312
3) Activate env and install airflow wheels offline:
   conda activate airflow312
   bash ./pip/install_pip_offline.sh
4) Validate:
   airflow version
   python -m pip check
5) Incremental install for FAB provider (optional, existing env):
   conda activate airflow312
   python -m pip install --no-index \
     --find-links ./pip/wheels \
     --constraint ./pip/constraints-${PYTHON_SERIES}.txt \
     apache-airflow-providers-fab
   python -m pip check

Files:
- conda runtime specs: ./conda/conda-runtime-specs.txt
- conda explicit lock: ./conda/explicit-linux-64.txt
- conda pkgs: ./conda/pkgs/linux-64 and ./conda/pkgs/noarch
- pip requirements: ./pip/requirements-airflow.txt
- airflow constraints: ./pip/constraints-${PYTHON_SERIES}.txt
- wheels: ./pip/wheels
- sdists: ./pip/sdists
EOF

log "Bundle completed: ${BUNDLE_DIR}"
log "Conda packages: $(find "${CONDA_DIR}/pkgs" -type f | wc -l | tr -d ' ') files"
log "Wheel files: $(find "${PIP_DIR}/wheels" -type f -name '*.whl' | wc -l | tr -d ' ') files"
