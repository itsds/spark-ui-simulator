# Source me: shared environment for Spark/Hadoop scripts.
#   JAVA_HOME   -> JDK 17 (Spark 3.5's bundled Arrow does not work on JDK 21; see ADR-007)
#   SPARK_HOME  -> the Spark 3.5.9 distribution inside the project's PySpark (uv venv)
#   HADOOP_HOME -> /opt/hadoop unless set (only needed for the YARN reference cluster)
ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

if [[ -z "${JAVA_HOME:-}" || "$("$JAVA_HOME/bin/java" -version 2>&1 | grep -oE 'version "[0-9]+' | grep -oE '[0-9]+$')" != "17" ]]; then
  for c in /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-17-openjdk-arm64 /usr/lib/jvm/temurin-17-jdk-amd64 \
           "$(/usr/libexec/java_home -v 17 2>/dev/null || true)"; do
    if [[ -n "$c" && -x "$c/bin/java" ]]; then export JAVA_HOME="$c"; break; fi
  done
fi
export PATH="$JAVA_HOME/bin:$PATH"

export SPARK_HOME="${SPARK_HOME:-$(cd "$ROOT" && uv run --quiet python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')}"
export PYTHON_BIN="$ROOT/.venv/bin/python"

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_BUILD="$ROOT/build/hadoop"
export HADOOP_CONF_DIR="$HADOOP_BUILD/conf"
export YARN_CONF_DIR="$HADOOP_CONF_DIR"
