"""YARN identities as they appear in Spark event logs (cluster deploy mode).

Formats come from the real YARN captures (EVENT_LOG_FORMAT.md §6):
`application_<clusterTs>_<seq:04>`, `container_<clusterTs>_<seq:04>_<attempt:02>_<n:06>`.
Container 000001 is the ApplicationMaster, which runs the driver in cluster mode.
NodeManager log URLs and executor attributes match what YARN's
`ExecutorRunnable` / `YarnAllocator` report.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NodeManager:
    host: str
    ip: str
    port: int = 45454  # yarn.nodemanager.address
    http_port: int = 8042  # yarn.nodemanager.webapp.address

    @property
    def http_address(self) -> str:
        return f"{self.host}:{self.http_port}"


@dataclass(frozen=True)
class YarnApp:
    cluster_timestamp: int  # ResourceManager start time (ms)
    seq: int
    attempt: int = 1

    @property
    def app_id(self) -> str:
        return f"application_{self.cluster_timestamp}_{self.seq:04d}"

    def container_id(self, n: int) -> str:
        return f"container_{self.cluster_timestamp}_{self.seq:04d}_{self.attempt:02d}_{n:06d}"

    @property
    def am_container_id(self) -> str:
        return self.container_id(1)

    @property
    def event_log_name(self) -> str:
        """`<appId>_<attemptId>`: the file name Spark gives the log in cluster mode."""
        return f"{self.app_id}_{self.attempt}"


def container_log_urls(nm: NodeManager, container_id: str, user: str) -> dict[str, str]:
    base = f"http://{nm.http_address}/node/containerlogs/{container_id}/{user}"
    return {"stdout": f"{base}/stdout?start=-4096", "stderr": f"{base}/stderr?start=-4096"}


def container_attributes(nm: NodeManager, container_id: str, user: str, cluster_id: str = "") -> dict[str, str]:
    """`Executor Info.Attributes` / `Driver Attributes`, key order as YARN cluster-mode logs write them."""
    return {
        "NM_HTTP_ADDRESS": nm.http_address,
        "USER": user,
        "LOG_FILES": "stderr,stdout",
        "NM_HTTP_PORT": str(nm.http_port),
        "CLUSTER_ID": cluster_id,
        "NM_PORT": str(nm.port),
        "HTTP_SCHEME": "http://",
        "NM_HOST": nm.host,
        "CONTAINER_ID": container_id,
    }


def container_work_dir(local_dir: str, user: str, app: YarnApp, container_id: str) -> str:
    """`<yarn.nodemanager.local-dirs>/usercache/<user>/appcache/<appId>/<containerId>`."""
    return f"{local_dir}/usercache/{user}/appcache/{app.app_id}/{container_id}"


def jvm_max_memory(heap_bytes: int) -> int:
    """`Runtime.maxMemory()` for `-Xmx<heap>` under G1 on JDK 17 (one byte less: see ref logs)."""
    return heap_bytes - 1


def unified_memory_max(heap_bytes: int, memory_fraction: float = 0.6) -> int:
    """BlockManager "Maximum Memory": (maxMemory - 300 MiB reserved) * spark.memory.fraction.

    Matches the reference logs: 1536m -> 777623961, 2g -> 1099746508.
    """
    return int((jvm_max_memory(heap_bytes) - 300 * 1024 * 1024) * memory_fraction)


MiB = 1024 * 1024
GiB = 1024 * MiB
