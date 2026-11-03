"""YARN identities, container sizing and the container memory model."""

from __future__ import annotations

from sparksim.yarn import NodeManager, YarnApp, container_attributes, container_log_urls


def test_container_ids_follow_hadoop_format():
    # Reference cluster (never restarted): no epoch, exactly as in reference/event-logs.
    ref = YarnApp(1793676968612, 17)
    assert ref.app_id == "application_1793676968612_0017"
    assert ref.container_id(2) == "container_1793676968612_0017_01_000002"
    assert ref.attempt_id == "appattempt_1793676968612_0017_000001"
    # Production RM after restarts: ContainerId.toString prints the epoch with at least 2 digits.
    prod = YarnApp(1791902144377, 48213, epoch=23)
    assert prod.app_id == "application_1791902144377_48213"
    assert prod.am_container_id == "container_e23_1791902144377_48213_01_000001"
    assert YarnApp(1, 5, epoch=7).container_id(12) == "container_e07_1_0005_01_000012"
    assert prod.event_log_name == "application_1791902144377_48213_1"


def test_log_urls_and_attributes():
    nm = NodeManager("wn017.hadoop.prod.internal", "10.31.1.27")
    cid = "container_e23_1791902144377_48213_01_000003"
    urls = container_log_urls(nm, cid, "svc_analytics")
    assert urls["stderr"] == ("http://wn017.hadoop.prod.internal:8042/node/containerlogs/"
                              f"{cid}/svc_analytics/stderr?start=-4096")
    attrs = container_attributes(nm, cid, "svc_analytics", "yarn-prod")
    # key order as YARN cluster-mode logs write them (reference/event-logs)
    assert list(attrs) == ["NM_HTTP_ADDRESS", "USER", "LOG_FILES", "NM_HTTP_PORT", "CLUSTER_ID", "NM_PORT",
                           "HTTP_SCHEME", "NM_HOST", "CONTAINER_ID"]
    assert attrs["CLUSTER_ID"] == "yarn-prod" and attrs["NM_PORT"] == "45454"
