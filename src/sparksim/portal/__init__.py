"""The scenario portal (Phase 7, ADR-031): tickets, progressive hints, solutions and deep links into the real
History Server. `sparksim portal` serves it alone; `sparksim up` and `docker compose up` run it with the
History Server and the mock NodeManager log server."""

from .server import PortalConfig, make_server, serve

__all__ = ["PortalConfig", "make_server", "serve"]
