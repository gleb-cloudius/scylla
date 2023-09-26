#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster import ReplaceConfig
from functools import reduce
import pytest
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_bootstrap_streaming_failure(request, manager: ManagerClient):
    """Fail streaming while bootstrapping a node"""
    servers = await manager.running_servers()
    s = await manager.server_add(start=False, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Bootstrap failed. See early errors")
    servers = await manager.running_servers()
    assert servers.count(s) == 0
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    matches = [await log.grep("rollback.*after bootstrapping failure to state left_token_ring") for log in logs]
    assert reduce((lambda x, y: x + y), map((lambda x: len(x)), matches)) == 1

@pytest.mark.asyncio
async def test_replace_streaming_failure(request, manager: ManagerClient):
    """Fail streaming while replacing a node"""
    servers = await manager.running_servers()
    await manager.server_stop(servers[2].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = True)
    s = await manager.server_add(start=False, replace_cfg=replace_cfg, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Bootstrap failed. See early errors")
    servers = await manager.running_servers()
    assert servers.count(s) == 0
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    matches = [await log.grep("rollback.*after replacing failure to state left_token_ring") for log in logs]
    assert reduce((lambda x, y: x + y), map((lambda x: len(x)), matches)) == 1

@pytest.mark.asyncio
async def test_decommission_streaming_failure(request, manager: ManagerClient):
    """Fail streaming while decommissioning a node"""
    servers = await manager.running_servers()
    await manager.api.enable_injection(servers[2].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.decommission_node(servers[2].server_id)
    servers = await manager.running_servers()
    assert len(servers) == 3
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    matches = [await log.grep("rollback.*after decommissioning failure to state normal") for log in logs]
    assert reduce((lambda x, y: x + y), map((lambda x: len(x)), matches)) == 1
    match = await logs[2].grep("Decommission failed. See early errors")
    assert len(match) == 1

@pytest.mark.asyncio
async def test_decommission_streaming_failure(request, manager: ManagerClient):
    """Fail streaming while removing a node"""
    await manager.server_add()
    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.api.enable_injection(servers[2].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.remove_node(servers[0].server_id, servers[3].server_id)
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    matches = [await log.grep("rollback.*after removing failure to state normal") for log in logs]
    assert reduce((lambda x, y: x + y), map((lambda x: len(x)), matches)) == 1
    match = await logs[0].grep("Removenode failed. See early errors")
    assert len(match) == 1
