#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster import ReplaceConfig
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)

async def inject_error_on(manager, error_name, servers):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, True) for s in servers]
    await asyncio.gather(*errs)

@pytest.mark.asyncio
async def test_topology_streaming_failure(request, manager: ManagerClient):
    """Fail streaming while doing a topology operation"""
    # decommission failure
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.api.enable_injection(servers[2].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.decommission_node(servers[2].server_id, expected_error="Decommission failed. See earlier errors")
    servers = await manager.running_servers()
    assert len(servers) == 3
    matches = [await log.grep("storage_service - rollback.*after decommissioning failure to state rollback_to_normal", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # remove failure
    marks = [await log.mark() for log in logs]
    await manager.server_add()
    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.api.enable_injection(servers[2].ip_addr, 'stream_ranges_fail', one_shot=True)
    await manager.remove_node(servers[0].server_id, servers[3].server_id, expected_error="Removenode failed. See earlier errors")
    matches = [await log.grep("storage_service - rollback.*after removing failure to state rollback_to_normal", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    await manager.server_start(servers[3].server_id)
    await manager.servers_see_each_other(servers)
    # bootstrap failure
    marks = [await log.mark() for log in logs]
    servers = await manager.running_servers()
    s = await manager.server_add(start=False, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Bootstrap failed. See earlier errors")
    servers = await manager.running_servers()
    assert s not in servers
    matches = [await log.grep("storage_service - rollback.*after bootstrapping failure to state left_token_ring", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1
    # replace failure
    marks = [await log.mark() for log in logs]
    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[2].server_id)
    replace_cfg = ReplaceConfig(replaced_id = servers[2].server_id, reuse_ip_addr = False, use_host_id = True)
    s = await manager.server_add(start=False, replace_cfg=replace_cfg, config={
        'error_injections_at_startup': ['stream_ranges_fail']
    })
    await manager.server_start(s.server_id, expected_error="Replace failed. See earlier errors")
    servers = await manager.running_servers()
    assert s not in servers
    matches = [await log.grep("storage_service - rollback.*after replacing failure to state left_token_ring", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1

@pytest.mark.asyncio
async def test_tablet_drain_failure_during_decommission(manager: ManagerClient):
    servers = await manager.running_servers()

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': 1, 'initial_tablets': 32};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    logger.info("Populating table")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    await inject_error_on(manager, "stream_tablet_fail_on_drain", servers)

    await manager.decommission_node(servers[2].server_id, expected_error="Decommission failed. See earlier errors")

    matches = [await log.grep("storage_service - rollback.*after decommissioning failure to state rollback_to_normal", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 1

    await cql.run_async("DROP KEYSPACE test;")

@pytest.mark.asyncio
async def test_coordinator_queue_management(manager: ManagerClient):
    """This test creates a 5 node cluster with 2 down nodes (A and B). After that it
       creates a queue of 3 topology operation: bootstrap, removenode A and removenode B
       with ignore_nodes=A. Check that all operation manage to complete.
       Then it downs one node and creates a queue with two requests:
       bootstrap and decommission. Since none can proceed both should be canceled.
    """
    await manager.server_add()
    await manager.server_add()
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.server_stop_gracefully(servers[4].server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[3].ip_addr)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[4].ip_addr)

    inj = 'topology_coordinator_pause_before_processing_backlog'
    [await manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers[:3]]

    s3_id = await manager.get_host_id(servers[3].server_id)
    tasks = [asyncio.create_task(manager.server_add()),
             asyncio.create_task(manager.remove_node(servers[0].server_id, servers[3].server_id)),
             asyncio.create_task(manager.remove_node(servers[1].server_id, servers[4].server_id, [s3_id]))]

    search = [asyncio.create_task(l.wait_for("received request to join from host_id", m) for l, m in zip(logs[:3], marks[:3]))]
    done, pending = await asyncio.wait(search, return_when = asyncio.FIRST_COMPLETED)
    for t in pending: t.cancel()

    [await l.wait_for("raft topology: removenode: wait for completion", m) for l, m in zip(logs[:2], marks[:2])]

    [await manager.api.message_injection(s.ip_addr, inj) for s in servers[:3]]

    await asyncio.gather(*tasks)

    servers = await manager.running_servers()
    await manager.server_stop_gracefully(servers[3].server_id)
    await manager.server_not_sees_other_server(servers[0].ip_addr, servers[3].ip_addr)

    [await manager.api.enable_injection(s.ip_addr, inj, one_shot=True) for s in servers[:3]]

    s = await manager.server_add(start=False)

    tasks = [asyncio.create_task(manager.server_start(s.server_id, expected_error="request canceled because some required nodes are dead")),
             asyncio.create_task(manager.decommission_node(servers[1].server_id, expected_error="Decommission failed. See earlier errors"))]

    search = [asyncio.create_task(l.wait_for("received request to join from host_id", m) for l, m in zip(logs[:3], marks[:3]))]
    done, pending = await asyncio.wait(search, return_when = asyncio.FIRST_COMPLETED)
    for t in pending: t.cancel()

    logs[1].wait_for("raft topology: decommission: wait for completion", marks[1])

    [await manager.api.message_injection(s.ip_addr, inj) for s in servers[:3]]

    await asyncio.gather(*tasks)
