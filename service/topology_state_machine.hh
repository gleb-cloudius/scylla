/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <boost/range/algorithm/find_if.hpp>
#include "boost/range/join.hpp"
#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include "dht/token.hh"
#include "raft/raft.hh"
#include "utils/UUID.hh"
#include "dht/i_partitioner.hh"

namespace service {

enum class node_state: uint8_t {
    none,
    bootstrapping,
    decommissioning,
    removing,
    replacing,
    normal,
    left
};

enum class topology_request: uint8_t {
    join,
    leave,
    remove,
    replace
};

struct ring_slice {
    enum class replication_state: uint8_t {
        write_both_read_old,
        write_both_read_new,
        owner
    };

    replication_state state;
    std::unordered_set<dht::token> tokens;
};

struct replica_state {
    node_state state;
    seastar::sstring datacenter;
    seastar::sstring rack;
    seastar::sstring release_version;
    std::optional<raft::server_id> replaced_id; // id of the node this node replaces/d if engaged
    std::optional<ring_slice> ring; // if engaged contain the set of tokens the node owns together with their state
};

struct topology {
    // Nodes that are normal members of the ring
    std::unordered_map<raft::server_id, replica_state> normal_nodes;
    // Nodes that are left
    std::unordered_set<raft::server_id> left_nodes;
    // Nodes that are waiting to be joined by the topology coordinator
    std::unordered_map<raft::server_id, replica_state> new_nodes;
    // Nodes that are in the process to be added to the ring
    // Currently only at mpst one node at a time will be here
    std::unordered_map<raft::server_id, replica_state> transition_nodes;

    // pending topology requests
    std::unordered_map<raft::server_id, topology_request> requests;

    // Find only nodes in non 'left' state
    const std::pair<const raft::server_id, replica_state>* find(raft::server_id id) {
        auto it = normal_nodes.find(id);
        if (it != normal_nodes.end()) {
            return &*it;
        }
        it = transition_nodes.find(id);
        if (it != transition_nodes.end()) {
            return &*it;
        }
        it = new_nodes.find(id);
        if (it != new_nodes.end()) {
            return &*it;
        }
        return nullptr;
    }
    // Return true if node exists in any state including 'left' one
    bool contains(raft::server_id id) {
        return normal_nodes.contains(id) ||
               transition_nodes.contains(id) ||
               new_nodes.contains(id) ||
               left_nodes.contains(id);
    }
};

// State machine that is responsible for topology change
struct topology_state_machine {
    using topology_type = topology;
    topology_type _topology;
    condition_variable event;
};

static std::unordered_map<ring_slice::replication_state, sstring> replication_state_to_name_map = {
    {ring_slice::replication_state::write_both_read_old, "write both read old"},
    {ring_slice::replication_state::write_both_read_new, "write both read new"},
    {ring_slice::replication_state::owner, "owner"},
};

inline std::ostream& operator<<(std::ostream& os, ring_slice::replication_state s) {
    os << replication_state_to_name_map[s];
    return os;
}

inline ring_slice::replication_state replication_state_from_string(const sstring& s) {
    for (auto&& e : replication_state_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    throw std::runtime_error(fmt::format("cannot map name {} to token_state", s));
}

static std::unordered_map<node_state, sstring> node_state_to_name_map = {
    {node_state::bootstrapping, "bootstrapping"},
    {node_state::decommissioning, "decommissioning"},
    {node_state::removing, "removing"},
    {node_state::normal, "normal"},
    {node_state::left, "left"},
    {node_state::replacing, "replacing"},
    {node_state::none, "none"}
};

inline std::ostream& operator<<(std::ostream& os, node_state s) {
    os << node_state_to_name_map[s];
    return os;
}

inline node_state node_state_from_string(const sstring& s) {
    for (auto&& e : node_state_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    throw std::runtime_error(fmt::format("cannot map name {} to node_state", s));
}

static std::unordered_map<topology_request, sstring> topology_request_to_name_map = {
    {topology_request::join, "join"},
    {topology_request::leave, "leave"},
    {topology_request::remove, "remove"},
    {topology_request::replace, "replace"}
};

inline std::ostream& operator<<(std::ostream& os, const topology_request& req) {
    os << topology_request_to_name_map[req];
    return os;
}

inline topology_request topology_request_from_string(const sstring& s) {
    for (auto&& e : topology_request_to_name_map) {
        if (e.second == s) {
            return e.first;
        }
    }
    throw std::runtime_error(fmt::format("cannot map name {} to topology_request", s));
}
}
