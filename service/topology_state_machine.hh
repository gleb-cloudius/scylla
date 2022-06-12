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

struct ring_state {
    enum class replication_state: uint8_t {
        write_only,
        read_write,
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
    std::optional<topology_request> request;
    std::optional<ring_state> ring; // if engaged contain the set of tokens the node owns together with their state
};

struct topology {
    std::unordered_map<raft::server_id, replica_state> normal_nodes;
    std::unordered_map<raft::server_id, replica_state> left_nodes;
    std::unordered_map<raft::server_id, replica_state> new_nodes;
    std::unordered_map<raft::server_id, replica_state> transition_nodes;
    const std::pair<const raft::server_id, replica_state>* find(raft::server_id id) {
        auto range = boost::range::join(normal_nodes,
                                boost::range::join(transition_nodes,
                                    boost::range::join(new_nodes, left_nodes)));
        auto it = boost::find_if(range, [id] (auto& e) { return e.first == id; });
        return it == range.end() ? nullptr : &*it;
    }
    bool contains(raft::server_id id) {
        auto range = boost::range::join(normal_nodes,
                                boost::range::join(transition_nodes,
                                    boost::range::join(new_nodes, left_nodes)));
        return boost::find_if(range, [id] (auto& e) { return e.first == id; }) != range.end();
    }
};

// State machine that is responsible for topology change
struct topology_state_machine {
    using topology_type = topology;
    topology_type _topology;
    condition_variable event;
};

static std::unordered_map<ring_state::replication_state, sstring> replication_state_to_name_map = {
    {ring_state::replication_state::write_only, "write only"},
    {ring_state::replication_state::read_write, "read write"},
    {ring_state::replication_state::owner, "owner"},
};

inline std::ostream& operator<<(std::ostream& os, ring_state::replication_state s) {
    os << replication_state_to_name_map[s];
    return os;
}

inline ring_state::replication_state replication_state_from_string(const sstring& s) {
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
