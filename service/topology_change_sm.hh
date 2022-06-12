/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <boost/range/algorithm/find.hpp>
#include "boost/range/join.hpp"
#include<iostream>
#include<unordered_set>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include "dht/token.hh"
#include "raft/raft.hh"
#include "utils/UUID.hh"
#include "utils/rjson.hh"

namespace service {

enum class tokens_state: uint8_t {
    write_only,
    read_write,
    owner
};

enum class node_state: uint8_t {
    none,
    bootstrapping,
    unbootstrapping,
    removing,
    replacing,
    normal,
    left
};

struct replica_state {
    node_state state;
    rjson::value state_params;
    seastar::sstring datacenter;
    seastar::sstring rack;
    seastar::sstring release_version;
    // contains a request (joining, leaving, removing, replacing)
    // and request's parameters to do a topology operation on the node
    std::optional<rjson::value> topology_request;
    struct ring_state {
        tokens_state state;
        std::unordered_set<dht::token> tokens;
    };
    std::optional<ring_state> ring;
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
struct topology_change_sm {
    using topology_type = topology;
    topology_type _topology;
    condition_variable event;
};

inline std::ostream& operator<<(std::ostream& os, tokens_state s) {
    switch (s) {
        case tokens_state::write_only:
            os << "write only";
        break;
        case tokens_state::read_write:
            os << "read write";
        break;
        case tokens_state::owner:
            os << "owner";
        break;
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, node_state s) {
    switch (s) {
        case node_state::bootstrapping:
            os << "bootstrapping";
        break;
        case node_state::unbootstrapping:
            os << "unbootstrapping";
        break;
        case node_state::removing:
            os << "removing";
        break;
        case node_state::normal:
            os << "normal";
        break;
        case node_state::left:
            os << "left";
        break;
        case node_state::replacing:
            os << "replacing";
        break;
        case node_state::none:
            os << "none";
        break;
    }
    return os;
}

}
