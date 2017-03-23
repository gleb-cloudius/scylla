/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "db/consistency_level.hh"

#include <boost/range/algorithm/stable_partition.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/transform.hpp>
#include "exceptions/exceptions.hh"
#include "core/sstring.hh"
#include "schema.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "db/read_repair_decision.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/network_topology_strategy.hh"
#include "utils/fb_utilities.hh"

namespace db {

logging::logger cl_logger("consistency");

size_t quorum_for(keyspace& ks) {
    return (ks.get_replication_strategy().get_replication_factor() / 2) + 1;
}

size_t local_quorum_for(keyspace& ks, const sstring& dc) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();

    if (rs.get_type() == replication_strategy_type::network_topology) {
        network_topology_strategy* nrs =
            static_cast<network_topology_strategy*>(&rs);

        return (nrs->get_replication_factor(dc) / 2) + 1;
    }

    return quorum_for(ks);
}

size_t block_for_local_serial(keyspace& ks) {
    using namespace locator;

    //
    // TODO: Consider caching the final result in order to avoid all these
    //       useless dereferencing. Note however that this will introduce quite
    //       a lot of complications since both snitch output for a local host
    //       and the snitch itself (and thus its output) may change dynamically.
    //
    auto& snitch_ptr = i_endpoint_snitch::get_local_snitch_ptr();
    auto local_addr = utils::fb_utilities::get_broadcast_address();

    return local_quorum_for(ks, snitch_ptr->get_datacenter(local_addr));
}

size_t block_for_each_quorum(keyspace& ks) {
    using namespace locator;

    auto& rs = ks.get_replication_strategy();

    if (rs.get_type() == replication_strategy_type::network_topology) {
        network_topology_strategy* nrs =
            static_cast<network_topology_strategy*>(&rs);
        size_t n = 0;

        for (auto& dc : nrs->get_datacenters()) {
            n += local_quorum_for(ks, dc);
        }

        return n;
    } else {
        return quorum_for(ks);
    }
}

size_t block_for(keyspace& ks, consistency_level cl) {
    switch (cl) {
    case consistency_level::ONE:
    case consistency_level::LOCAL_ONE:
        return 1;
    case consistency_level::ANY:
        return 1;
    case consistency_level::TWO:
        return 2;
    case consistency_level::THREE:
        return 3;
    case consistency_level::QUORUM:
    case consistency_level::SERIAL:
        return quorum_for(ks);
    case consistency_level::ALL:
        return ks.get_replication_strategy().get_replication_factor();
    case consistency_level::LOCAL_QUORUM:
    case consistency_level::LOCAL_SERIAL:
        return block_for_local_serial(ks);
    case consistency_level::EACH_QUORUM:
        return block_for_each_quorum(ks);
    default:
        abort();
    }
}

bool is_datacenter_local(consistency_level l) {
    return l == consistency_level::LOCAL_ONE || l == consistency_level::LOCAL_QUORUM;
}

bool is_local(gms::inet_address endpoint) {
    using namespace locator;

    auto& snitch_ptr = i_endpoint_snitch::get_local_snitch_ptr();
    auto local_addr = utils::fb_utilities::get_broadcast_address();

    return snitch_ptr->get_datacenter(local_addr) ==
           snitch_ptr->get_datacenter(endpoint);
}

std::vector<gms::inet_address>
filter_for_query(consistency_level cl,
                 keyspace& ks,
                 std::vector<gms::inet_address> live_endpoints,
                 read_repair_decision read_repair, gms::inet_address* extra, column_family* cf) {
    size_t local_count;

    if (read_repair == read_repair_decision::GLOBAL) { // take RRD.GLOBAL out of the way
        return std::move(live_endpoints);
    }

    if (read_repair == read_repair_decision::DC_LOCAL || is_datacenter_local(cl)) {
        auto it = boost::range::stable_partition(live_endpoints, is_local);
        local_count = std::distance(live_endpoints.begin(), it);
        if (is_datacenter_local(cl)) {
            live_endpoints.erase(it, live_endpoints.end());
        }
    }

    size_t bf = block_for(ks, cl);

    if (read_repair == read_repair_decision::DC_LOCAL) {
        bf = std::max(block_for(ks, cl), local_count);
    }

    if (bf >= live_endpoints.size()) { // RRD.DC_LOCAL + CL.LOCAL or CL.ALL
        return std::move(live_endpoints);
    }

    if (cf) {
        static thread_local std::default_random_engine random_engine;
        static thread_local std::uniform_real_distribution<> lbalance = std::uniform_real_distribution<>(0, 1);

        auto get_hit_rate = [cf] (gms::inet_address ep) -> float {
            constexpr float max_hit_rate = 0.999;
            auto ht = cf->get_hit_rate(ep);
            if (float(ht.rate) < 0) {
                return float(ht.rate);
            } else if (lowres_clock::now() - ht.last_updated > std::chrono::milliseconds(1000)) {
                // if a cache entry is not updates for a while try to send traffic there
                // to get more up to date data, mark it updated to not send to much traffic there
                cf->set_hit_rate(ep, ht.rate);
                return max_hit_rate;
            } else {
                return std::min(float(ht.rate), max_hit_rate); // calculation below cannot work with hit rate 1
            }
        };

        struct ep_info {
            gms::inet_address ep;
            float ht;
            float p;
        };

        float ht_max = 0;
        float ht_min = 1;
        float mr_sum = 0;
        float psum = 1;
        bool old_node = false;
        auto rf = live_endpoints.size();

        sstring log_message = "\n";
        sstring log;
        auto epi = boost::copy_range<std::vector<ep_info>>(live_endpoints | boost::adaptors::transformed([&] (gms::inet_address ep) {
            auto ht = get_hit_rate(ep);
            log += sprint("%d: %.10f ", ep, ht);
            old_node = old_node || ht < 0;
            ht_max = std::max(ht_max, ht);
            ht_min = std::min(ht_min, ht);
            mr_sum += 1/(1.0f - ht);
            // initially probability of each node is 1/rf, but may be recalculated later
            return ep_info{ep, ht, psum/rf};
        }));
        log_message += sprint("hit rates: (mr_sum=%.10f) %s\n",mr_sum, log); log = "";

        live_endpoints.clear();

        auto use_endpoint = [&] (ep_info& epi) {
            live_endpoints.push_back(epi.ep);
            psum -= epi.p;
            epi.p = 0;
        };

        thread_local static unsigned prev_branches = 0;
        unsigned branches = 0;

        if (!old_node && ht_max - ht_min > 0.01) { // if there is old node or hit rates are close skip calculations
            branches |= (1 << 0);
            float diffsum = 0;
            float restsum = 0;
            psum = 0;

            // recalculate p and psum according to hit rates
            for (auto&& ep : epi) {
                ep.p = 1 / (1.0f - ep.ht) / mr_sum;
                psum += ep.p;
                log += sprint("%d: %.10f ", ep.ep, ep.p);
                // shoehorn probabilities to be not greater than 1/CL
                if (ep.p > 1.0f / bf) {
                    diffsum += (ep.p - 1.0f / bf);
                    ep.p = 1.0f / bf;
                } else {
                    restsum += ep.p;
                }
            }

 //           cl_logger.debug("p according to hit rate (diffsum={}, restsum={}, psum={}): {}", diffsum, restsum, psum, log); log = "";
            log_message += sprint("p according to hit rate (diffsum=%.10f, restsum=%.10f, psum=%.10f): %s\n", diffsum, restsum, psum, log); log = "";

            // local node is always first if present (see storage_proxy::get_live_sorted_endpoints)
            if (epi[0].ep == utils::fb_utilities::get_broadcast_address()) {
                branches |= (1 << 1);
                auto is_mixed = [bf, rf] (const ep_info& e) { return 1.0f / (rf * bf) <= e.p; };
                float D = 0; // total deficit
                float Dtag = 0;
                for (auto&& ep : epi) {
                    // redistribute everything above 1/CL
                    if (ep.p < 1.0f / bf) {
                        ep.p += (ep.p * diffsum / restsum);
                    }
                    auto x = rf * ep.p - 1.0f / bf;
                    if (x >= 0) {
                        D += x;
                    } else {
                        Dtag += (1.0f - rf * ep.p);
                    }
                    log += sprint("%d: %.10f ", ep.ep, ep.p);
                }
                //cl_logger.debug("p after shoehorn (D={}, Dtag={}): {}", log, D, Dtag); log = "";
                log_message += sprint("p after shoehorn (D=%.10f, Dtag=%.10f): %s\n", D, Dtag, log); log = "";

                // Calculate sum in Dtag formula
                float Dtagsum = 0;
                for (auto&& e : epi) {
                    if (is_mixed(e)) {
                        // 1/(D - (NPi - 1/C))
                        Dtagsum += 1.0f / (D - (rf * e.p - 1.0f / bf));
                    }
                }
                //cl_logger.debug("Dtagsum={}", Dtagsum);
                log_message += sprint("%.10f\n", Dtagsum);

                auto p = epi[0].p;
                if (is_mixed(epi[0])) {
                    branches |= (1 << 2);
                    psum = epi[0].p = 1.0f / bf;
                    log += sprint("mixed %d: %.10f", epi[0].ep, epi[0].p);
                    for (auto i = std::next(epi.begin()); i != epi.end(); i++) {
                        if (is_mixed(*i)) {
                            // (1-1/C)(NPj-1/C)/(D-(NPi-1/C))
                            i->p = (1.0f - 1.0f / bf) * (rf * i->p - 1.0f / bf) / (D - (rf * p - 1.0f / bf));
                        } else {
                            i->p = 0.00001;
                        }
                        psum += i->p;
                        log += sprint(" %d: %.10f", i->ep, i->p);
                    }
                } else {
                    psum = epi[0].p = epi[0].p * rf;
                    log += sprint("not mixed %d: %.10f", epi[0].ep, epi[0].p);
                    for (auto i = std::next(epi.begin()); i != epi.end(); i++) {
                        if (is_mixed(*i)) {
                            auto x = (rf * i->p - 1.0f / bf);
                            auto Dtagj = isinf(Dtagsum) ? x : x * (1.0f - (1.0f - 1.0f / bf) * (Dtagsum - (1.0f / (D - x))));
                            // (1 - NPi)Dtagj/Dtag
                            i->p = (1.0f - rf * p) * Dtagj / Dtag;
                        } else {
                            i->p = 0.00001;
                        }
                        psum += i->p;
                        log += sprint( " %d: %.10f", i->ep, i->p);
                    }
                }
//                cl_logger.debug("final p (psum={}): {}", psum, log); log = "";
                log_message += sprint("final p (psum=%.10f): %s\n", psum, log); log = "";
            }

        } else {
            // local node is always first if present (see storage_proxy::get_live_sorted_endpoints)
            if (epi[0].ep == utils::fb_utilities::get_broadcast_address()) {
                use_endpoint(epi[0]); // always use local node
            }
        }
        if (prev_branches != branches) {
            cl_logger.error("new branch {}!={}\n{}", prev_branches, branches, log_message);
            prev_branches= branches;
        } else {
            if (cl_logger.is_enabled(logging::log_level::debug)) {
                thread_local static lowres_clock::time_point last(std::chrono::milliseconds(0));
                if (lowres_clock::now() - last > std::chrono::milliseconds(1000)) {
                    last = lowres_clock::now();
                    cl_logger.debug(log_message.c_str());
                }
            }
        }

        int xxx = 0;
        while (live_endpoints.size() != bf + bool(extra)) {
            if (xxx++ == 100) {
                cl_logger.error(log_message.c_str());
            }
            auto r = lbalance(random_engine) * psum;
            float s = 0;
            for (auto&& e : epi | boost::adaptors::filtered(std::mem_fn(&ep_info::p))) {
                s += e.p;
                if (s >= r) {
                    use_endpoint(e);
                    break;
                }
            }
        }
    }

    if (extra) {
        *extra = live_endpoints[bf]; // extra replica for speculation
    }

    live_endpoints.erase(live_endpoints.begin() + bf, live_endpoints.end());

    return std::move(live_endpoints);
}

std::vector<gms::inet_address> filter_for_query(consistency_level cl, keyspace& ks, std::vector<gms::inet_address>& live_endpoints, column_family* cf) {
    return filter_for_query(cl, ks, live_endpoints, read_repair_decision::NONE, nullptr, cf);
}

bool
is_sufficient_live_nodes(consistency_level cl,
                         keyspace& ks,
                         const std::vector<gms::inet_address>& live_endpoints) {
    using namespace locator;

    switch (cl) {
    case consistency_level::ANY:
        // local hint is acceptable, and local node is always live
        return true;
    case consistency_level::LOCAL_ONE:
        return count_local_endpoints(live_endpoints) >= 1;
    case consistency_level::LOCAL_QUORUM:
        return count_local_endpoints(live_endpoints) >= block_for(ks, cl);
    case consistency_level::EACH_QUORUM:
    {
        auto& rs = ks.get_replication_strategy();

        if (rs.get_type() == replication_strategy_type::network_topology) {
            for (auto& entry : count_per_dc_endpoints(ks, live_endpoints)) {
                if (entry.second.live < local_quorum_for(ks, entry.first)) {
                    return false;
                }
            }

            return true;
        }
    }
        // Fallthough on purpose for SimpleStrategy
    default:
        return live_endpoints.size() >= block_for(ks, cl);
    }
}

void validate_for_read(const sstring& keyspace_name, consistency_level cl) {
    switch (cl) {
        case consistency_level::ANY:
            throw exceptions::invalid_request_exception("ANY ConsistencyLevel is only supported for writes");
        case consistency_level::EACH_QUORUM:
            throw exceptions::invalid_request_exception("EACH_QUORUM ConsistencyLevel is only supported for writes");
        default:
            break;
    }
}

void validate_for_write(const sstring& keyspace_name, consistency_level cl) {
    switch (cl) {
        case consistency_level::SERIAL:
        case consistency_level::LOCAL_SERIAL:
            throw exceptions::invalid_request_exception("You must use conditional updates for serializable writes");
        default:
            break;
    }
}

#if 0
    // This is the same than validateForWrite really, but we include a slightly different error message for SERIAL/LOCAL_SERIAL
    public void validateForCasCommit(String keyspaceName) throws InvalidRequestException
    {
        switch (this)
        {
            case EACH_QUORUM:
                requireNetworkTopologyStrategy(keyspaceName);
                break;
            case SERIAL:
            case LOCAL_SERIAL:
                throw new InvalidRequestException(this + " is not supported as conditional update commit consistency. Use ANY if you mean \"make sure it is accepted but I don't care how many replicas commit it for non-SERIAL reads\"");
        }
    }

    public void validateForCas() throws InvalidRequestException
    {
        if (!isSerialConsistency())
            throw new InvalidRequestException("Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL");
    }
#endif

bool is_serial_consistency(consistency_level cl) {
    return cl == consistency_level::SERIAL || cl == consistency_level::LOCAL_SERIAL;
}

void validate_counter_for_write(schema_ptr s, consistency_level cl) {
    if (cl == consistency_level::ANY) {
        throw exceptions::invalid_request_exception(sprint("Consistency level ANY is not yet supported for counter table %s", s->cf_name()));
    }

    if (is_serial_consistency(cl)) {
        throw exceptions::invalid_request_exception("Counter operations are inherently non-serializable");
    }
}

#if 0
    private void requireNetworkTopologyStrategy(String keyspaceName) throws InvalidRequestException
    {
        AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
        if (!(strategy instanceof NetworkTopologyStrategy))
            throw new InvalidRequestException(String.format("consistency level %s not compatible with replication strategy (%s)", this, strategy.getClass().getName()));
    }
#endif

}
