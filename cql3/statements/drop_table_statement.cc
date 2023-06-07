/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_table_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "mutation/mutation.hh"
#include "cdc/log.hh"

namespace cql3 {

namespace statements {

drop_table_statement::drop_table_statement(cf_name cf_name, bool if_exists)
    : schema_altering_statement{std::move(cf_name), &timeout_config::truncate_timeout}
    , _if_exists{if_exists}
{
}

future<> drop_table_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    // invalid_request_exception is only thrown synchronously.
    try {
        return state.has_column_family_access(qp.db(), keyspace(), column_family(), auth::permission::DROP);
    } catch (exceptions::invalid_request_exception&) {
        if (!_if_exists) {
            throw;
        }
        return make_ready_future();
    }
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_table_statement::prepare_schema_mutations(query_processor& qp, api::timestamp_type ts) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;
    std::vector<mutation> m;

    if (cdc::is_log_for_some_table(qp.db().real_database(), keyspace(), column_family())) {
        // we should really throw invalid_request_exception but for legacy reasons
        // with throw unauthorized_exception here
        co_return coroutine::exception(std::make_exception_ptr(exceptions::unauthorized_exception(
                   format("Cannot DROP cdc log table {}", column_family()))));
    }

    try {
        m = co_await qp.get_migration_manager().prepare_column_family_drop_announcement(keyspace(), column_family(), ts);

        using namespace cql_transport;
        ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::DROPPED,
                event::schema_change::target_type::TABLE,
                this->keyspace(),
                this->column_family());
    } catch (const exceptions::configuration_exception& e) {
        if (!_if_exists) {
            co_return coroutine::exception(std::current_exception());
        }
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_table_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_table_statement>(*this));
}

}

}
