#include "quack_activity.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"

#include "quack_startstop.hpp"
#include "quack_storage.hpp"

namespace duckdb {

static string QueryStateToString(QuackQueryState state) {
	switch (state) {
	case QuackQueryState::IDLE:
		return "idle";
	case QuackQueryState::ACTIVE:
		return "active";
	case QuackQueryState::FINISHED:
		return "finished";
	case QuackQueryState::CANCELLED:
		return "cancelled";
	default:
		return "unknown";
	}
}

struct QuackActivityData : FunctionData {
	bool finished = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackActivityData>();
		result->finished = finished;
		return result;
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

static unique_ptr<FunctionData> QuackActivityBind(ClientContext &, TableFunctionBindInput &,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::TIMESTAMP};
	names = {"server_id", "connection_id", "query", "state", "query_started_at"};
	return make_uniq<QuackActivityData>();
}

static void QuackActivityScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<QuackActivityData>();
	if (data.finished) {
		return;
	}

	auto snapshots = QuackStorageExtensionInfo::GetState(*context.db).GetActiveConnectionSnaps();

	idx_t row = 0;
	for (auto &snap : snapshots) {
		output.SetValue(0, row, snap.server_id);
		output.SetValue(1, row, snap.session_id);
		output.SetValue(2, row, snap.sql_query);
		output.SetValue(3, row, Value(QueryStateToString(snap.query_state)));
		if (snap.query_state == QuackQueryState::IDLE) {
			output.SetValue(4, row, Value(LogicalType::TIMESTAMP));
		} else {
			output.SetValue(4, row, Value::TIMESTAMP(snap.query_started_at));
		}
		row++;
	}
	output.SetChildCardinality(row);
	data.finished = true;
}

TableFunction QuacktivityFunction::GetFunction() {
	return TableFunction("quack_activity", {}, QuackActivityScan, QuackActivityBind);
}

} // namespace duckdb
