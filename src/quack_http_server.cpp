#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_uri.hpp"

#include "httplib.hpp"

namespace duckdb {

void HttpQuackServer::StopAccepting() {
	// Closes the listening socket only. Idempotent. Safe to call from a
	// request-handler thread.
	lock_guard<mutex> guard(state_lock);
	if (server_state == QuackServerState::RUNNING) {
		// server is running - stop it and decommission
		server->wait_until_ready();
		server->stop();
		server->decommission();
	}
	// close the server
	server_state = QuackServerState::CLOSED;
}

void HttpQuackServer::Close() {
	// Stops accepting new connections AND joins the listener threads (NOT the
	// httplib worker pool). Must not be called from a worker thread — the
	// listener's exit path inside httplib joins all workers, so a worker
	// joining the listener would deadlock through that chain.
	StopAccepting();
	for (auto &thread : listen_threads) {
		if (thread.joinable()) {
			thread.join();
		}
	}
}

HttpQuackServer::~HttpQuackServer() {
	try {
		HttpQuackServer::Close();
	} catch (std::exception &) {
	}
}

void HttpQuackServer::ListenThread(HttpQuackServer *server, const string &listen_host, uint16_t listen_port) {
	D_ASSERT(server->server);
	// Socket is already bound (synchronously, in the constructor).
	// Catch everything so the listener thread never lets an exception escape — that
	// would call std::terminate and abort the host process.
	{
		lock_guard<mutex> guard(server->state_lock);
		if (server->server_state != QuackServerState::WAITING_TO_START) {
			return;
		}
		server->server_state = QuackServerState::RUNNING;
	}
	try {
		server->server->listen_after_bind();
	} catch (...) {
		server->server_state = QuackServerState::CLOSED;
	}
}

HttpQuackServer::HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p)
    : QuackServer(context_p, uri_p, token_p) {
	server = make_uniq<duckdb_httplib::Server>();

	// Each keep-alive connection holds a server thread for its lifetime.
	// We need enough threads to handle all concurrent keep-alive connections
	// (catalog clients + scan thread clients) simultaneously, otherwise requests
	// from scan thread clients can deadlock waiting for threads held by the
	// catalog clients that are in turn waiting for the scan to complete.
	server->new_task_queue = [] {
		return new duckdb_httplib::ThreadPool(128);
	};
	server->set_keep_alive_max_count(128);
	server->set_keep_alive_timeout(10);
	server->set_tcp_nodelay(true);

	server->Get("/", [=](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		res.set_content("This is a DuckDB Quack RPC endpoint. Use ATTACH 'quack:...' to connect here.\n", "text/plain");
	});

	// TODO: this is very liberal, and there might be reasonable cases to restrict to trusted domains (note, this is
	// only relevant from within a Web browser, since other actors can just ignore the CORS convention
	server->Options("/quack", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		res.set_header("Access-Control-Allow-Origin", "*");
		res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
		res.set_header("Access-Control-Allow-Headers", "*");
		res.status = 204;
	});

	server->Post("/quack", [&](const duckdb_httplib::Request &, duckdb_httplib::Response &res,
	                           const duckdb_httplib::ContentReader &content_reader) {
		res.set_header("Access-Control-Allow-Origin", "*");
		MemoryStream stream;
		content_reader([&](const char *data, size_t data_length) {
			stream.WriteData((data_ptr_t)data, data_length);
			return true;
		});
		auto response = HandleMessage(stream);
		response->ToMemoryStream(stream);
		res.set_content((const char *)stream.GetData(), stream.GetPosition(), "application/vnd.duckdb");
	});

	if (!server->is_valid()) {
		throw IOException("Failed to instantiate DuckDB server at %s / %s", uri_p.Uri(), uri_p.Http());
	}

	bool success;
	if (uri_p.Port() == 0) {
		int actual_port = server->bind_to_any_port(uri_p.Host());
		success = actual_port >= 0;
		if (success) {
			auto bound_port = NumericCast<uint16_t>(actual_port);
			uri = QuackUri(uri_p, bound_port);
		}
	} else {
		success = server->bind_to_port(uri_p.Host(), uri_p.Port());
	}
	if (!success) {
		throw IOException("Failed to bind DuckDB Quack RPC server to %s (address in use, permission denied, "
		                  "or invalid host/port)",
		                  uri_p.Http());
	}

	server_state = QuackServerState::WAITING_TO_START;
	listen_threads.emplace_back(ListenThread, this, uri.Host(), uri.Port());
}

} // namespace duckdb
