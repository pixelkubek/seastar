/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2022 ScyllaDB
 */

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/rpc/rpc.hh>
#include <seastar/core/sleep.hh>
#include <iostream>

using namespace seastar;
using namespace std::chrono_literals;

struct serializer {};

template <typename T, typename Output>
inline
void write_arithmetic_type(Output& out, T v) {
    static_assert(std::is_arithmetic_v<T>, "must be arithmetic type");
    return out.write(reinterpret_cast<const char*>(&v), sizeof(T));
}

template <typename T, typename Input>
inline
T read_arithmetic_type(Input& in) {
    static_assert(std::is_arithmetic_v<T>, "must be arithmetic type");
    T v;
    in.read(reinterpret_cast<char*>(&v), sizeof(T));
    return v;
}

template <typename Output>
inline void write(serializer, Output& output, int32_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, uint32_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, int64_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, uint64_t v) { return write_arithmetic_type(output, v); }
template <typename Output>
inline void write(serializer, Output& output, double v) { return write_arithmetic_type(output, v); }
template <typename Input>
inline int32_t read(serializer, Input& input, rpc::type<int32_t>) { return read_arithmetic_type<int32_t>(input); }
template <typename Input>
inline uint32_t read(serializer, Input& input, rpc::type<uint32_t>) { return read_arithmetic_type<uint32_t>(input); }
template <typename Input>
inline uint64_t read(serializer, Input& input, rpc::type<uint64_t>) { return read_arithmetic_type<uint64_t>(input); }
template <typename Input>
inline uint64_t read(serializer, Input& input, rpc::type<int64_t>) { return read_arithmetic_type<int64_t>(input); }
template <typename Input>
inline double read(serializer, Input& input, rpc::type<double>) { return read_arithmetic_type<double>(input); }

template <typename Output>
inline void write(serializer, Output& out, const sstring& v) {
    write_arithmetic_type(out, uint32_t(v.size()));
    out.write(v.c_str(), v.size());
}

template <typename Input>
inline sstring read(serializer, Input& in, rpc::type<sstring>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    sstring ret = uninitialized_string(size);
    in.read(ret.data(), size);
    return ret;
}

using payload_t = std::vector<uint64_t>;

template <typename Output>
inline void write(serializer, Output& out, const payload_t& v) {
    write_arithmetic_type(out, uint32_t(v.size()));
    out.write((const char*)v.data(), v.size() * sizeof(payload_t::value_type));
}

template <typename Input>
inline payload_t read(serializer, Input& in, rpc::type<payload_t>) {
    auto size = read_arithmetic_type<uint32_t>(in);
    payload_t ret;
    ret.resize(size);
    in.read((char*)ret.data(), size * sizeof(payload_t::value_type));
    return ret;
}

using payload_t = std::vector<uint64_t>;

enum rpc_verb { STREAM_INTS };

using rpc_protocol = rpc::protocol<serializer, rpc_verb>;

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("server", "run as server")
        ("client", "run as client")
        ("port", boost::program_options::value<int>()->default_value(12345), "port")
        ("addr", boost::program_options::value<std::string>()->default_value("127.0.0.1"), "address");

    return app.run(ac, av, [&] {
        return seastar::async([&] {
            auto& opts = app.configuration();
            bool is_server = opts.count("server");
            bool is_client = opts.count("client");
            if (is_server == is_client) {
                std::cout << "Specify either --server or --client" << std::endl;
                return 0;
            }

            int port = opts["port"].as<int>();
            std::string addr = opts["addr"].as<std::string>();
            rpc_protocol proto(serializer{});

            if (is_server) {
                promise<> _bye;
                proto.register_handler(rpc_verb::STREAM_INTS, [&_bye] (rpc::source<payload_t> source) -> seastar::future<> {
                    while (true) {
                        try{
                            auto data = co_await source();
                            if (!data) {
                                break;
                            }
                            auto [payload] = *data;
                            std::cout << "Received payload of size: " << payload.size() << std::endl;
                        } catch (const std::exception& e) {
                            std::cout << "Exception in stream source: " << e.what() << std::endl;
                            break;
                        }
                    }
                    _bye.set_value();
                    co_return;
                });
                ipv4_addr listen_addr(addr, port);
                rpc::server_options so;
                rpc::resource_limits limits;
                auto server = std::make_unique<rpc_protocol::server>(proto, so, listen_addr, limits);
                _bye.get_future().get();
                server->stop().get();
                return 0;
            } else {
                ipv4_addr connect_addr(addr, port);
                rpc::client_options co;
                auto client = std::make_unique<rpc_protocol::client>(proto, co, connect_addr);
                auto stream = client->make_stream_sink<serializer, payload_t>().get();
                auto call = proto.make_client<void(rpc::sink<payload_t>)>(rpc_verb::STREAM_INTS);
                call(*client, stream).get();

                payload_t payload;
                payload.resize(10);
                std::iota(payload.begin(), payload.end(), 5);

                for (int i = 0; i < 10; ++i) {
                    stream(payload).get();
                    seastar::sleep(100ms).get();
                }

                fmt::print("Client done sending\n");
                seastar::sleep(1s).get();
                fmt::print("Client exiting\n");
                client->stop().get();
                fmt::print("Client stopped\n");
                return 0;
            }
        });
    });
}