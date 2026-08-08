#include "cli.h"

#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <initializer_list>
#include <limits>
#include <string_view>
#include <thread>

#include "context_fabric/api/context_fabric.h"
#include "context_fabric/authority/client.h"
#include "context_fabric/authority/coordinator.h"
#include "context_fabric/core/byte_size_util.h"
#include "context_fabric/core/prng.h"
#include "context_fabric/core/version.h"
#include "context_fabric/runtime/node_runtime.h"
#include "cli_args.h"
#include "fabric_launcher.h"

namespace ctf {

namespace fs = std::filesystem;

namespace {

std::string help_text() {
    std::string h =
        "Context Fabric " +
        std::string(kVersionString) +
        "\n"
        "\n"
        "Usage:\n"
        "  context-fabric --version\n"
        "  context-fabric help\n"
        "  context-fabric capabilities\n"
        "  context-fabric coordinator [--port N] [--store DIR] [--telemetry FILE]\n"
        "  context-fabric node --coordinator HOST:PORT --id NAME [--store DIR]\n"
        "      [--host-cap SIZE] [--persist-cap SIZE] [--accel-cap SIZE] [--cuda]\n"
        "      [--data-port N] [--telemetry FILE] [--label LABEL]\n"
        "  context-fabric fabric local [--nodes N] [--cuda-node] [--workload default]\n"
        "      [--retain] [--root DIR] [--coord-port N] [--size SIZE] [--quick]\n"
        "  context-fabric object create --coordinator HOST:PORT --namespace NS --name NAME\n"
        "      [--class CLASS] [--size SIZE|--bytes N] [--owner NODE] [--seed N]\n"
        "      [--persistence POLICY] [--sharing POLICY] [--mutable] [--lifetime SEC]\n"
        "      [--recompute-sec X] [--parent ID] [--derivation OP]\n"
        "  context-fabric object inspect --coordinator HOST:PORT --id ID\n"
        "  context-fabric object list --coordinator HOST:PORT [--namespace NS] [--json]\n"
        "  context-fabric object lineage --coordinator HOST:PORT --id ID\n"
        "  context-fabric object delete --coordinator HOST:PORT --id ID [--force]\n"
        "  context-fabric lease acquire --coordinator HOST:PORT --id ID --holder NODE\n"
        "      --mode read|write [--duration MS]\n"
        "  context-fabric lease release --coordinator HOST:PORT --id ID --holder NODE --lease-id N\n"
        "  context-fabric replicate --coordinator HOST:PORT --id ID --to NODE\n"
        "  context-fabric migrate --coordinator HOST:PORT --id ID --to NODE [--keep-source]\n"
        "      [--domain host|accelerator]\n"
        "  context-fabric persist --coordinator HOST:PORT --id ID [--on NODE]\n"
        "  context-fabric restore --coordinator HOST:PORT --id ID [--to NODE]\n"
        "  context-fabric verify --coordinator HOST:PORT --id ID [--on NODE] [--deep]\n"
        "  context-fabric placement --coordinator HOST:PORT --namespace NS --name NAME\n"
        "      --size SIZE [--current NODE] [--class CLASS]\n"
        "  context-fabric decide --coordinator HOST:PORT --namespace NS --name NAME --size SIZE\n"
        "      --current NODE --target NODE --recompute-sec X [--bandwidth BPS]\n"
        "  context-fabric benchmark [--quick] [--cuda] [--size SIZE]\n"
        "\n"
        "Sizes accept suffixes: B, KB, MB, GB, TB, KiB, MiB, GiB, TiB.\n";
    return h;
}

void print_help() { std::printf("%s", help_text().c_str()); }

bool validate_args(const Args& args, std::initializer_list<std::string_view> values,
                   std::initializer_list<std::string_view> flags = {}) {
    auto error = args.validate_remaining(values, flags);
    if (!error) return true;
    std::fprintf(stderr, "error: %s\n", error->c_str());
    return false;
}

bool parse_u64_option(const Args& args, const char* name, uint64_t& out, uint64_t min_value = 0,
                      uint64_t max_value = std::numeric_limits<uint64_t>::max()) {
    auto text = args.value(name);
    if (!text) return true;
    auto value = Args::parse_u64(*text);
    if (!value || *value < min_value || *value > max_value) {
        std::fprintf(stderr, "error: invalid --%s value '%s'\n", name, text->c_str());
        return false;
    }
    out = *value;
    return true;
}

bool parse_u16_option(const Args& args, const char* name, uint16_t& out) {
    uint64_t value = out;
    if (!parse_u64_option(args, name, value, 0, 65535)) return false;
    out = static_cast<uint16_t>(value);
    return true;
}

bool parse_node_ref(const std::string& text, uint64_t& out) {
    if (text.empty()) return false;
    bool decimal = true;
    for (char c : text) decimal = decimal && c >= '0' && c <= '9';
    if (decimal) {
        auto parsed = Args::parse_u64(text);
        if (!parsed || *parsed == 0) return false;
        out = *parsed;
        return true;
    }
    out = NodeId::deterministic(text).value();
    return out != 0;
}

std::optional<double> parse_finite_double(const std::string& text) {
    if (text.empty()) return std::nullopt;
    double value = 0.0;
    auto parsed = std::from_chars(text.data(), text.data() + text.size(), value, std::chars_format::general);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || !std::isfinite(value)) {
        return std::nullopt;
    }
    return value;
}

bool parse_coord(const Args& args, std::string& host, uint16_t& port) {
    auto c = args.value("coordinator");
    if (!c) {
        std::fprintf(stderr, "error: --coordinator HOST:PORT is required\n");
        return false;
    }
    auto colon = c->rfind(':');
    if (colon == std::string::npos) {
        std::fprintf(stderr, "error: --coordinator must be HOST:PORT\n");
        return false;
    }
    host = c->substr(0, colon);
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    auto p = Args::parse_u64(c->substr(colon + 1));
    if (host.empty() || !p || *p == 0 || *p > 65535) {
        std::fprintf(stderr, "error: invalid coordinator port\n");
        return false;
    }
    port = static_cast<uint16_t>(*p);
    return true;
}

ContextMetadata make_metadata(const Args& args, const std::string& ns, const std::string& name) {
    ContextMetadata md;
    md.namespace_id = ns;
    auto sc = args.value("class");
    if (sc) {
        auto parsed = semantic_class_from_name(*sc);
        if (!parsed) {
            throw CfError(ErrKind::InvalidArgument, "unknown class " + *sc);
        }
        md.semantic_class = *parsed;
    }
    auto size = args.value("size");
    if (size) {
        uint64_t bytes = 0;
        if (!parse_byte_size(*size, bytes) || bytes == 0) {
            throw CfError(ErrKind::InvalidArgument, "invalid --size " + *size);
        }
        md.byte_size = bytes;
        md.logical_size = bytes;
    }
    auto bytes_text = args.value("bytes");
    if (size && bytes_text) {
        throw CfError(ErrKind::InvalidArgument, "--size and --bytes are mutually exclusive");
    }
    if (bytes_text) {
        auto bytes = Args::parse_u64(*bytes_text);
        if (!bytes || *bytes == 0) throw CfError(ErrKind::InvalidArgument, "invalid --bytes " + *bytes_text);
        md.byte_size = *bytes;
        md.logical_size = *bytes;
    }
    auto pers = args.value("persistence");
    if (pers) {
        auto p = persistence_policy_from_name(*pers);
        if (!p) {
            throw CfError(ErrKind::InvalidArgument, "unknown persistence policy " + *pers);
        }
        md.persistence = *p;
    }
    auto sharing = args.value("sharing");
    if (sharing) {
        auto s = sharing_policy_from_name(*sharing);
        if (!s) {
            throw CfError(ErrKind::InvalidArgument, "unknown sharing policy " + *sharing);
        }
        md.sharing = *s;
    }
    md.mutable_flag = args.has("mutable");
    if (auto lifetime = args.value("lifetime")) {
        auto parsed = Args::parse_u64(*lifetime);
        if (!parsed) throw CfError(ErrKind::InvalidArgument, "invalid --lifetime " + *lifetime);
        md.lifetime_seconds = *parsed;
    }
    if (auto recompute = args.value("recompute-sec")) {
        auto parsed = parse_finite_double(*recompute);
        if (!parsed || *parsed < 0.0) {
            throw CfError(ErrKind::InvalidArgument, "invalid --recompute-sec " + *recompute);
        }
        md.recompute_seconds = *parsed;
    }
    if (auto reuse = args.value("reuse")) {
        auto parsed = parse_finite_double(*reuse);
        if (!parsed || *parsed < 0.0 || *parsed > 1.0) {
            throw CfError(ErrKind::InvalidArgument, "invalid --reuse " + *reuse);
        }
        md.reuse_probability = *parsed;
    }
    md.producer = "context-fabric-cli";
    md.model_id = name;
    md.architecture_namespace = ns;
    md.compatibility_fingerprint = md.compute_compatibility_fingerprint();
    return md;
}

int cmd_capabilities(const Args& args) {
    if (!validate_args(args, {})) return 2;
    std::printf("Context Fabric %s\n", kVersionString);
    std::printf("protocol-version: %u\n", kProtocolVersion);
    std::printf("persist-format: %u\n", kPersistFormatVersion);
    std::printf("coordinator-format: %u\n", kCoordinatorFormatVersion);
    std::printf("telemetry-schema: %u\n", kTelemetrySchemaVersion);
    std::printf("storage-domains: host-memory, local-persistent\n");
#if defined(CONTEXT_FABRIC_HAS_CUDA)
    std::printf("optional-storage-domain: accelerator-local (requires a usable CUDA device)\n");
#endif
    std::printf("reserved-storage-domains: remote-memory, remote-persistent\n");
    std::printf("semantic-classes: generic, kv-cache, prefix-state, recurrent-state, tensor, embedding, retrieval-state, agent-state, tool-artifact, checkpoint, application-defined\n");
    std::printf("sharing-policies: private, namespace-shared, globally-shareable\n");
    std::printf("persistence-policies: volatile, ephemeral, persistent\n");
    std::printf("placement-policies: baseline, cost-aware\n");
#if defined(CONTEXT_FABRIC_HAS_CUDA)
    std::printf("cuda: enabled\n");
#else
    std::printf("cuda: not compiled in this build\n");
#endif
    return 0;
}

int cmd_coordinator(const Args& args) {
    if (!validate_args(args, {"port", "store", "telemetry"}, {"quiet"})) return 2;
    Coordinator::Config cfg;
    if (!parse_u16_option(args, "port", cfg.control_port)) return 2;
    auto store = args.value("store");
    if (store) cfg.store_path = *store;
    auto t = args.value("telemetry");
    if (t) cfg.telemetry_path = *t;
    Telemetry telemetry;
    telemetry.set_stderr_enabled(!args.has("quiet"));
    Coordinator coord(cfg, telemetry);
    if (!coord.start()) {
        std::fprintf(stderr, "error: cannot bind coordinator control port\n");
        return 1;
    }
    std::printf("coordinator listening on 127.0.0.1:%u epoch=%u pid=%llu\n",
                static_cast<unsigned>(coord.bound_port()), static_cast<unsigned>(coord.epoch()),
                static_cast<unsigned long long>(current_pid()));
    std::fflush(stdout);
    coord.run();
    coord.shutdown();
    return 0;
}

int cmd_node(const Args& args) {
    if (!validate_args(args,
                       {"coordinator", "id", "store", "host-cap", "persist-cap", "accel-cap",
                        "data-port", "telemetry", "label"},
                       {"cuda", "quiet"})) {
        return 2;
    }
    NodeConfig cfg;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    cfg.coord_host = host;
    cfg.coord_port = port;

    auto id = args.value("id");
    if (!id || id->empty()) {
        std::fprintf(stderr, "error: --id NAME is required\n");
        return 2;
    }
    cfg.node_id = NodeId::deterministic(*id);
    cfg.label = args.value("label").value_or(*id);

    auto store = args.value("store");
    if (store) {
        if (store->empty()) {
            std::fprintf(stderr, "error: --store must not be empty\n");
            return 2;
        }
        cfg.store_root = *store;
    }
    uint64_t host_cap = 0;
    if (auto s = args.value("host-cap")) {
        if (!parse_byte_size(*s, host_cap) || host_cap == 0) {
            std::fprintf(stderr, "error: invalid --host-cap\n");
            return 2;
        }
    }
    if (host_cap == 0) host_cap = 256ull * 1024 * 1024;
    cfg.host_capacity = host_cap;

    if (auto s = args.value("persist-cap")) {
        if (!store) {
            std::fprintf(stderr, "error: --persist-cap requires --store\n");
            return 2;
        }
        if (!parse_byte_size(*s, cfg.persistent_capacity) || cfg.persistent_capacity == 0) {
            std::fprintf(stderr, "error: invalid --persist-cap\n");
            return 2;
        }
    } else if (store) {
        cfg.persistent_capacity = 1ull * 1024 * 1024 * 1024;
    } else {
        cfg.persistent_capacity = 0;
    }

    if (auto s = args.value("accel-cap")) {
        if (!parse_byte_size(*s, cfg.accelerator_capacity) || cfg.accelerator_capacity == 0) {
            std::fprintf(stderr, "error: invalid --accel-cap\n");
            return 2;
        }
    } else {
        cfg.accelerator_capacity = 6ull * 1024 * 1024 * 1024;  // safe default
    }
    cfg.enable_cuda = args.has("cuda");
#if !defined(CONTEXT_FABRIC_HAS_CUDA)
    if (cfg.enable_cuda) {
        std::fprintf(stderr, "error: --cuda requested, but CUDA is not compiled into this build\n");
        return 2;
    }
#endif
    if (!parse_u16_option(args, "data-port", cfg.data_port)) return 2;
    auto t = args.value("telemetry");
    if (t) cfg.telemetry_path = *t;

    Telemetry telemetry;
    telemetry.set_stderr_enabled(!args.has("quiet"));
    NodeRuntime node(cfg, telemetry);
    if (!node.start()) {
        std::fprintf(stderr, "error: node failed to start\n");
        return 1;
    }
    if (cfg.enable_cuda && node.accelerator() == nullptr) {
        std::fprintf(stderr, "error: --cuda requested, but no usable CUDA device is available\n");
        node.shutdown();
        return 1;
    }
    std::printf("node %s (%s) data=%s host_cap=%llu pid=%llu\n", cfg.label.c_str(),
                cfg.node_id.to_string().c_str(), node.data_addr().to_string().c_str(),
                static_cast<unsigned long long>(cfg.host_capacity),
                static_cast<unsigned long long>(current_pid()));
    std::fflush(stdout);
    node.run();
    node.shutdown();
    return 0;
}

int cmd_fabric_local(const Args& args) {
    if (!validate_args(args, {"nodes", "workload", "root", "coord-port", "size"},
                       {"cuda-node", "retain", "quick"})) {
        return 2;
    }
    FabricLauncher::Config lcfg;
    uint64_t nodes = 3;
    if (!parse_u64_option(args, "nodes", nodes, 1, 64)) return 2;
    lcfg.node_count = static_cast<size_t>(nodes);
    lcfg.enable_cuda_node = args.has("cuda-node");
#if !defined(CONTEXT_FABRIC_HAS_CUDA)
    if (lcfg.enable_cuda_node) {
        std::fprintf(stderr, "error: --cuda-node requested, but CUDA is not compiled into this build\n");
        return 2;
    }
#endif
    auto wl = args.value("workload");
    if (wl) lcfg.workload = *wl;
    lcfg.retain = args.has("retain");
    auto root = args.value("root");
    if (root) {
        if (root->empty()) {
            std::fprintf(stderr, "error: --root must not be empty\n");
            return 2;
        }
        lcfg.root = *root;
    }
    uint16_t coord_port = 0;
    if (!parse_u16_option(args, "coord-port", coord_port)) return 2;
    lcfg.coord_port = coord_port;
    auto sizearg = args.value("size");
    if (sizearg && (!parse_byte_size(*sizearg, lcfg.workload_size) || lcfg.workload_size == 0)) {
        std::fprintf(stderr, "error: invalid --size\n");
        return 2;
    }
    if (args.has("quick")) lcfg.quick = true;

    FabricLauncher launcher;
    return launcher.run(lcfg);
}

int cmd_object_create(const Args& args) {
    if (!validate_args(args,
                       {"coordinator", "namespace", "name", "class", "size", "bytes", "owner",
                        "seed", "persistence", "sharing", "lifetime", "recompute-sec", "reuse",
                        "parent", "derivation"},
                       {"mutable"})) {
        return 2;
    }
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto ns = args.value("namespace");
    auto name = args.value("name");
    if (!ns || !name || ns->empty() || name->empty()) {
        std::fprintf(stderr, "error: --namespace and --name are required\n");
        return 2;
    }
    ContextMetadata md = make_metadata(args, *ns, *name);
    if (md.byte_size == 0) {
        std::fprintf(stderr, "error: --size or --bytes required\n");
        return 2;
    }
    auto owner = args.value("owner");
    uint64_t owner_node = 0;
    if (owner) {
        if (!parse_node_ref(*owner, owner_node)) {
            std::fprintf(stderr, "error: invalid --owner\n");
            return 2;
        }
    }
    if (owner_node == 0) {
        // Default: ask the coordinator for a placement.
        ContextFabric cf;
        if (!cf.connect(host, port)) {
            std::fprintf(stderr, "error: cannot connect to coordinator\n");
            return 1;
        }
        auto placement = cf.query_placement(md, 0, 0);
        owner_node = placement.location.node_id;
        std::fprintf(stderr, "placement: node %llu (%s)\n",
                     static_cast<unsigned long long>(owner_node), placement.reason.c_str());
    }

    uint64_t seed = 0;
    if (!parse_u64_option(args, "seed", seed)) return 2;
    auto content = generate_content(seed, 0, md.byte_size);

    ObjectId parent;
    auto parent_str = args.value("parent");
    if (parent_str) parent = ObjectId::from_hex(*parent_str);
    auto deriv = args.value("derivation").value_or("create");

    ContextFabric cf;
    if (!cf.connect(host, port)) {
        std::fprintf(stderr, "error: cannot connect to coordinator\n");
        return 1;
    }
    try {
        ObjectId id = cf.create_object(md, owner_node, content, parent, deriv);
        std::printf("%s\n", id.to_hex().c_str());
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s (%s)\n", e.what(), e.code_string().c_str());
        return 1;
    }
}

int cmd_object_inspect(const Args& args) {
    if (!validate_args(args, {"coordinator", "id"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    if (!idstr) {
        std::fprintf(stderr, "error: --id required\n");
        return 2;
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        auto r = cf.inspect(ObjectId::from_hex(*idstr));
        if (!r.has) {
            std::printf("object %s: not found\n", idstr->c_str());
            return 1;
        }
        std::printf("id: %s\n", r.object_id.to_hex().c_str());
        std::printf("generation: %s\n", r.generation.to_hex().c_str());
        std::printf("lease-generation: %llu\n", static_cast<unsigned long long>(r.lease_generation));
        std::printf("lifecycle: %s\n", lifecycle_state_name(static_cast<LifecycleState>(r.lifecycle)));
        std::printf("class: %s\n", semantic_class_name(r.metadata.semantic_class));
        std::printf("namespace: %s\n", r.metadata.namespace_id.c_str());
        std::printf("bytes: %llu\n", static_cast<unsigned long long>(r.metadata.byte_size));
        std::printf("sharing: %s\n", sharing_policy_name(r.metadata.sharing));
        std::printf("persistence: %s\n", persistence_policy_name(r.metadata.persistence));
        std::printf("mutable: %s\n", r.metadata.mutable_flag ? "yes" : "no");
        std::printf("authoritative: node=%llu domain=%s\n",
                    static_cast<unsigned long long>(r.authoritative_node),
                    storage_domain_name(static_cast<StorageDomain>(r.authoritative_domain)));
        std::printf("replicas: ");
        for (size_t i = 0; i < r.replica_nodes.size(); ++i) {
            std::printf("%s%llu", i ? "," : "", static_cast<unsigned long long>(r.replica_nodes[i]));
        }
        std::printf("\n");
        std::printf("persistent-nodes: ");
        for (size_t i = 0; i < r.persistent_nodes.size(); ++i) {
            std::printf("%s%llu", i ? "," : "", static_cast<unsigned long long>(r.persistent_nodes[i]));
        }
        std::printf("\n");
        std::printf("owner: %llu\n", static_cast<unsigned long long>(r.owner_node));
        std::printf("created-unix-ms: %llu\n", static_cast<unsigned long long>(r.created_unix_ms));
        std::printf("compatibility-fingerprint: %016llx\n",
                    static_cast<unsigned long long>(r.metadata.compatibility_fingerprint));
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_object_list(const Args& args) {
    if (!validate_args(args, {"coordinator", "namespace"}, {"json"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    auto ns = args.value("namespace").value_or("");
    auto r = cf.list(ns);
    bool json = args.has("json");
    if (json) {
        std::string out = "[";
        for (size_t i = 0; i < r.ids.size(); ++i) {
            if (i) out += ",";
            out += "\"" + r.ids[i].to_hex() + "\"";
        }
        out += "]\n";
        std::printf("%s", out.c_str());
    } else {
        for (const auto& id : r.ids) std::printf("%s\n", id.to_hex().c_str());
    }
    return 0;
}

int cmd_object_lineage(const Args& args) {
    if (!validate_args(args, {"coordinator", "id"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    if (!idstr) {
        std::fprintf(stderr, "error: --id required\n");
        return 2;
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    auto r = cf.lineage(ObjectId::from_hex(*idstr));
    if (r.lineage.empty()) {
        std::printf("no lineage recorded\n");
        return 0;
    }
    for (const auto& e : r.lineage) {
        std::printf("%s gen=%s op=%s producer=%s seq=%llu parent=%s\n",
                    e.object.to_hex().c_str(), e.generation.to_hex().c_str(), e.operation.c_str(),
                    e.producer.c_str(), static_cast<unsigned long long>(e.creation_sequence),
                    e.parent.valid() ? e.parent.to_hex().c_str() : "-");
    }
    return 0;
}

int cmd_object_delete(const Args& args) {
    if (!validate_args(args, {"coordinator", "id"}, {"force"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    if (!idstr) {
        std::fprintf(stderr, "error: --id required\n");
        return 2;
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        cf.remove(ObjectId::from_hex(*idstr), args.has("force"));
        std::printf("deleted\n");
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_lease_acquire(const Args& args) {
    if (!validate_args(args, {"coordinator", "id", "holder", "mode", "duration"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    auto holder = args.value("holder");
    auto mode = args.value("mode");
    if (!idstr || !holder || !mode) {
        std::fprintf(stderr, "error: --id, --holder, --mode required\n");
        return 2;
    }
    uint64_t holder_node = 0;
    if (!parse_node_ref(*holder, holder_node)) {
        std::fprintf(stderr, "error: invalid --holder\n");
        return 2;
    }
    if (*mode != "read" && *mode != "write") {
        std::fprintf(stderr, "error: --mode must be read or write\n");
        return 2;
    }
    LeaseMode lm = *mode == "write" ? LeaseMode::Write : LeaseMode::Read;
    uint64_t duration = 0;
    if (!parse_u64_option(args, "duration", duration)) return 2;

    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        Lease l = lm == LeaseMode::Write ? cf.acquire_write_lease(ObjectId::from_hex(*idstr), holder_node, duration)
                                         : cf.acquire_read_lease(ObjectId::from_hex(*idstr), holder_node, duration);
        std::printf("lease-id: %llu\n", static_cast<unsigned long long>(l.lease_id));
        std::printf("mode: %s\n", lease_mode_name(l.mode));
        std::printf("holder: %llu\n", static_cast<unsigned long long>(l.holder.value()));
        std::printf("epoch: %llu\n", static_cast<unsigned long long>(l.coordinator_epoch));
        std::printf("lease-generation: %llu\n", static_cast<unsigned long long>(l.lease_generation));
        std::printf("expires-unix-ms: %llu\n", static_cast<unsigned long long>(l.expires_unix_ms));
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_lease_release(const Args& args) {
    if (!validate_args(args, {"coordinator", "id", "holder", "lease-id"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    auto holder = args.value("holder");
    auto lid_text = args.value("lease-id");
    if (!idstr || !holder || !lid_text) {
        std::fprintf(stderr, "error: --id, --holder, --lease-id required\n");
        return 2;
    }
    uint64_t lease_id = 0;
    if (!parse_u64_option(args, "lease-id", lease_id, 1)) return 2;
    uint64_t holder_node = 0;
    if (!parse_node_ref(*holder, holder_node)) {
        std::fprintf(stderr, "error: invalid --holder\n");
        return 2;
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        cf.release_lease(ObjectId::from_hex(*idstr), holder_node, lease_id);
        std::printf("released\n");
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_replicate(const Args& args) {
    if (!validate_args(args, {"coordinator", "id", "to"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    auto to = args.value("to");
    if (!idstr || !to) {
        std::fprintf(stderr, "error: --id and --to required\n");
        return 2;
    }
    uint64_t target = 0;
    if (!parse_node_ref(*to, target)) {
        std::fprintf(stderr, "error: invalid --to\n");
        return 2;
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        auto loc = cf.replicate(ObjectId::from_hex(*idstr), target);
        std::printf("replicated to node %llu (%s)\n", static_cast<unsigned long long>(loc.node_id),
                    storage_domain_name(loc.domain));
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_migrate(const Args& args) {
    if (!validate_args(args, {"coordinator", "id", "to", "domain"}, {"keep-source"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    auto to = args.value("to");
    if (!idstr || !to) {
        std::fprintf(stderr, "error: --id and --to required\n");
        return 2;
    }
    uint64_t target = 0;
    if (!parse_node_ref(*to, target)) {
        std::fprintf(stderr, "error: invalid --to\n");
        return 2;
    }
    StorageDomain domain = StorageDomain::HostMemory;
    auto dom = args.value("domain");
    if (dom) {
        if (*dom == "accelerator") {
            domain = StorageDomain::AcceleratorLocal;
        } else if (*dom != "host") {
            std::fprintf(stderr, "error: --domain must be host or accelerator\n");
            return 2;
        }
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        auto loc = cf.migrate(ObjectId::from_hex(*idstr), target, domain, args.has("keep-source"));
        std::printf("migrated to node %llu (%s) keep-source=%s\n",
                    static_cast<unsigned long long>(loc.node_id), storage_domain_name(loc.domain),
                    args.has("keep-source") ? "yes" : "no");
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_persist(const Args& args) {
    if (!validate_args(args, {"coordinator", "id", "on"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    if (!idstr) {
        std::fprintf(stderr, "error: --id required\n");
        return 2;
    }
    uint64_t on_node = 0;
    if (auto o = args.value("on")) {
        if (!parse_node_ref(*o, on_node)) {
            std::fprintf(stderr, "error: invalid --on\n");
            return 2;
        }
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        uint64_t node = cf.persist(ObjectId::from_hex(*idstr), on_node);
        std::printf("persisted on node %llu\n", static_cast<unsigned long long>(node));
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_restore(const Args& args) {
    if (!validate_args(args, {"coordinator", "id", "to"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    if (!idstr) {
        std::fprintf(stderr, "error: --id required\n");
        return 2;
    }
    uint64_t to_node = 0;
    if (auto o = args.value("to")) {
        if (!parse_node_ref(*o, to_node)) {
            std::fprintf(stderr, "error: invalid --to\n");
            return 2;
        }
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        auto loc = cf.restore(ObjectId::from_hex(*idstr), to_node);
        std::printf("restored to node %llu (%s)\n", static_cast<unsigned long long>(loc.node_id),
                    storage_domain_name(loc.domain));
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_verify(const Args& args) {
    if (!validate_args(args, {"coordinator", "id", "on"}, {"deep"})) return 2;
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto idstr = args.value("id");
    if (!idstr) {
        std::fprintf(stderr, "error: --id required\n");
        return 2;
    }
    uint64_t on_node = 0;
    if (auto o = args.value("on")) {
        if (!parse_node_ref(*o, on_node)) {
            std::fprintf(stderr, "error: invalid --on\n");
            return 2;
        }
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        auto r = cf.verify(ObjectId::from_hex(*idstr), on_node, args.has("deep"));
        std::printf("verified: %s bytes=%llu crc32c=%016llx sha=%016llx duration_ms=%llu\n",
                    r.ok ? "yes" : "no", static_cast<unsigned long long>(r.bytes),
                    static_cast<unsigned long long>(r.crc32c), static_cast<unsigned long long>(r.sha),
                    static_cast<unsigned long long>(r.duration_ms));
        return r.ok ? 0 : 1;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_placement(const Args& args) {
    if (!validate_args(args,
                       {"coordinator", "namespace", "name", "class", "size", "bytes", "current",
                        "persistence", "sharing", "lifetime", "recompute-sec", "reuse"},
                       {"mutable"})) {
        return 2;
    }
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto ns = args.value("namespace");
    auto name = args.value("name");
    if (!ns || !name || ns->empty() || name->empty()) {
        std::fprintf(stderr, "error: --namespace and --name required\n");
        return 2;
    }
    ContextMetadata md = make_metadata(args, *ns, *name);
    if (md.byte_size == 0) {
        std::fprintf(stderr, "error: --size required\n");
        return 2;
    }
    uint64_t current = 0;
    if (auto o = args.value("current")) {
        if (!parse_node_ref(*o, current)) {
            std::fprintf(stderr, "error: invalid --current\n");
            return 2;
        }
    }
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        auto d = cf.query_placement(md, 0, current);
        std::printf("location: node=%llu domain=%s\n", static_cast<unsigned long long>(d.location.node_id),
                    storage_domain_name(d.location.domain));
        std::printf("score: %.4f\n", d.score);
        std::printf("policy: %s\n", d.policy.c_str());
        std::printf("reason: %s\n", d.reason.c_str());
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_decide(const Args& args) {
    if (!validate_args(args,
                       {"coordinator", "namespace", "name", "class", "size", "bytes", "current",
                        "target", "bandwidth", "persistence", "sharing", "lifetime", "recompute-sec",
                        "reuse"},
                       {"mutable"})) {
        return 2;
    }
    std::string host;
    uint16_t port = 0;
    if (!parse_coord(args, host, port)) return 2;
    auto ns = args.value("namespace");
    auto name = args.value("name");
    if (!ns || !name || ns->empty() || name->empty()) {
        std::fprintf(stderr, "error: --namespace and --name required\n");
        return 2;
    }
    ContextMetadata md = make_metadata(args, *ns, *name);
    if (md.byte_size == 0) {
        std::fprintf(stderr, "error: --size required\n");
        return 2;
    }
    auto current = args.value("current");
    auto target = args.value("target");
    if (!current || !target) {
        std::fprintf(stderr, "error: --current and --target required\n");
        return 2;
    }
    uint64_t cn = 0, tn = 0;
    if (!parse_node_ref(*current, cn) || !parse_node_ref(*target, tn)) {
        std::fprintf(stderr, "error: invalid --current or --target\n");
        return 2;
    }
    uint64_t bandwidth = 1000000000ull;
    if (!parse_u64_option(args, "bandwidth", bandwidth, 1)) return 2;
    double bw = static_cast<double>(bandwidth);
    ContextFabric cf;
    if (!cf.connect(host, port)) return 1;
    try {
        auto d = cf.decide_transfer_vs_recompute(md, cn, tn, bw);
        std::printf("decision: %s\n", d.chose_transfer ? "transfer" : "recompute");
        std::printf("score: %.4f\n", d.score);
        std::printf("transfer-estimate: %.4fs (%.0f bytes)\n", d.transfer.expected_seconds,
                    d.transfer.expected_bytes);
        std::printf("recompute-estimate: %.4fs\n", d.recompute.expected_seconds);
        std::printf("reason: %s\n", d.reason.c_str());
        return 0;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

int cmd_benchmark(const Args& args) {
    if (!validate_args(args, {"size"}, {"quick", "cuda"})) return 2;
    return run_benchmarks(args);
}

}  // namespace

int cli_main(int argc, char** argv) {
    Args args(argc, argv);
    std::string cmd = args.next();
    try {
    if (cmd.empty() || cmd == "help" || cmd == "--help" || cmd == "-h") {
        if (args.has_next()) {
            std::fprintf(stderr, "error: help does not accept additional arguments\n");
            return 2;
        }
        print_help();
        return 0;
    }
    if (cmd == "--version" || cmd == "version") {
        if (args.has_next()) {
            std::fprintf(stderr, "error: version does not accept additional arguments\n");
            return 2;
        }
        std::printf("context-fabric %s (protocol v%u)\n", kVersionString, kProtocolVersion);
        return 0;
    }
    if (cmd == "capabilities") return cmd_capabilities(args);
    if (cmd == "coordinator") return cmd_coordinator(args);
    if (cmd == "node") return cmd_node(args);
    if (cmd == "fabric") {
        std::string sub = args.next();
        if (sub == "local") return cmd_fabric_local(args);
        std::fprintf(stderr, "error: unknown fabric subcommand %s\n", sub.c_str());
        return 2;
    }
    if (cmd == "object") {
        std::string sub = args.next();
        if (sub == "create") return cmd_object_create(args);
        if (sub == "inspect") return cmd_object_inspect(args);
        if (sub == "list") return cmd_object_list(args);
        if (sub == "lineage") return cmd_object_lineage(args);
        if (sub == "delete") return cmd_object_delete(args);
        std::fprintf(stderr, "error: unknown object subcommand %s\n", sub.c_str());
        return 2;
    }
    if (cmd == "lease") {
        std::string sub = args.next();
        if (sub == "acquire") return cmd_lease_acquire(args);
        if (sub == "release") return cmd_lease_release(args);
        std::fprintf(stderr, "error: unknown lease subcommand %s\n", sub.c_str());
        return 2;
    }
    if (cmd == "replicate") return cmd_replicate(args);
    if (cmd == "migrate") return cmd_migrate(args);
    if (cmd == "persist") return cmd_persist(args);
    if (cmd == "restore") return cmd_restore(args);
    if (cmd == "verify") return cmd_verify(args);
    if (cmd == "placement") return cmd_placement(args);
    if (cmd == "decide") return cmd_decide(args);
    if (cmd == "benchmark") return cmd_benchmark(args);

    std::fprintf(stderr, "error: unknown command %s\n", cmd.c_str());
    print_help();
    return 2;
    } catch (const CfError& e) {
        std::fprintf(stderr, "error: %s (%s)\n", e.what(), e.code_string().c_str());
        return e.kind() == ErrKind::InvalidArgument ? 2 : 1;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}

}  // namespace ctf
