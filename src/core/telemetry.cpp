#include "context_fabric/core/telemetry.h"

#include <iostream>
#include <chrono>

#include "context_fabric/core/clock.h"
#include "context_fabric/core/version.h"

namespace ctf {

Telemetry::~Telemetry() { close(); }

void Telemetry::open(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (opened_) return;
    path_ = path;
    if (!path_.empty()) {
        file_.open(path_, std::ios::out | std::ios::trunc);
        opened_ = file_.is_open();
        if (!opened_) {
            std::cerr << "telemetry: cannot open " << path_ << "; falling back to stderr\n";
        }
    }
}

void Telemetry::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (file_.is_open()) file_.close();
    opened_ = false;
}

bool Telemetry::is_open() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return opened_;
}

void Telemetry::set_node_id(uint64_t node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    node_id_ = node_id;
}

void Telemetry::set_coordinator_epoch(uint64_t epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    epoch_ = epoch;
}

void Telemetry::set_stderr_enabled(bool on) {
    std::lock_guard<std::mutex> lock(mutex_);
    stderr_enabled_ = on;
}

void Telemetry::emit(const std::string& event, const JsonObject& doc) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string line;
    JsonObject env;
    env.field("schema", static_cast<uint64_t>(kTelemetrySchemaVersion));
    env.field("seq", ++sequence_);
    env.field("t_unix_ms", now_unix_ms());
    env.field("node", node_id_);
    env.field("epoch", epoch_);
    line += "{\"event\":";
    line += json_escape_string(event);
    line += ",\"envelope\":";
    std::string envelope;
    env.write_to(envelope);
    line += envelope;
    line += ",\"doc\":";
    std::string doc_str;
    doc.write_to(doc_str);
    line += doc_str;
    line += "}\n";

    if (file_.is_open()) file_ << line;
    if (stderr_enabled_) std::cerr << line;
}

void Telemetry::emit_msg(const std::string& event, const std::string& message) {
    JsonObject doc;
    doc.field("message", message);
    emit(event, doc);
}

}  // namespace ctf
