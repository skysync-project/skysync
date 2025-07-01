#ifndef DSYNC_HTTP_H
#define DSYNC_HTTP_H

#include <vector>
#include <string>
#include "dsync.pb.h"  // Generated protobuf header
#include "dsync.h"
#include "sync_common.h"

// Protobuf serialization functions for dsync data structures

// Protobuf serialization for DataQueue<uint32_t>
dsync::Uint32Queue serialize_uint32_queue_to_protobuf(DataQueue<uint32_t>& queue);
void deserialize_uint32_queue_from_protobuf(const dsync::Uint32Queue& pb_queue, DataQueue<uint32_t>& queue);

// Protobuf serialization for DataQueue<matched_item_rpc_1>
dsync::MatchedItemRpc1Queue serialize_matched_item_rpc_1_queue_to_protobuf(DataQueue<matched_item_rpc_1>& queue);
void deserialize_matched_item_rpc_1_queue_from_protobuf(const dsync::MatchedItemRpc1Queue& pb_queue, DataQueue<matched_item_rpc_1>& queue);

// Protobuf serialization for DataQueue<data_cmd>
dsync::DataCmdQueue serialize_data_cmd_queue_to_protobuf(DataQueue<data_cmd>& queue);
void deserialize_data_cmd_queue_from_protobuf(const dsync::DataCmdQueue& pb_queue, DataQueue<data_cmd>& queue);

// Helper conversion functions for individual structs
dsync::OffsetLength ol_to_protobuf(const ol& o);
ol protobuf_to_ol(const dsync::OffsetLength& pb_ol);

dsync::MatchedItemRpc1 matched_item_rpc_1_to_protobuf(const matched_item_rpc_1& item);
matched_item_rpc_1 protobuf_to_matched_item_rpc_1(const dsync::MatchedItemRpc1& pb_item);

dsync::DataCmd data_cmd_to_protobuf(const data_cmd& cmd);
data_cmd protobuf_to_data_cmd(const dsync::DataCmd& pb_cmd);

// Implementation of protobuf serialization functions

// Helper conversion functions for individual structs
inline dsync::OffsetLength ol_to_protobuf(const ol& o) {
    dsync::OffsetLength pb_ol;
    pb_ol.set_offset(o.offset);
    pb_ol.set_length(o.length);
    return pb_ol;
}

inline ol protobuf_to_ol(const dsync::OffsetLength& pb_ol) {
    ol o;
    o.offset = pb_ol.offset();
    o.length = pb_ol.length();
    return o;
}

inline dsync::MatchedItemRpc1 matched_item_rpc_1_to_protobuf(const matched_item_rpc_1& item) {
    dsync::MatchedItemRpc1 pb_item;
    pb_item.set_item_nums(item.item_nums);
    pb_item.set_weak_hash(item.weak_hash);
    pb_item.set_end_of_stream(item.end_of_stream);
    
    // Convert the SHA hash to chunk map
    for (const auto& pair : item.sha_to_chunk_map) {
        dsync::ShaToChunkEntry* entry = pb_item.add_sha_to_chunk_map();
        entry->set_sha_hash(pair.first);  // Binary data is natively supported in protobuf
        *entry->mutable_chunk() = ol_to_protobuf(pair.second);
    }
    
    return pb_item;
}

inline matched_item_rpc_1 protobuf_to_matched_item_rpc_1(const dsync::MatchedItemRpc1& pb_item) {
    matched_item_rpc_1 item;
    item.item_nums = pb_item.item_nums();
    item.weak_hash = pb_item.weak_hash();
    item.end_of_stream = pb_item.end_of_stream();
    
    // Convert the SHA hash to chunk map
    item.sha_to_chunk_map.clear();
    for (const auto& entry : pb_item.sha_to_chunk_map()) {
        std::string sha_hash = entry.sha_hash();
        ol chunk_info = protobuf_to_ol(entry.chunk());
        item.sha_to_chunk_map[sha_hash] = chunk_info;
    }
    
    return item;
}

inline dsync::DataCmd data_cmd_to_protobuf(const data_cmd& cmd) {
    dsync::DataCmd pb_cmd;
    
    // Set command type
    if (cmd.cmd == 0) {
        pb_cmd.set_cmd(dsync::DataCmd::COPY);
    } else {
        pb_cmd.set_cmd(dsync::DataCmd::LITERAL);
    }
    
    pb_cmd.set_offset(cmd.offset);
    pb_cmd.set_length(cmd.length);
    pb_cmd.set_end_of_stream(cmd.end_of_stream);
    
    // Handle binary data - protobuf natively supports binary data
    if (cmd.data && cmd.length > 0) {
        pb_cmd.set_data(cmd.data, cmd.length);
    }
    
    return pb_cmd;
}

inline data_cmd protobuf_to_data_cmd(const dsync::DataCmd& pb_cmd) {
    data_cmd cmd;
    
    // Convert command type
    cmd.cmd = (pb_cmd.cmd() == dsync::DataCmd::COPY) ? 0 : 1;
    cmd.offset = pb_cmd.offset();
    cmd.length = pb_cmd.length();
    cmd.end_of_stream = pb_cmd.end_of_stream();
    
    // Handle binary data
    if (!pb_cmd.data().empty()) {
        const std::string& data_str = pb_cmd.data();
        cmd.data = (uint8_t*)mi_malloc(data_str.size());
        if (cmd.data) {
            memcpy(cmd.data, data_str.data(), data_str.size());
            cmd.length = data_str.size();
        }
    } else {
        cmd.data = nullptr;
    }
    
    return cmd;
}

// Queue serialization functions
inline dsync::Uint32Queue serialize_uint32_queue_to_protobuf(DataQueue<uint32_t>& queue) {
    dsync::Uint32Queue pb_queue;
    
    // Create a temporary vector to preserve original queue
    std::vector<uint32_t> items;
    while (!queue.empty()) {
        items.push_back(queue.pop());
    }
    
    // Add items to protobuf and restore queue
    for (const auto& item : items) {
        pb_queue.add_weak_hashes(item);
        queue.push(item);
    }
    
    queue.setDone();
    return pb_queue;
}

inline void deserialize_uint32_queue_from_protobuf(const dsync::Uint32Queue& pb_queue, DataQueue<uint32_t>& queue) {
    queue.init(); // Clear the queue
    
    for (const auto& weak_hash : pb_queue.weak_hashes()) {
        queue.push(weak_hash);
    }
    
    queue.setDone();
}

inline dsync::MatchedItemRpc1Queue serialize_matched_item_rpc_1_queue_to_protobuf(DataQueue<matched_item_rpc_1>& queue) {
    dsync::MatchedItemRpc1Queue pb_queue;
    
    // Create a temporary vector to preserve original queue
    std::vector<matched_item_rpc_1> items;
    while (!queue.empty()) {
        items.push_back(queue.pop());
    }
    
    // Add items to protobuf and restore queue
    for (const auto& item : items) {
        *pb_queue.add_items() = matched_item_rpc_1_to_protobuf(item);
        queue.push(item);
    }
    
    queue.setDone();
    return pb_queue;
}

inline void deserialize_matched_item_rpc_1_queue_from_protobuf(const dsync::MatchedItemRpc1Queue& pb_queue, DataQueue<matched_item_rpc_1>& queue) {
    queue.init(); // Clear the queue
    
    for (const auto& pb_item : pb_queue.items()) {
        matched_item_rpc_1 item = protobuf_to_matched_item_rpc_1(pb_item);
        queue.push(item);
    }
    
    queue.setDone();
}

inline dsync::DataCmdQueue serialize_data_cmd_queue_to_protobuf(DataQueue<data_cmd>& queue) {
    dsync::DataCmdQueue pb_queue;
    
    // Create a temporary vector to preserve original queue
    std::vector<data_cmd> items;
    while (!queue.empty()) {
        items.push_back(queue.pop());
    }
    
    // Add items to protobuf and restore queue
    for (const auto& item : items) {
        *pb_queue.add_commands() = data_cmd_to_protobuf(item);
        queue.push(item);
    }
    
    queue.setDone();
    return pb_queue;
}

inline void deserialize_data_cmd_queue_from_protobuf(const dsync::DataCmdQueue& pb_queue, DataQueue<data_cmd>& queue) {
    queue.init(); // Clear the queue
    
    for (const auto& pb_cmd : pb_queue.commands()) {
        data_cmd cmd = protobuf_to_data_cmd(pb_cmd);
        queue.push(cmd);
    }
    
    queue.setDone();
}

#endif // DSYNC_HTTP_H