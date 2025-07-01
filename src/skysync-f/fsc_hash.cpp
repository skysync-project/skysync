#include "fsc_hash.h"
#include <functional>
#include <algorithm>

// WeakHashTable implementation
WeakHashTable::WeakHashTable(int size) : table_size(size) {
    table = new WeakHashNode*[table_size]();
    for (int i = 0; i < table_size; ++i) {
        table[i] = nullptr;
    }
}

WeakHashTable::~WeakHashTable() {
    destroy();
}

int WeakHashTable::hash(uint32_t key) const {
    return key % table_size;
}

void WeakHashTable::insert(uint32_t key) {
    int hash_val = hash(key);
    
    // Check if key already exists
    WeakHashNode* current = table[hash_val];
    while (current != nullptr) {
        if (current->key == key) {
            return; // Key already exists
        }
        current = current->next;
    }
    
    // Insert new node at head
    WeakHashNode* new_node = new WeakHashNode(key);
    new_node->next = table[hash_val];
    table[hash_val] = new_node;
}

bool WeakHashTable::contains(uint32_t key) {
    int hash_val = hash(key);
    WeakHashNode* current = table[hash_val];
    
    while (current != nullptr) {
        if (current->key == key) {
            return true;
        }
        current = current->next;
    }
    
    return false;
}

void WeakHashTable::clear() {
    for (int i = 0; i < table_size; ++i) {
        WeakHashNode* entry = table[i];
        while (entry != nullptr) {
            WeakHashNode* prev = entry;
            entry = entry->next;
            delete prev;
        }
        table[i] = nullptr;
    }
}

void WeakHashTable::destroy() {
    if (table != nullptr) {
        clear();
        delete[] table;
        table = nullptr;
        table_size = 0;
    }
}

// StrongHashTable implementation
StrongHashTable::StrongHashTable(int size) : table_size(size) {
    table.resize(table_size);
}

StrongHashTable::~StrongHashTable() {
    destroy();
}

int StrongHashTable::hash(const std::string& key) const {
    std::hash<std::string> hasher;
    return hasher(key) % table_size;
}

void StrongHashTable::insert(const std::string& key, const ol& value) {
    int hash_val = hash(key);
    if (!table[hash_val]) {
        table[hash_val] = std::make_unique<std::list<ol_pair>>();
    }

    for (const auto& pair : *table[hash_val]) {
        if (pair.key == key) {
            return;
        }
    }

    table[hash_val]->push_back({key, value});
}

bool StrongHashTable::contains(const std::string& key) {
    int hash_val = hash(key);
    if (table[hash_val]) {
        for (const auto& pair : *table[hash_val]) {
            if (pair.key == key) {
                return true;
            }
        }
    }
    return false;
}

ol StrongHashTable::find(const std::string& key) {
    int hash_val = hash(key);
    if (table[hash_val]) {
        for (const auto& pair : *table[hash_val]) {
            if (pair.key == key) {
                return pair.value;
            }
        }
    }
    return ol{0, 0};
}

void StrongHashTable::clear() {
    table.clear();
    table.resize(table_size);
}

void StrongHashTable::destroy() {
    table.clear();
    table_size = 0;
}