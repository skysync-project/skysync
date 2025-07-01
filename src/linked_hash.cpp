#include "linked_hash.h"
#include <cstddef>

// Constructor
LinkedHashTable::LinkedHashTable(int size) : table_size(size) {
    table = new LinkedHashNode*[table_size]();
    for (int i = 0; i < table_size; ++i) {
        table[i] = nullptr; // Initialize each bucket to nullptr
    }
}

// Destructor
LinkedHashTable::~LinkedHashTable() {
    destroy();
}

// Hash function
int LinkedHashTable::hash(uint32_t key) const {
    // return key % table_size;
    return std::hash<uint32_t>()(key) % table_size; // Use std::hash for better distribution
}

// Inserts a new ol value for a given key.
void LinkedHashTable::insert(uint32_t key, const ol& value) {
    int hash_val = hash(key);
    LinkedHashNode* new_node = new LinkedHashNode(key, value);
    
    // Insert at head for better performance
    new_node->next = table[hash_val];
    table[hash_val] = new_node;
}

// Finds the linked list of ol values for a given key.
std::vector<ol> LinkedHashTable::find(uint32_t key) {
    std::vector<ol> result;
    int hash_val = hash(key);
    LinkedHashNode* current = table[hash_val];
    
    while (current != nullptr) {
        if (current->key == key) {
            result.push_back(current->value);
        }
        current = current->next;
    }
    
    return result;
}

bool LinkedHashTable::contains(uint32_t key) {
    int hash_val = hash(key);
    LinkedHashNode* current = table[hash_val];
    
    while (current != nullptr) {
        if (current->key == key) {
            return true;
        }
        current = current->next;
    }
    
    return false;
}

// Removes all entries from the hash table.
void LinkedHashTable::clear() {
    for (int i = 0; i < table_size; ++i) {
        LinkedHashNode* entry = table[i];
        while (entry != nullptr) {
            LinkedHashNode* prev = entry;
            entry = entry->next;
            delete prev;
        }
        table[i] = nullptr;
    }
}

// Frees all allocated memory.
void LinkedHashTable::destroy() {
    if (table != nullptr) {
        clear();
        delete[] table;
        table = nullptr;
        table_size = 0;
    }
}