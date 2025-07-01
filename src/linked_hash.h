#ifndef LINKED_HASH_H
#define LINKED_HASH_H

#include <cstdint>
#include <vector>

#include "fastcdc.h"

// Node for the linked list in each hash table bucket.
struct LinkedHashNode {
    uint32_t key;
    ol value;
    LinkedHashNode* next;
    
    LinkedHashNode(uint32_t k, const ol& val) : key(k), value(val), next(nullptr) {}
};

class LinkedHashTable {
public:
    // Constructor to create and initialize the hash table.
    LinkedHashTable(int size);

    // Destructor to free all allocated memory.
    ~LinkedHashTable();

    // Inserts a new ol value for a given key.
    void insert(uint32_t key, const ol& value);

    // Finds all ol values for a given key and returns them as a vector.
    std::vector<ol> find(uint32_t key);
    
    // Checks if a key exists in the hash table.
    bool contains(uint32_t key);

    // Removes all entries from the hash table.
    void clear();

    // Frees all allocated memory.
    void destroy();

private:
    LinkedHashNode** table;
    int table_size;

    // Hash function.
    int hash(uint32_t key) const;
};

#endif // LINKED_HASH_H