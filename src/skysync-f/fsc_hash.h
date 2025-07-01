#ifndef FSC_HASH_H
#define FSC_HASH_H

#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include <list>
#include <unordered_map>
#include <mimalloc-2.1/mimalloc.h>
#include "fastcdc.h"

// Node for strong hash table - maps string hash to ol
struct ol_pair {
    std::string key;
    ol value;
};

struct WeakHashNode {
    uint32_t key;
    WeakHashNode* next;
    
    WeakHashNode(uint32_t k) : key(k), next(nullptr) {}
};

class WeakHashTable {
public:
    WeakHashTable(int size);
    ~WeakHashTable();
    
    void insert(uint32_t key);
    bool contains(uint32_t key);
    void clear();
    void destroy();

private:
    WeakHashNode** table;
    int table_size;
    int hash(uint32_t key) const;
};

class StrongHashTable {
public:
    StrongHashTable(int size);
    ~StrongHashTable();
    
    void insert(const std::string& key, const ol& value);
    bool contains(const std::string& key);
    ol find(const std::string& key);
    void clear();
    void destroy();

private:
    std::vector<std::unique_ptr<std::list<ol_pair>>> table;
    int table_size;
    int hash(const std::string& key) const;
};

#endif // FSC_HASH_H