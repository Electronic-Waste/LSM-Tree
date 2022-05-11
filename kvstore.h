#ifndef LSM_TREE_KVSTORE_H
#define LSM_TREE_KVSTORE_H


#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"

/**
 * @param NORMALLY Read all K-V pairs
 * @param RMDELETE Read all K-V pairs except for nodes that V = "~DELETE~"
 */
enum KVReadMode
{
    NORMALLY = 1,
    RMDELETE
};

struct KVArray {
    std::vector<std::pair<uint64_t, std::string>> KVCache;     //K-V pairs in SSTable
    uint64_t cacheSize;                             //The number of K-V pairs
    uint64_t cachePos;                              //The pos of index (used for sort)
    uint64_t timeStamp;                             //The timeStamp of cache
    bool isOverFlow;                                //If cachePos = cacheSize, overflow.
    KVReadMode mode;                                //The read mode we take
    KVArray(SSTable *st, KVReadMode _mode) {
        cacheSize = st->returnHeader()->size;
        timeStamp = st->returnHeader()->timeStamp;
        isOverFlow = false;
        cachePos = 0;
        mode = _mode;
        /* Initialize KVCache */
        std::vector<uint64_t> keyArr;               //Keys in the dic of SSTable
        st->getDicKey(keyArr);
        for (int i = 0; i < cacheSize; ++i) {
            uint64_t key = keyArr[i];
            std::string val = st->get(key);
            if (mode == KVReadMode::RMDELETE && val == "~DELETE~") continue;
            else KVCache.push_back(std::pair<uint64_t, std::string>(key, val));
        }
    }
};

struct KWayNode {
    uint64_t KWayArrayIndex;
    uint64_t timeStamp;
    std::pair<uint64_t, std::string> KVNode;
    KWayNode(uint64_t _index, uint64_t _stamp, const std::pair<uint64_t, std::string> &_node) {
        KWayArrayIndex = _index;
        timeStamp = _stamp;
        uint64_t key = _node.first;
        std::string val = _node.second;
        KVNode = std::pair<uint64_t, std::string>(key, val);
    }
};

class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
    MemTable *mem;

    std::vector<SSTable *> SSVec;

    uint64_t maxTimeStamp;

    std::string dataDir;

    bool isOverflow(uint64_t key, const std::string &str);
public:
    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

    bool isToCompact();

    void compact();

    void kwayCombine(std::vector<KVArray *> &Arr, const std::string &dirPath);

    void display();
};



#endif //LSM_TREE_KVSTORE_H
