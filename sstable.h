#pragma once
#include <vector>

#include "bloomfilter.h"
#include "memtable.h"

struct SSInfo
{
    uint64_t timeStamp;
    uint64_t size;
    uint64_t minKey;
    uint64_t maxKey;
    SSInfo(uint64_t t, uint64_t s, uint64_t _min, uint64_t _max)
        : timeStamp(t), size(s), minKey(_min), maxKey(_max) {}
};


class SSTable
{
private:
    SSInfo *header;
    BloomFilter *bf;
    std::vector<std::pair<uint64_t, uint32_t>> dic;
    std::string file_path;
    
public:
    SSTable(SSInfo *h, BloomFilter *b, std::vector<std::pair<uint64_t, uint32_t>> &d, const std::string &p)
        : header(h), bf(b), dic(d), file_path(p) {}
    
    ~SSTable(){
        delete header;
        delete bf;
        dic.clear();
    }

    std::string get(uint64_t key);

    bool getOffSet(uint64_t key, uint32_t &offset, uint32_t &len);

};

