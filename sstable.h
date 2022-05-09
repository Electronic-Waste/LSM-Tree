#pragma once
#include <vector>

#include "bloomfilter.h"

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
    SSTable(SSInfo *h, BloomFilter *b, const std::vector<std::pair<uint64_t, uint32_t>> &d, const std::string &p)
        : header(h), bf(b), file_path(p) {
            uint64_t size = d.size();
            for (uint64_t i = 0; i < size; ++i) {
                uint64_t key = d[i].first;
                uint32_t offset = d[i].second;
                dic.push_back(std::pair<uint64_t, uint32_t>(key, offset));
            }
        }
    
    ~SSTable(){
        delete header;
        delete bf;
        dic.clear();
    }

    std::string get(uint64_t key);

    bool getOffSet(uint64_t key, uint32_t &offset, uint32_t &len);

    void reset();

    std::string returnPath(){return file_path;}
};

