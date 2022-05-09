#pragma once
#include "MurmurHash3.h"

#define CAPACITY 10240

class BloomFilter
{
private:
    char *data;

public:
    BloomFilter();
    ~BloomFilter();
    void insert(uint64_t key);
    bool isFind(uint64_t key);
    char *returnData();

};

