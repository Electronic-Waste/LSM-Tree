#ifndef LSM_TREE_BLOOMFILTER_H
#define LSM_TREE_BLOOMFILTER_H


#pragma once
#include "MurmurHash3.h"

#define CAPACITY 10240

class BloomFilter
{
private:
    char *data;

public:
    BloomFilter();
    BloomFilter(char *bf);
    ~BloomFilter();
    void insert(uint64_t key);
    bool isFind(uint64_t key);
    char *returnData();

};




#endif //LSM_TREE_BLOOMFILTER_H
