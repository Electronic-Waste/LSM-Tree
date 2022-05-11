#include <cstring>

#include "bloomfilter.h"

BloomFilter::BloomFilter()
{
    data = new char[CAPACITY];
    for (int i = 0; i < CAPACITY; ++i)
        data[i] = '0';
}

BloomFilter::BloomFilter(char *bf)
{
    data = new char[CAPACITY];
    strcpy(data, bf);
}

BloomFilter::~BloomFilter()
{
    delete data;
}

void BloomFilter::insert(uint64_t key)
{
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(uint64_t), 1, hash);
    for (int i = 0; i < 4; ++i)
        data[hash[i] % CAPACITY] = '1';
}

bool BloomFilter::isFind(uint64_t key)
{
    bool flag = true;
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(uint64_t), 1, hash);
    for (int i = 0; i < 4; ++i) {
        if (data[hash[i] % CAPACITY] == '0')
            flag = false;
    }
    return flag;

}

char *BloomFilter::returnData()
{
    char *retData = new char[CAPACITY];
    strncpy(retData, data, CAPACITY);
    return retData;
}