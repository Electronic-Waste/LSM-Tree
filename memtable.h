#pragma once

#include <vector>
#include <list>
#include <string>
#include <cstdint>


#define MAX_LEVEL 8
#define MAX_BYTE 2 * 1024 * 1024

enum MemNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct MemNode
{
    uint64_t key;
    std::string val;
    MemNodeType type;
    std::vector<MemNode *> forwards;
    MemNode(uint64_t _key, const std::string &_val, MemNodeType _type)
        : key(_key), val(_val), type(_type)
    {
        for (int i = 0; i < MAX_LEVEL; ++i)
            forwards.push_back(nullptr);
    }
};

class MemTable
{
    private:
        int byteSize;
        int NumOfMemNode;
        uint64_t minKey;
        uint64_t maxKey;
        MemNode *head;
        MemNode *tail;
        unsigned long long s = 1;
        double my_rand();
        int randomLevel();
        void transIntoSSTable(const std::string &input_path);

public:
    MemTable() {
        byteSize = 10240 + 32;
        NumOfMemNode = 0;
        minKey = UINT64_MAX;
        maxKey = 0;
        head = new MemNode(0, "", MemNodeType::HEAD);
        tail = new MemNode(UINT64_MAX, "", MemNodeType::NIL);
        for (int i = 0; i < MAX_LEVEL; ++i)
            head->forwards[i] = tail;
    }
    
    void put(uint64_t key, const std::string &val);

    std::string get(uint64_t key);

    bool del(uint64_t key);

    void reset();

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list);

    int getByteSize(){return byteSize;}

    void deleteTable();

};
