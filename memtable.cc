#include <iostream>
#include <fstream>

#include "memtable.h"

double MemTable::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int MemTable::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

void MemTable::createSSTable(std::vector<SSTable *> &SSVec, std::string file_path)
{
    // printf("yes!\n");
    uint64_t timeStamp = SSVec.size();
    uint32_t offset = 10240 + 32;
    SSInfo *header = new SSInfo(timeStamp, NumOfMemNode, minKey, maxKey);
    BloomFilter *bf = new BloomFilter;
    std::vector<std::pair<uint64_t, uint32_t>> dic;
    // printf("yes!\n");
    // printf("Byte Size: %d", byteSize);
    MemNode *p = head->forwards[0];
    while(p->type != MemNodeType::NIL) {
        uint64_t key = p->key;
        // printf("Key: %llu, Offset: %u\n", key, offset);
        bf->insert(key);
        dic.push_back(std::pair<uint64_t, uint32_t>(key, offset));
        offset += p->val.length();
        p = p->forwards[0];
    }
    SSTable *st = new SSTable(header, bf, dic, file_path);
    SSVec.push_back(st);

    // printf("yes!\n");
    std::ofstream in;
    in.open(file_path, std::ios::out | std::ios::binary);
    // printf("yes!\n");
    in.write((char *)&timeStamp, sizeof(uint64_t));
    in.write((char *)&NumOfMemNode, sizeof(uint64_t));
    in.write((char *)&minKey, sizeof(uint64_t));
    in.write((char *)&maxKey, sizeof(uint64_t));
    in.write(bf->returnData(), CAPACITY);
    MemNode *q = head->forwards[0];
    while (q->type != MemNodeType::NIL) {
        in.write(q->val.c_str(), q->val.length());
        q = q->forwards[0];
    }

}

void MemTable::put(uint64_t key, const std::string &val)
{
    
    MemNode *p = head;
    MemNode *update[MAX_LEVEL];
    for (int i = 0; i < MAX_LEVEL; ++i) update[i] = NULL;

    /* record the path */
    for (int i = MAX_LEVEL - 1; i >= 0; --i) {
        while (p->forwards[i]->key < key)
            p = p->forwards[i];
        update[i] = p;
    }
    p = p->forwards[0];

    /* same value, change the val */
    if (p->key == key) {
        byteSize += val.length() - p->val.length();     //update byteSize
        p->val = val;
    }
    /* different value, insert the node */
    else {
        int level = randomLevel();
        MemNode *newNode = new MemNode(key, val, MemNodeType::NORMAL);
        for (int i = 0; i < level; ++i) {
            newNode->forwards[i] = update[i]->forwards[i];
            update[i]->forwards[i] = newNode;
        }
        minKey = key < minKey ? key : minKey;
        maxKey = key > maxKey ? key : maxKey;
        byteSize += 12 + val.length() + 1;
        NumOfMemNode++;
    }
}

std::string MemTable::get(uint64_t key)
{
    MemNode *p = head;
    for (int i = MAX_LEVEL - 1; i >= 0; --i) {
        while (p->forwards[i]->key < key) {
            p = p->forwards[i];
        }
    }
    
    if (p->forwards[0]->key == key && p->forwards[0]->val != "~DELETE~") {
        return p->forwards[0]->val;
    }
    else return "";
}

bool MemTable::del(uint64_t key)
{
    MemNode *p = head;
    for (int i = MAX_LEVEL - 1; i >= 0; --i) {
        while (p->forwards[i]->key < key) {
            p = p->forwards[i];
        }
    }
    p = p->forwards[0];
    if (p->key == key && p->val != "~DELETE~") {
        byteSize += 8 - p->val.length();            //update byteSize
        p->val = "~DELETE~";
        return true;
    }
    else return false;
}

void MemTable::reset()
{
    /* Make MemTable empty */
    deleteTable();

    /* Rebuild MemTable */
    byteSize = 10240 + 32;
    NumOfMemNode = 0;
    minKey = UINT64_MAX;
    maxKey = 0;
    head = new MemNode(0, "", MemNodeType::HEAD);
    tail = new MemNode(UINT64_MAX, "", MemNodeType::NIL);
    for (int i = 0; i < MAX_LEVEL; ++i)
        head->forwards[i] = tail;
}

void MemTable::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    MemNode *p = head;
    list.clear();
    for (int i = MAX_LEVEL - 1; i >= 0; --i) {
        while (p->forwards[i]->key < key1)
            p = p->forwards[i];
    }
    p = p->forwards[0];

    while (p->key <= key2) {
        list.push_back(std::pair<uint64_t, std::string>(p->key, p->val));
        p = p->forwards[0];
    }
}

void MemTable::deleteTable()
{
    /* Make MemTable empty */
    MemNode *p1 = head;
    MemNode *p2;
    while (p1) {
        p2 = p1->forwards[0];
        delete p1;
        p1 = p2;
    }
}