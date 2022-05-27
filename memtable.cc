#include <iostream>
#include <fstream>
#include <cstring>
#include "memtable.h"

/**
 * @brief Used to generate random number
 * @return Random number
 */
double MemTable::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

/**
 * @brief Generate a height for newly inserted node
 * @return height of the node
 */
int MemTable::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

/**
 * @brief Generate cache for SSTable and write the whole SSTable into disk.
 * @param SSVec Cache for SSTable(Organized in array)
 * @param timeStamp The time stamp that will be added to SSTable's header.
 */
void MemTable::createSSTable(std::vector<SSTable *> &SSVec, uint64_t timeStamp, const std::string &filePath)
{
     /* Initialize some variables used for generating cache and SSTable */
    uint32_t offset = 10240 + 32 + 12 * NumOfMemNode;
    SSInfo *header = new SSInfo(timeStamp, NumOfMemNode, minKey, maxKey);
    BloomFilter *bf = new BloomFilter;
    std::vector<std::pair<uint64_t, uint32_t>> dic;
    std::string file_path;

    /* Generate bloomfilter and dictionary part of SSTable */
    MemNode *p = head->forwards[0];
    while(p->type != MemNodeType::NIL) {
        uint64_t key = p->key;
        bf->insert(key);
        dic.push_back(std::pair<uint64_t, uint32_t>(key, offset));
        offset += p->val.length();
        p = p->forwards[0];
    }
    /* Generate path for SSTable to store in (level0 at beginning) */
    file_path = filePath;
    /* Initialize cache for SSTable */
    SSTable *st = new SSTable(header, bf, dic, file_path);
    SSVec.push_back(st);

    /* Write SSTable to disk */
    std::ofstream in;
    in.open(file_path, std::ios::out | std::ios::binary);
    in.write((char *)header, 32);
    char *tmp = bf->returnData();
    in.write(tmp, CAPACITY);
    delete[] tmp;
    /* Dictionary part */
    int dicSize = dic.size();
    for (int i = 0; i < dicSize; ++i) {
        in.write((char *) &dic[i].first, 8);
        in.write((char *) &dic[i].second, 4);
    }
    /* Value part */
    MemNode *q = head->forwards[0];
    uint64_t totalLength = 0;
    char *buf = new char[2100000];
    while (q->type != MemNodeType::NIL) {
        uint64_t length = q->val.length();
        strcat(buf, q->val.c_str());
        totalLength += length;
        q = q->forwards[0];
    }
    in.write(buf, totalLength);
    delete[] buf;
}

/**
 * @brief Add <key, val> to MemTable.
 * @param key uint64_t type.
 * @param val std::string type.
 */
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
    if (p->key == key && p->val != val) {
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
        minKey = key < minKey ? key : minKey;           //update minKey
        maxKey = key > maxKey ? key : maxKey;           //update maxKey
        byteSize += 12 + val.length();                  //update bytesize
        NumOfMemNode++;                                 //update NumOfMemNode
    }
}

/**
 * @brief Get value in <key, val>
 * @param key uint64_t type
 * @return "" if not exsits or has already been deleted; val else.
 */
std::string MemTable::get(uint64_t key)
{
    MemNode *p = head;
    for (int i = MAX_LEVEL - 1; i >= 0; --i) {
        while (p->forwards[i]->key < key) {
            p = p->forwards[i];
        }
    }

    /* If found in MemTable and the node has not been deleted */
    if (p->forwards[0]->key == key && p->forwards[0]->val != "~DELETE~") {
        return p->forwards[0]->val;
    }
    /* Else return false */
    else return "";
}

/**
 * @brief delete node of which key is @param key
 * @param key uint64_t type
 * @return True: if the node exists in MemTable. False: Not Found or has been deleted.
 */
bool MemTable::del(uint64_t key)
{
    MemNode *p = head;
    for (int i = MAX_LEVEL - 1; i >= 0; --i) {
        while (p->forwards[i]->key < key) {
            p = p->forwards[i];
        }
    }
    p = p->forwards[0];

    /* The node having key @param key is found and has not been deleted */
    if (p->key == key && p->val != "~DELETE~") {
        byteSize += 8 - p->val.length();            //update byteSize
        p->val = "~DELETE~";
        return true;
    }
    /* Else return false */
    else return false;
}

/**
 * @brief ReInitialize the MemTable
 */
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

/**
* Return a list including all the key-value pair between key1 and key2.
* keys in the list should be in an ascending order.
* An empty string indicates not found.
 * @param key1 lower bound of keys to search
 * @param key2 upper bound of keys to search
 * @param list the array for key-value pairs
*/
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

/**
 * @brief Delete the MemTable and release its memory space
 */
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

/**
 * @details This is designed to fix the bug in get(key) that appears in delete().
 *          If the k-v pair is deleted in memtable, the get function will simply return "", which will make delete() continue to run.
 * @brief Judge whether the key is deleted in memtable or not.
 * @param key
 * @return true: if deleted; false: else.
 */
bool MemTable::isDeleted(uint64_t key)
{
    /* Search for key */
    MemNode *p = head;
    for (int i = MAX_LEVEL - 1; i >= 0; --i) {
        while (p->forwards[i]->key < key) {
            p = p->forwards[i];
        }
    }
    p = p->forwards[0];

    /* Judge if p->val equals to "~DELETE~" */
    if (p->key == key && p->val == "~DELETE~") return true;
    else return false;
}