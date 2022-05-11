#include <iostream>
#include <fstream>

#include "sstable.h"
#include "utils.h"

/**
 * @brief Load SSTable to cache.
 * @param path the file path of SSTable
 */
SSTable::SSTable(const std::string &path)
{
    /* Define some variables used in this function */
    char timeStamp[8];
    char num[8];
    char minKey[8];
    char maxKey[8];
    char buf[CAPACITY];
    char key[8];
    char offset[4];
    uint64_t _timeStamp;
    uint64_t  _num;
    uint64_t  _minKey;
    uint64_t  _maxKey;
    uint64_t _key;
    uint32_t _offset;

    /* Load header, bloomfilter, dic from disk */
    std::ifstream out(path);
    out.read(timeStamp, 8);
    out.read(num, 8);
    out.read(minKey, 8);
    out.read(maxKey, 8);
    out.read(buf, CAPACITY);
    _timeStamp = * (uint64_t *) timeStamp;
    _num = * (uint64_t *) num;
    _minKey = * (uint64_t *) minKey;
    _maxKey = * (uint64_t *) maxKey;
    header = new SSInfo(_timeStamp, _num, _minKey, _maxKey);
    bf = new BloomFilter(buf);
    for (int i = 0; i < _num; ++i) {
        out.read(key, 8);
        out.read(offset, 4);
        _key = * (uint64_t *) key;
        _offset = * (uint32_t *) offset;
        dic.push_back(std::pair<uint64_t, uint32_t>(_key, _offset));
    }

}

/**
 * Get value string according to key
 * @param key key to be searched.
 * @return value string if found, "" else.
 */
std::string SSTable::get(uint64_t key)
{
    uint32_t offset;
    uint32_t len = 0;
    char *buf;
    std::ifstream out;

    /* Key out of range */
    if (key < header->minKey || key > header->maxKey) return "";
    /* Not Found in BloomFilter */
    else if (bf->isFind(key) == false) return "";
    /* Not Found in Dic */
    else if (getOffSet(key, offset, len) == false) return "";
    /* Found in Dic */
    else {
        out.open(file_path, std::ios::in | std::ios::binary);
        out.seekg(offset, out.beg);
        /* Value locate in the end of file */
        if (len == 0) {
            uint32_t fileSize;
            out.seekg(0, out.end);
            fileSize = out.tellg();
            out.seekg(offset, out.beg);
            buf = new char[fileSize - offset + 1];
            out.read(buf, fileSize - offset);
            buf[fileSize - offset] = '\0';
        }
        /* Read value according to len */
        else {
            buf = new char[len + 1];
            out.read(buf, len);
            buf[len] = '\0';
        }
        out.close();
        /* the value is "~DELETE~", return "" */
        if (strcmp(buf, "~DELETE~") == 0) return "";
        /* return value string */
        else {
            std::string retStr(buf);
            return retStr;
        }
    }
}

/**
 * Get offset and length of value according to key
 * @param key key to be searched.
 * @param offset the postion of value in the file
 * @param len the length of the value
 * @return true if found, false else. Meanwhile, get @param offset @param len updated.
 */
bool SSTable::getOffSet(uint64_t key, uint32_t &offset, uint32_t &len)
{
    uint64_t sizeOfDic = dic.size();
    uint64_t left, right, mid;
    left = 0;
    right = sizeOfDic - 1;
    /* Search: O(logn) */
    while (left <= right) {
        mid = (left + right) / 2;
        if (dic[mid].first == key) break;
        else if (dic[mid].first < key) left = mid + 1;
        else right = mid - 1;
    }
    /* Not Found: return false */
    if (key != dic[mid].first) return false;
    /* Found: update offset, len; return true */
    else {
        offset = dic[mid].second;
        /* dic[mid] is the last element or not?
         * true: len = 0; false: len = dic[mid + 1].second - offset */
        if (mid != sizeOfDic - 1)
            len = dic[mid + 1].second - offset;
        return true;
    }
}

/**
 * @brief Clear the SSTable in cache and corresponding file in disk
 */
void SSTable::reset()
{
    delete header;
    delete bf;
    dic.clear();
    utils::rmfile(file_path.c_str());
}

SSInfo *SSTable::returnHeader()
{
    uint64_t timeStamp = header->timeStamp;
    uint64_t  size = header->size;
    uint64_t minKey = header->minKey;
    uint64_t  maxKey = header->maxKey;
    SSInfo *h = new SSInfo(timeStamp, size, minKey, maxKey);
    return h;
}

/**
 * @brief Copy this.dic to d
 * @param d Array which we copy this.dic to
 */
void SSTable::getDicKey(std::vector<uint64_t> &dk)
{
    uint64_t dicSize = dic.size();
    for (int i = 0; i < dicSize; ++i) {
        uint64_t key = dic[i].first;
        dk.push_back(key);
    }
}
