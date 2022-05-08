#include <iostream>
#include <fstream>

#include "sstable.h"

/**
 * Get value string according to key
 * @param key key to be searched.
 * @return value string if found, "" else.
 */
std::string SSTable::get(uint64_t key)
{
    char c;
    uint32_t offset;
    uint32_t len = 0;
    char buf[512];
    std::ifstream out;

    /* Not Found in BloomFilter */
    if (bf->isFind(key) == false) return "";
    /* Not Found in Dic */
    else if (getOffSet(key, offset, len) == false) return "";
    /* Found in Dic */
    else {
        out.open(file_path);
        out.seekg(offset, out.beg);
        /* Value locate in the end of file */
        if (len == 0) 
            out.read(buf, out.end - offset);
        /* Read value according to len */
        else out.read(buf, len);
        out.close();
        /* the value is "~DELETE", return "" */
        if (buf == "~DELETE~") return "";
        /* return value string */
        else return buf;
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
        len = dic[mid + 1].second - offset;
        return true;
    }
}