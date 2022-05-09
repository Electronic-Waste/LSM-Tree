#include <iostream>
#include <fstream>

#include "sstable.h"
#include "utils.h"

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

/**
 * @brief Scan from K1 to K2
 */
