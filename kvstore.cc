#include "kvstore.h"
#include "memtable.h"
#include <string>
#include "utils.h"
#include <fstream>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    /* Initialize MemTable */
    mem = new MemTable();

    /* Initialize the path in which the data store */
    dataDir = dir;

    /* If "dir" does not exist, create it */
    if (!utils::dirExists(dir))
        utils::mkdir(dir.c_str());

    /* If "dir/Level0" exists, load SSTables from disk to generate cache, and set maxTimeStamp */
    int currentLevel = 0;
    std::string dirPath = dir + "/Level0";
    if (utils::dirExists(dirPath)) {
        maxTimeStamp = 0;
        SSVec.clear();
        std::vector<std::string> fileVec;
        /* Enter directories and load SSTables to cache */
        while (utils::dirExists(dirPath)) {
            utils::scanDir(dirPath, fileVec);
            uint64_t size = fileVec.size();
            /* Load every SSTables in this dir to cache */
            for (int i = 0; i < size; ++i) {
                std::string filePath = dirPath + "/" + fileVec[i];
                SSTable *st = new SSTable(filePath);
                SSVec.push_back(st);
                /* Set maxTimeStamp */
                SSInfo *h = st->returnHeader();
                if (h->timeStamp >= maxTimeStamp)
                    maxTimeStamp = h->timeStamp + 1;
            }
            /* Update dir path */
            dirPath = dir + "/Level" + std::to_string(++currentLevel);
            /* Clear fileVec */
            fileVec.clear();
        }
    }
    /* Else just set TimeStamp to 1 */
    else {
        maxTimeStamp = 1;
    }
}

KVStore::~KVStore()
{
    std::string dirPath = dataDir + "/Level0";
    std::string path = dirPath + "/sstable" + std::to_string(maxTimeStamp) + ".sst";
    mem->createSSTable(SSVec, maxTimeStamp++, path);
    if (isToCompact()) compact();
}

/**
 * @brief If inserting <key, str>, the MemTable will overflow or not?
 * @param key Inserted pair's key
 * @param str Inserted pair's key
 * @return true: will overflow; false: will not overflow.
 */
bool KVStore::isOverflow(uint64_t key, const std::string &str)
{
    int size = mem->getByteSize();
    std::string pStr = mem->get(key);
    /* Key not found or has been deleted */
    if (pStr == "") {
        return size + str.length() + 12 > MAX_BYTE;                 //Insert a new MemNode
    }
    /* Key found */
    else {
        return size + str.length() - pStr.length() > MAX_BYTE;      //Update the val of the original MemNode
    }
}

/**
 * @brief Determine whether to compact or not.
 * @return True: to compact. False: not to compact.
 */
bool KVStore::isToCompact()
{
    std::string dirPath = dataDir + "/Level0";
    std::vector<std::string> fileVec;
    utils::scanDir(dirPath, fileVec);
    /* Files num in level0 <= 2, not to compact */
    if (fileVec.size() <= 2) return false;
    /* Else to compact */
    else return true;
}

/**
 * This function is invoked when there are more than 2 files in level0.
 * @brief Conduct compaction operation.
 */
void KVStore::compact()
{
    int currentLevel = 0;                       //Maintain current directory's level
    int maxFilesNum;                            //Max number of files in current directory: 2 ^ (currentLevel + 1)
    int currentFilesNum;                        //The number of files in current directory
    std::vector<SSTable *> compactSSVec;          //Get cache files that need to be compacted under current directory
    std::vector<std::string> fileVec;           //Get files' name under current directory
    std::string dirPath = dataDir + "/Level0";      //Next directory to be visited

    /* Perform compaction operation */
    while (utils::dirExists(dirPath)) {
        utils::scanDir(dirPath, fileVec);
        currentFilesNum = fileVec.size();
        maxFilesNum = 2 << currentLevel;
        /* If the number of files in current directory outnumbers 2 ^ (currentLevel + 1), do compaction */
        if (fileVec.size() > maxFilesNum) {
            /******* Select compact files and put them into compactVec *******/
            /* If the level of dir is 0; compactVec = fileVec */
            int SSVecSize = SSVec.size();
            if (currentLevel == 0) {
                for (int i = 0; i < currentFilesNum; ++i) {
                    fileVec[i] = dirPath + "/" + fileVec[i];
                    std::string p1 = fileVec[i];
                    for (int j = 0 ; j < SSVecSize; ++j) {
                        std::string p2 = SSVec[j]->returnPath();
                        if (p1 == p2) compactSSVec.push_back(SSVec[j]);
                    }
                }
            }
            /* Else select (2 ^ (currentLevel + 1) - maxFilesNum) files and insert them into compactVec.
             * Those files have minimum timeStamp in the current directories */
            else {
                int compactFileNum = currentFilesNum - maxFilesNum;
                int SSVecSize = SSVec.size();
                std::vector<SSTable *> fileSSVec;               //Corresponding cache for path in fileVec
                /* Add corresponding cache to fileSSVec (using the path in fileVec) */
                for (int i = 0; i < currentFilesNum; ++i) {
                    fileVec[i] = dirPath + "/" + fileVec[i];
                    std::string p1 = fileVec[i];
                    for (int j = 0 ; j < SSVecSize; ++j) {
                        std::string p2 = SSVec[j]->returnPath();
                        if (p1 == p2) fileSSVec.push_back(SSVec[j]);
                    }
                }
                /* Sort fileSSVec from small to big according to timeStamp */
                for (int i = 1; i < currentFilesNum; ++i) {
                    for (int j = 0; j < currentFilesNum - i; ++j) {
                        /* Select according to timeStamp */
                        if (fileSSVec[j+1]->returnHeader()->timeStamp
                                < fileSSVec[j]->returnHeader()->timeStamp)
                        {
                            SSTable *tmp;
                            tmp = fileSSVec[j];
                            fileSSVec[j] = fileSSVec[j+1];
                            fileSSVec[j+1] = tmp;
                        }
                        /* If timestamp is equal, select SSTable that has smaller minKey */
                        else if (fileSSVec[j+1]->returnHeader()->timeStamp == fileSSVec[j]->returnHeader()->timeStamp
                                    && fileSSVec[j+1]->returnHeader()->minKey < fileSSVec[j]->returnHeader()->minKey)
                        {
                            SSTable *tmp;
                            tmp = fileSSVec[j];
                            fileSSVec[j] = fileSSVec[j+1];
                            fileSSVec[j+1] = tmp;
                        }
                    }
                }
                /* Select compactFileNum elements(in sequence) in fileSSVec */
                for (int i = 0; i < compactFileNum; ++i) {
                    compactSSVec.push_back(fileSSVec[i]);
                }
            }

            /********* Start to compact files into next level **************/
            std::string nextDirPath = dataDir + "/Level" + std::to_string(currentLevel + 1);
            std::vector<std::string> nextFileVec;
            std::vector<SSTable *> nextFileSSVec;
            std::vector<KVArray *> KVArrayVec;
            /* Current level is the last level. Create a new directory and do a compaction that deletes "~DELETE~" symbols */
            if (!utils::dirExists(nextDirPath)) {
                /* Since there are no files in next dir, we do not need to find files in next dir. */
                /* Create next dir */
                utils::mkdir(nextDirPath.c_str());
                /* Get KVArrays for every cache in compactSSVec */
                uint64_t KVArraysNum = compactSSVec.size();
                for (int i = 0; i < KVArraysNum; ++i) {
                    KVArray * kv = new KVArray(compactSSVec[i], KVReadMode::RMDELETE);
                    KVArrayVec.push_back(kv);
                }
                /* Deallocate those cache in compactSSVec because their corresponding SSTable in disk will be deleted */
                uint64_t comPactSSVecSize = compactSSVec.size();
                for (int i = 0; i < comPactSSVecSize; ++i) {
                    std::string deleteFilePath = compactSSVec[i]->returnPath();
                    uint64_t SSVecSize = SSVec.size();
                    for (int j = 0; j < SSVecSize; ++j) {
                        if (deleteFilePath == SSVec[j]->returnPath()) {
                            SSVec[j]->reset();                          // delete files and deallocate memory in cache
                            SSVec.erase(SSVec.begin() + j);
                            break;
                        }
                    }
                }

                /* Combine K-Way K-V pair arrays. Write the result into a MemTable and generate SSTable. */
                kwayCombine(KVArrayVec, nextDirPath);


            }
            /* Current level is not the last level. Do a normal compaction */
            else {
                /* We need to find files in the next directory that have keys in the range [minkey, maxKey] */
                uint64_t minKey = UINT64_MAX;
                uint64_t maxKey = 0;
                int compactFileNum = compactSSVec.size();
                /* Calculate minKey and maxKey */
                for (int i = 0; i < compactFileNum; ++i) {
                    SSInfo *header = compactSSVec[i]->returnHeader();
                    minKey = (minKey < header->minKey) ? minKey : header->minKey;
                    maxKey = (maxKey > header->maxKey) ? maxKey : header->maxKey;
                }
                /* Get paths of next directory's files */
                utils::scanDir(nextDirPath, nextFileVec);
                int nextFileNum = nextFileVec.size();
                /* Add corresponding cache files, which have key in range [minKey, maxKey], to compactSSVec */
                for (int i = 0; i < nextFileNum; ++i) {
                    std::string p1 = nextFileVec[i];
                    for (int j = 0; j < SSVecSize; ++j) {
                        SSInfo *header = SSVec[j]->returnHeader();
                        std::string p2 = SSVec[j]->returnPath();
                        if (p1 == p2 && !((header->minKey < minKey && header->maxKey < minKey) || (header->minKey > maxKey && header->maxKey > maxKey)))
                            compactSSVec.push_back(SSVec[j]);
                    }
                }
                /* Get KVArrays for every cache in compactSSVec */
                uint64_t KVArraysNum = compactSSVec.size();
                for (int i = 0; i < KVArraysNum; ++i) {
                    KVArray * kv = new KVArray(compactSSVec[i], KVReadMode::NORMALLY);
                    KVArrayVec.push_back(kv);
                }
                /* Deallocate those cache in compactSSVec because their corresponding SSTable in disk will be deleted */
                uint64_t comPactSSVecSize = compactSSVec.size();
                for (int i = 0; i < comPactSSVecSize; ++i) {
                    std::string deleteFilePath = compactSSVec[i]->returnPath();
                    uint64_t SSVecSize = SSVec.size();
                    for (int j = 0; j < SSVecSize; ++j) {
                        if (deleteFilePath == SSVec[j]->returnPath()) {
                            SSVec[j]->reset();                          // delete files and deallocate memory in cache
                            SSVec.erase(SSVec.begin() + j);
                            break;
                        }
                    }
                }
                /* Combine K-Way K-V pair arrays. Write the result into a MemTable and generate SSTable. */
                kwayCombine(KVArrayVec, nextDirPath);

            }

            /**** Deallocate some vectors' memory which was allocated in the if expression ****/
            /* KVArray */
            uint64_t KVArraySize = KVArrayVec.size();
            for (int i = 0 ; i < KVArraySize; ++i)
                delete KVArrayVec[i];

        }
        /* Else simply break the loop */
        else break;
        /* Set dirPath to the next directory */
        dirPath = dataDir + "/Level" + std::to_string(++currentLevel);
        /* Clear vectors */
        compactSSVec.clear();
        fileVec.clear();
    }
}

/**
 * @brief Combine K-Way K-V pair arrays. Write the result into a MemTable and generate SSTable.
 * @param Arr Combine source (The Vector that we store our KVArray in)
 * @param dirPath The dir path that we write SSTable into
 */
void KVStore::kwayCombine(std::vector<KVArray *> &Arr, const std::string &dirPath)
{
    bool isContinue = true;
    int KVArraysNum = Arr.size();
    std::vector<KWayNode *> KWayBuf;    //K-Way combination's buffer
    uint64_t KVTimeStamp = 0;           //Max timeStamp in all KVArrays
    MemTable *m = new MemTable;         //For generate SSTable

    /* Get the max timeStamp in all KVArrays */
    uint64_t arrSize = Arr.size();
    for (int i = 0; i < arrSize; ++i) {
        uint64_t timeStamp = Arr[i]->timeStamp;
        KVTimeStamp = (KVTimeStamp > timeStamp) ? KVTimeStamp : timeStamp;
    }
    /* Load the first element in every KVArray */
    for (int i = 0; i < KVArraysNum; ++i) {
        uint64_t index = Arr[i]->cachePos;
        KWayNode *node = new KWayNode(i, Arr[i]->timeStamp, Arr[i]->KVCache[index]);
        KWayBuf.push_back(node);
    }
    /* K-Way Combine */
    while (isContinue){
        isContinue = false;
        int KWayBufSize = KWayBuf.size();
        /********** Bubble Sort *************/
        for (int i = 1; i < KWayBufSize; ++i) {
            for (int j = 0; j < KWayBufSize - i; ++j) {
                if (KWayBuf[j]->KVNode.first > KWayBuf[j+1]->KVNode.first) {
                    KWayNode *tmp = KWayBuf[j];
                    KWayBuf[j] = KWayBuf[j+1];
                    KWayBuf[j+1] = tmp;
                }
            }
        }

        /******* Deal with the KWayBuf which was newly sorted (buffer for K-Way Combination) *********/
        std::vector<uint64_t> updateVec;                    // Index in KWayBuf to be updated
        uint64_t minKey = KWayBuf[0]->KVNode.first;         // Min key in KWayBuf
        uint64_t selectTimeStamp = 0;                       // Max timeStamp in the first few KWayNodes
        uint64_t reserveIndex = 0;                          // The Index of KWayNode that is reserved

        /* Determine which KWayNode is to be updated and Store their index in updateVec */
        for (uint64_t i = 0; i < KWayBufSize; ++i) {
            /* The nodes have same key with the first KWayNode */
            if (KWayBuf[i]->KVNode.first == minKey) {
                updateVec.push_back(i);
                /* Check timeStamp and update reserveIndex and maxTimeStamp(selectTimeStamp) */
                if (KWayBuf[i]->timeStamp > selectTimeStamp) {
                    reserveIndex = i;
                    selectTimeStamp = KWayBuf[i]->timeStamp;
                }
            }
            /* Else break */
            else break;
        }

        /* Store key and value of the element which has minimum element */
        minKey = KWayBuf[reserveIndex]->KVNode.first;                                   // key of the minimum element
        std::string valForMinKey = KWayBuf[reserveIndex]->KVNode.second;                   // value of the minimum element

        /* Update KWayNode having index in updateVec */
        uint64_t updateVecSize = updateVec.size();
        for (uint64_t i = 0 ; i< updateVecSize; ++i) {
            uint64_t arrIndex = KWayBuf[updateVec[i]]->KWayArrayIndex;
            uint64_t KVCachePos = ++Arr[arrIndex]->cachePos;           // Update current cachePos
            /* If is to overflow: Set overflow flag to true (although it doesn't work in the following steps) */
            if (KVCachePos >= Arr[arrIndex]->cacheSize) {
                Arr[arrIndex]->isOverFlow = true;
            }
            /* Not to overflow: Push the next element into KWayBuf */
            else {
                KWayNode *kWayNode = new KWayNode(arrIndex, Arr[arrIndex]->timeStamp, Arr[arrIndex]->KVCache[KVCachePos]);
                KWayBuf.push_back(kWayNode);
            }
        }

        /* Erase those KWayNode having index in updateVec */
        for (int i = updateVecSize - 1; i >= 0; --i) {
            delete KWayBuf[updateVec[i]];
            KWayBuf.erase(KWayBuf.cbegin() + updateVec[i]);
        }

        /****** Insert the minimum K-V node into memtable (and generate cache, SSTable). ********/
        /* Whether overflow or not? */
        bool isToOverflow = false;
        int mSize = m->getByteSize();
        std::string pStr = m->get(minKey);
        /* Key not found or has been deleted */
        if (pStr == "") isToOverflow = (mSize + valForMinKey.length() + 12 > MAX_BYTE);                 //Insert a new MemNode
        /* Key found */
        else isToOverflow =  (mSize + valForMinKey.length() - pStr.length() > MAX_BYTE);      //Update the val of the original MemNode
        /* Insert K-V node, deal with overflow and create new cache for SSTable in disk */
        /* If is to overflow */
        if (isToOverflow) {
            std::string path = dirPath + "/sstable" + std::to_string(maxTimeStamp++) + ".sst";
            /* Create cache and write SSTable to disk */
            m->createSSTable(SSVec, KVTimeStamp, path);
            m->reset();
        }
        m->put(minKey, valForMinKey);

        /****** Judge if the loop is to an end *******/
        if (!KWayBuf.empty()) isContinue = true;

    }
    /* Write remaining nodes in m(MemTable) to the disk and create cache */
    std::string remainPath = dirPath + "/sstable" + std::to_string(maxTimeStamp++) + ".sst";
    m->createSSTable(SSVec, KVTimeStamp, remainPath);
    /* Deallocating Memory */
    m->deleteTable();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    /* If is to overflow */
    if (isOverflow(key, s)) {
        std::string dirPath = dataDir + "/Level0";
        /* Check if dir exits or not */
        if (!utils::dirExists(dirPath)) {
            utils::mkdir(dirPath.c_str());
        }
        /* Store some parts of sstable in cache and write whole to disk */
        std::string path = dirPath + "/sstable" + std::to_string(maxTimeStamp) + ".sst";
        mem->createSSTable(SSVec, maxTimeStamp++, path);
        /* If files num in level0 > 2, compact SSTables in disk */
        if (isToCompact()) compact();
        /* Reset MemTable */
        mem->reset();
    }
    mem->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    std::string getMemStr;
    /* Deleted in mem */
    if (mem->isDeleted(key)) return "";
    /* Not found in mem ( not deleted ) */
    else if ((getMemStr = mem->get(key)) != "") return getMemStr;
    /* Search it in SSTables */
    else if (!SSVec.empty()){
        int size = SSVec.size();
        std::string retStr = "";
        uint64_t maxStamp = 0;
        for (int i = 0; i < size; ++i) {
            std::string loopStr = SSVec[i]->get(key);
            uint64_t timeStamp = SSVec[i]->returnHeader()->timeStamp;
            if (loopStr == "~DELETE~" && timeStamp > maxStamp) {
                retStr = "";
                maxStamp = timeStamp;
            }
            else if (loopStr != "" && timeStamp > maxStamp) {
                retStr = loopStr;
                maxStamp = timeStamp;
            }
        }
        return retStr;
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string getStr = mem->get(key);
    /* Key is deleted in MemTable */
    if (mem->isDeleted(key))
        return false;
    /* Key is found In MemTable */
    else if (getStr != "")
        return mem->del(key);
    /* Search in SSTables */
    else if (!SSVec.empty()){
        int size = SSVec.size();
        for (int i = size - 1; i >= 0; --i) {
            std::string retStr = SSVec[i]->get(key);
            if (retStr != "" && retStr != "~DELETE") {
                put(key, "~DELETE~");
                return true;
            }
        }
    }
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    /* Reinitialize MemTable */
    mem->reset();
    /* Delete cache for SSTables and corresponding files in disk */
    uint64_t size = SSVec.size();
    for (uint64_t i = 0; i < size; ++i) {
        SSVec[i]->reset();
    }
    /* Reset maxTimeStamp */
    maxTimeStamp = 1;
    /* Delete the remaining empty directories */
    int level = 0;
    while (true) {
        std::string dirPath = dataDir + "/Level" + std::to_string(level);
        if (utils::dirExists(dirPath))
            utils::rmdir(dirPath.c_str());
        else break;
    }
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    /******* Part1: Scan MemTable and the result will be stored in list1 *******/
    std::list<std::pair<uint64_t, std::string> > list1;
    mem->scan(key1, key2, list1);
    if (SSVec.empty()) {list = list1; return; }

    /******* Part2: Scan SSTable and the result will be stored in list2 *******/
    std::list<std::pair<uint64_t, std::string> > list2;

    /* Initialize some variables and vectors for scan */
    uint64_t SSVecSize = SSVec.size();
    MemTable *SSMem = new MemTable;
    std::vector<int> ScanIndexVec;

    /* Select SSTable that is in the range [key1, key2]. Store their indexes in ScanIndexVec */
    for (uint64_t i = 0; i < SSVecSize; ++i) {
        SSInfo *header = SSVec[i]->returnHeader();
        if (!(header->minKey < key1 && header->maxKey < key1
        || header->minKey >= key2 && header->maxKey >= key2)) {
            ScanIndexVec.push_back(i);
        }
    }

    /* Load corresponding SSTable to memory, and write them to SkipList */
    uint64_t scanSize = ScanIndexVec.size();
    for (int i = 0 ; i < scanSize; ++i) {
        KVArray *kv = new KVArray(SSVec[ScanIndexVec[i]], KVReadMode::RMDELETE);
        int size = kv->cacheSize;
        for (int j = 0; j < size; ++j) {
            if (kv->KVCache[j].first < key1) continue;
            else if (kv->KVCache[j].first > key2) break;
            else SSMem->put(kv->KVCache[j].first, kv->KVCache[j].second);
        }
    }
    SSMem->scan(key1, key2, list2);


    /********* Part3: Combine list1 and list2 **********/
    /* Two Way Combine */
    while (!list1.empty() && !list2.empty()) {
        std::pair<uint64_t, std::string> node1(list1.front().first, list1.front().second);
        std::pair<uint64_t, std::string> node2(list2.front().first, list2.front().second);
        /* key in list1 < key in list2, choose node1 */
        if (node1.first < node2.first) {
            list.push_back(std::pair<uint64_t, std::string>(node1.first, node1.second));
            list1.pop_front();
        }
        /* key in list1 = key in list2, choose node1 (because node1 has bigger timestamp) */
        else if (node1.first == node2.first) {
            list.push_back(std::pair<uint64_t, std::string>(node1.first, node1.second));
            list1.pop_front();
            list2.pop_front();
        }
        /* key in list1 > key in list2, choose node2 */
        else {
            list.push_back(std::pair<uint64_t, std::string>(node2.first, node2.second));
            list2.pop_front();
        }
    }
    /* Add the remaining nodes to list */
    std::list<std::pair<uint64_t, std::string> > &tmp = (list1.empty()) ? list2 : list1;
    while (!tmp.empty()) {
        list.push_back(std::pair<uint64_t, std::string>(tmp.front().first, tmp.front().second));
        tmp.pop_front();
    }


}

/**
 * @brief Used for debug
 */
void KVStore::display()
{
    printf("MemTable ByteSize: %d\n", mem->getByteSize());
    printf("SSTable Num: %lld", SSVec.size());
}