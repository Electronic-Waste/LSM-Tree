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
    if (utils::dirExists(dir + "/Level0")) {
        SSVec.clear();
        std::vector<std::string> fileVec;
        int levelNum = 0;
        std::string path = dir + "/Level" + std::to_string(levelNum);
        /* Enter directories and load SSTables to cache */
        while (utils::dirExists(path)) {
            utils::scanDir(path, fileVec);
            uint64_t size = fileVec.size();
            /* Load every SSTables in this dir to cache */
            for (int i = 0; i < size; ++i) {
                SSTable *st = new SSTable(path);
                SSVec.push_back(st);
            }
            /* Update dir path */
            path = dir + "/Level" + std::to_string(++levelNum);
        }
    }
    /* Else just set TimeStamp to 1 */
    else {
        maxTimeStamp = 1;
    }
}

KVStore::~KVStore()
{

}

/**
 * @brief If inserting <key, str>, the MemTable will overflow or not?
 * @param key Inserted pair's key
 * @param str Inserted pair's key
 * @return
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
                int compactFileNum = maxFilesNum - currentFilesNum;
                int SSVecSize = SSVec.size();
                std::vector<SSTable *> fileSSVec;               //Corresponding cache for path in fileVec
                /* Add corresponding cache to fileSSVec (using the path in fileVec) */
                for (int i = 0; i < currentFilesNum; ++i) {
                    std::string p1 = fileVec[i];
                    for (int j = 0 ; j < SSVecSize; ++j) {
                        std::string p2 = SSVec[j]->returnPath();
                        if (p1 == p2) fileSSVec.push_back(SSVec[j]);
                    }
                }
                /* Sort fileSSVec from small to big according to timeStamp */
                for (int i = 1; i < currentFilesNum; ++i) {
                    for (int j = 0; j < currentFilesNum - i; ++j) {
                        if (fileSSVec[j+1]->returnHeader()->timeStamp
                                < fileSSVec[j]->returnHeader()->timeStamp)
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
            std::string nextDirPath = dataDir + "/Level" + std::to_string(++currentLevel);
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
                            SSVec[j]->reset();
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
                        if (p1 == p2 && (minKey < header->minKey || maxKey > header->maxKey))
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
                            SSVec[j]->reset();
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
 * @param Arr KWayNode *
 * @param dirPath The dir path that we write SSTable into
 */
void KVStore::kwayCombine(std::vector<KVArray *> &Arr, const std::string &dirPath)
{
    bool isContinue = true;
    int KVArraysNum = Arr.size();
    std::vector<KWayNode *> KVec;       //K-Way combination's buffer
    std::vector<int> eraseIndex;        //The element index in KVec (KVec[index] need to be deleted)
    std::vector<int> updateIndex;       //The element index in KVArrayVec(Arr). We need to pull Arr[index]->KVCache[cachePos] into the buffer.
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
        KVec.push_back(node);
        ++Arr[i]->cachePos;
    }
    /* K-Way Combine */
    while (isContinue){
        isContinue = false;
        int KWayArrSize = KVec.size();
        /********** Bubble Sort *************/
        for (int i = 1; i < KWayArrSize; ++i) {
            for (int j = 0; j < KWayArrSize - i; ++j) {
                if (KVec[j]->KVNode.first > KVec[j+1]->KVNode.first) {
                    KWayNode *tmp = KVec[j];
                    KVec[j] = KVec[j+1];
                    KVec[j+1] = tmp;
                }
            }
        }
        /******* Deal with the newly sorted KVec (buffer for K-Way Combination) *********/
        uint64_t minKey;
        std::string valForMinKey;
        /* Get those which have the same key into the eraseVec */
        for (int i = 0; i < KWayArrSize; ++i) {
            uint64_t key1 = KVec[i]->KVNode.first;
            uint64_t stamp1 = KVec[i]->timeStamp;
            int reserveIndex = i;
            int j;
            /* Search behind */
            for (j = i + 1; j < KWayArrSize; ++j) {
                /* Not found, break */
                if (KVec[j]->KVNode.first != key1) break;
                /* If the latter one has bigger timestamp, then update reserveIndex */
                else if (KVec[j]->timeStamp > stamp1) reserveIndex = j;
            }
            /* Push index into the eraseVec except reserveIndex */
            for (int k = i; k < j; ++k) {
                if (k != reserveIndex) eraseIndex.push_back(k);
            }
            /* Update i */
            i = j - 1;
        }
        /* Erase the elements that have their indexes in eraseVec. Update updateIndex array. */
        int eraseNum = eraseIndex.size();
        for (int i = 0; i < eraseNum; ++i) {
            int index = eraseIndex[i];
            updateIndex.push_back(KVec[index]->KWayArrayIndex);
            KVec.erase(KVec.begin() + index);
        }
        /* Get the minimum k-v pair */
        int minIndex = KVec[0]->KWayArrayIndex;
        minKey = KVec[0]->KVNode.first;             // Get minKey!
        valForMinKey = KVec[0]->KVNode.second;      // Get valForMinKey!
        updateIndex.push_back(minIndex);            // Index KVec[0]->KwayArrayIndex in Arr(KWayArray Vec) needs to be updated too.
        KVec.erase(KVec.begin());
        /* Update KVec */
        int updateSize = updateIndex.size();
        for (int i = 0; i < updateSize; ++i) {
            int index = updateIndex[i];
            /* Overflow */
            if (++Arr[index]->cachePos >= Arr[index]->cacheSize)
                Arr[index]->isOverFlow = true;
            /* Not Overflow: push new KWayNode into KVec */
            else {
                int pos = Arr[index]->cachePos;
                KWayNode *node = new KWayNode(index, Arr[index]->timeStamp, Arr[index]->KVCache[pos]);
                KVec.push_back(node);
            }
        }

        /****** Insert the minimum K-V node into memtable (and generate cache, SSTable). ********/
        /* Whether overflow or not? */
        bool isToOverflow = false;
        int mSize = m->getByteSize();
        std::string pStr = m->get(minKey);
        /* Key not found or has been deleted */
        if (pStr == "") isToOverflow = mSize + valForMinKey.length() + 12 > MAX_BYTE;                 //Insert a new MemNode
        /* Key found */
        else isToOverflow =  mSize + valForMinKey.length() - pStr.length() > MAX_BYTE;      //Update the val of the original MemNode
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
        if (!KVec.empty()) isContinue = true;

        /******* Updating some vectors so that they won't affect the next iteration ********/
        KVec.clear();
        eraseIndex.clear();
        updateIndex.clear();
    }
    /* Write remaing node in m(MemTable) to the disk and create cache */
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
    std::string getMemStr = mem->get(key);
    /* Not found in mem or has been deleted in mem (Need to repair) */
    if (getMemStr != "") {
        return getMemStr;
    }
    /* Search it in SSTables */
    else {
        int size = SSVec.size();
        for (int i = 0; i < size; ++i) {
            std::string retStr;
            if ((retStr = SSVec[i]->get(key)) != "")
                return retStr;
        }
    }
    /* Not found anywhere */
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string getStr = mem->get(key);
    /* Key not found In MemTable or has been deleted (buggy!!)*/
    if (getStr != "")
        return mem->del(key);
    /* Search in SSTables */
    else {
        int size = SSVec.size();
        for (int i = 0; i < size; ++i) {
            std::string retStr;
            if ((retStr = SSVec[i]->get(key)) != "") {
                put(key, retStr);
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
    mem->scan(key1, key2, list);
}

/**
 * @brief Used for debug
 */
void KVStore::display()
{
    printf("MemTable ByteSize: %d\n", mem->getByteSize());
    printf("SSTable Num: %lld", SSVec.size());
}