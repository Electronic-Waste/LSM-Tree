#include "kvstore.h"
#include "memtable.h"
#include <string>
#include <cmath>
#include "utils.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
	mem = new MemTable();
}

KVStore::~KVStore()
{

}

bool KVStore::isOverflow(uint64_t key, const std::string &str)
{
	int size = mem->getByteSize();
	std::string pStr = mem->get(key);
	/* Not Found */
	if (pStr == "") {
		return size + str.length() + 12 > MAX_BYTE;
	}
	/* Found */
	else {
		return size + str.length() - pStr.length() > MAX_BYTE;
	}
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if (isOverflow(key, s)) {
		//printf("Yes\n");
		int sizeOfSSVec = SSVec.size();
		int level = log(sizeOfSSVec + 2) / log(2) - 1;		//dir level
		std::string dirPath = "Level" + std::to_string(level);
		std::string filePath = dirPath + "/sstable" + std::to_string(sizeOfSSVec) + ".sst";
		/* Check if dir exits or not */
		if (!utils::dirExists(dirPath)) {
			utils::mkdir(dirPath.c_str());
		}
		mem->createSSTable(SSVec, filePath);
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
	if (getMemStr != "") {
		return getMemStr;
	}
	else {
		int size = SSVec.size();
		for (int i = size - 1; i >= 0; --i) {
			std::string retStr;
			if ((retStr = SSVec[i]->get(key)) != "")
				return retStr;
		}
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
	/* Key Found In MemTable*/
	if (getStr != "")
		return mem->del(key);
	else {
		int size = SSVec.size();
		for (int i = size - 1; i >= 0; --i) {
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
	mem->reset();
	uint64_t size = SSVec.size();
	//printf("size: %lld\n", size);
	for (uint64_t i = 0; i < size; ++i) {
		SSVec[i]->reset();
	}
	int currentLevel = log(size + 1) / log(2) - 1;
	for (int i = 0; i < currentLevel; ++i) {
		std::string dirPath = "Level" + std::to_string(i);
		if (utils::dirExists(dirPath))
			utils::rmdir(dirPath.c_str());
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

void KVStore::display()
{
	printf("MemTable ByteSize: %d\n", mem->getByteSize());
	printf("SSTable Num: %lld", SSVec.size());
}