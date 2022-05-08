#include "kvstore.h"
#include "memtable.h"
#include <string>

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
	return mem->get(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	std::string getStr = mem->get(key);
	/* Key Found In MemTable*/
	if (getStr != "" && getStr != "~DELETE~")
		return mem->del(key);
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	mem->reset();
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