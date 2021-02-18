#ifndef HASH_STR_H_
#define HASH_STR_H_
#include <string>
#include "my_lib.hpp"
using namespace std;
//计算字符串的哈希值并保存
class hash_str {
private:
	string s;
	bool need_rehash;
	unsigned int hashVal;
public:
	hash_str() : need_rehash(true) {}
	hash_str(const string & str) : need_rehash(true) {
		s = str;
	}

	hash_str(const string & str, unsigned int hash) : need_rehash(false), hashVal(hash) {
		s = str;
	}

	hash_str(const hash_str & tocpy){
		s = tocpy.s;
		need_rehash = tocpy.need_rehash;
		hashVal = tocpy.hashVal;
	}

	unsigned int gethash() {
		if(need_rehash) {
			need_rehash = false;
			return hashVal = bkdr_hash(s.c_str(), s.size());
		} else 
			return hashVal;
	}
	const string & getcstr() {
		return s;
	}
	string & getstr() {
		need_rehash = true;
		return s;
	}
	const hash_str & operator = (const hash_str & tocpy) {
		s = tocpy.s;
		need_rehash = tocpy.need_rehash;
		hashVal = tocpy.hashVal;
		return tocpy;
	}

	friend bool operator == (hash_str & lhs , hash_str & rhs) {
		if(lhs.gethash() == rhs.gethash()) {
			return lhs.getcstr() == rhs.getcstr();
		} else 
			return false;
	}
};

#endif