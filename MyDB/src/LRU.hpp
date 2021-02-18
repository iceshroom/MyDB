#ifndef LRU_HPP_
#define LRU_HPP_
#include<unordered_map>
#include<list>
#include "hash_str.hpp"
#include <utility>
#include "type.hpp"

struct cmpHashStr {
	bool operator() (const hash_str & lhs , const hash_str & rhs) const {
		return  const_cast<hash_str &>(lhs) == const_cast<hash_str &>(rhs);
	}
};
struct getStrHash {
	bool operator()(const hash_str & s) const {
		return  const_cast<hash_str &>(s).gethash();
	}
};

class LRU {
private:
	typedef list<pair<hash_str, item>> LRULIST;
	typedef unordered_map<hash_str, LRULIST::iterator, getStrHash, cmpHashStr> LRUMAP;
	

	LRUMAP m;
	LRULIST l;
	size_t MaxStore;
	pthread_mutex_t lock;

	LRU(LRU && mv);
	LRU(const LRU & cpy);
	const LRU & operator=(const LRU & cpy);
	const LRU & operator=(LRU && cpy);
public:
	LRU(size_t max_store = 1024) : MaxStore(max_store) {
		pthread_mutex_init(&lock, NULL);
	}

	~LRU() {
		pthread_mutex_destroy(&lock);
	}

	item operator[](hash_str & key) {
		pthread_mutex_lock(&lock);
		if(!m.count(key)) {
			pthread_mutex_unlock(&lock);
			return item();
		} else {
			const auto & mvFront = m[key];
			l.push_back(move(*mvFront));
			l.erase(mvFront);
		}
		m[key] = --l.end();
		item & ret = l.back().second;
		pthread_mutex_unlock(&lock);
		return ret;
	}

	size_t count(hash_str & key) {
		pthread_mutex_lock(&lock);
		size_t result = m.count(key);
		if(result) {
			auto & mvFront = m[key];
			l.emplace_back(move(*mvFront));
			l.erase(mvFront);
			m[key] = --l.end();
		}
		pthread_mutex_unlock(&lock);
		return result;
	}

	void insert(hash_str & key, item & data) {
		pthread_mutex_lock(&lock);
		if(!m.count(key)) {
			if(l.size() == MaxStore) {
				auto del = l.begin();
				m.erase(del->first);
				l.pop_front();
			}
			l.push_back({key, data});
		} else {
			auto & mvFront = m[key];
			l.push_back(move(*mvFront));
			l.erase(mvFront);
		}
		m[key] = --l.end();
		pthread_mutex_unlock(&lock);
	}
};

#endif