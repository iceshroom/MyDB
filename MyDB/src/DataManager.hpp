#ifndef DATAMANAGER_HPP_
#define DATAMANAGER_HPP_

#include <queue>
#include <set>
#include "my_new.hpp"
#include "hash_str.hpp"
#include "type.hpp"
#include <sys/mman.h>
using namespace std;

enum {
	MAX_LEVEL = 29,
	BUCKET_SIZE = 149*16
};

class DataManager {
private:
		FD index, data;
		bool stat;

		off_t BeginOfIndex;
		size_t level;
		size_t next;
		size_t LenOfTable;
		off_t DataEndOffset;

		pthread_rwlock_t rwlock;

		void flush() {
			FD i = index, d = data;
			size_t indexHeadLen;
			lseek(i.getfd(), 0, SEEK_SET);
			Read(i.getfd(), &indexHeadLen, sizeof(size_t));
			indexHeadLen += sizeof(size_t);
			lseek(i.getfd(), indexHeadLen, SEEK_SET);
			Write(i.getfd(), &level, sizeof(size_t));
			Write(i.getfd(), &next, sizeof(size_t));
			Write(i.getfd(), &LenOfTable, sizeof(size_t));

			lseek(d.getfd(), 0, SEEK_SET);
			Write(d.getfd(), &DataEndOffset, sizeof(off_t));
			heapflush();
		};

		struct EmptySpace {
			off_t Begin;
			size_t Len;
			EmptySpace(off_t begin = 0, size_t len = 0) : Begin(begin), Len(len) {}
		};
		
		struct CmpEmptySpaceByLen {
			bool operator()(const EmptySpace & lhs, const EmptySpace & rhs) {
				return lhs.Len < rhs.Len;
			}
		};

		struct CmpEmptySpaceByBegin {
			bool operator()(const EmptySpace & lhs, const EmptySpace & rhs) {
				return lhs.Begin < rhs.Begin;
			}
		};

		//存储空闲空间的大根堆
		vector<EmptySpace> heap;

		//下面是大根堆的操作函数

		void heapInit() {
			FD r = data;
			lseek(r.getfd(), 0, SEEK_SET);
			off_t n;
			Read(r.getfd(), &n, sizeof(n));
			lseek(r.getfd(), n, SEEK_SET);
			EmptySpace dump;
			size_t len;
			Read(r.getfd(), &len, sizeof(len));
			for(int i = 0; i < len; ++i) {
				Read(r.getfd(), &dump, sizeof(dump));
				heap.push_back(dump);
			}
		}

		void heapflush() {
			merge();
			FD r = data;
			lseek(r.getfd(), sizeof(off_t), SEEK_SET);
			off_t n;
			Read(r.getfd(), &n, sizeof(n));
			lseek(r.getfd(), n, SEEK_SET);
			size_t len;
			Read(r.getfd(), &len, sizeof(len));
			push(EmptySpace(n, len*sizeof(dump)+sizeof(size_t)));
			off_t newPlace = GetSpace(heap.size()*sizeof(dump)+sizeof(size_t));
			lseek(r.getfd(), newPlace, SEEK_SET);
			len = heap.size();
			Write(r.getfd(), &len, sizeof(size_t));
			for(int i = 0; i < len; ++i) {
				Write(r.getfd(), &(heap[i]), sizeof(EmptySpace));
			}
		}
		
		//大根堆碎片整理
		void merge() {
			if(heap.empty())
				return;
			set<EmptySpace, CmpEmptySpaceByBegin> store(heap.begin(), heap.end());
			heap.clear();
			EmptySpace dump(store.begin()->Begin, store.begin()->Len);
			store.erase(store.begin());
			while(!store.empty()) {
				auto & c = *store.begin();
				if(dump.Len+dump.Begin == c.Begin) {
					dump.Len += c.Len;
				} else {
					push(dump);
					dump = *store.begin();
				}
				store.erase(store.begin());
			}
			push(dump);
		}

		void push(EmptySpace && toPush) {
			heap.emplace_back(toPush);
			push_heap(heap.begin(), heap.end(), CmpEmptySpaceByLen());
		}

		void push(EmptySpace & toPush) {
			heap.push_back(toPush);
			push_heap(heap.begin(), heap.end(), CmpEmptySpaceByLen());
		}

		void pop() {
			pop_heap(heap.begin(), heap.end(), CmpEmptySpaceByLen());
			heap.pop_back();
		}
		//大根堆的操作函数以上

		//找到一个能容纳n bit的空闲空间
		off_t GetSpace(size_t n) {
			if(heap.empty()) {
				off_t ret = DataEndOffset;
				DataEndOffset += n;
				char zero = '\0';
				return ret;
			}
				
			const EmptySpace i = heap.front();
			off_t ret;
			if(i.Len < n) {
				off_t ret = DataEndOffset;
				DataEndOffset += n;
				char zero = '\0';
				return ret;
			} else {
				pop();
				if(n < i.Len)
					push(EmptySpace(i.Begin+n, i.Len-n));
				return i.Begin;
			}
		}

		//分配一个新的Bucket
		off_t GetNewBucket() {
			off_t NewBucket = GetSpace(BUCKET_SIZE);
			FD w(data);
			lseek(w.getfd(), NewBucket, SEEK_SET);
			off_t dump = NewBucket+BUCKET_SIZE;
			Write(w.getfd(), &dump, sizeof(off_t));
			dump = BUCKET_SIZE-16;
			Write(w.getfd(), &dump, sizeof(off_t));
			return NewBucket;
		}

		//根据key寻找data，返回一个offset设置为找到data的FD， 否则返回一个无效FD（-1）
		FD FindLocat(hash_str & key) {
			FD r = FindBucket(key);
			item ret;
			if(r.getfd() != -1) {
				size_t End, EmptySpace;
				Read(r.getfd(), &End, sizeof(size_t));
				Read(r.getfd(), &EmptySpace, sizeof(size_t));

				off_t now = lseek(r.getfd(), 0, SEEK_CUR);
				End -= EmptySpace;
				char k[129];
				while(now < End) {
					size_t keyLen;
					unsigned int ReadHash;
					Read(r.getfd(), &ReadHash, sizeof(ReadHash));
					Read(r.getfd(), &keyLen, sizeof(size_t));

					if(ReadHash == key.gethash()) {
						Read(r.getfd(), k, keyLen);
						k[keyLen] = '\0';
						if(key.getstr() == k) {
							off_t go;
							lseek(r.getfd(), now+140, SEEK_SET);
							Read(r.getfd(), &go, sizeof(off_t));
							lseek(r.getfd(), go, SEEK_SET);
							return r;
						}
					}
					now = lseek(r.getfd(), now+148, SEEK_SET);
				}
			}
			return -1;
		}


		//根据key找到key应该在的Bucket
		FD FindBucket(hash_str & key) {
			unsigned int hash = key.gethash();
			unsigned int loc = hash & (UINT_MAX >> (30-level));
			if(loc < next)
				loc = hash & (UINT_MAX >> (29-level));
			off_t BucketLocate = ReadIndexGetLocat(loc);
			if(BucketLocate == 0)
				return FD();
			FD r(data);
			lseek(r.getfd(), BucketLocate, SEEK_SET);
			return r;
		}
		
		//读index，找到并返回下标n对应的offset
		off_t ReadIndexGetLocat(size_t n) {
			FD getLoc = index;
			off_t Location = BeginOfIndex + n*sizeof(off_t);
			off_t dataOffset;
			lseek(getLoc.getfd(), Location, SEEK_SET);
			Read(getLoc.getfd(), &dataOffset, sizeof(off_t));
			return dataOffset;
		}

		//写index，将toput写到n为下标的位置
		void WriteIndexPut(size_t n, off_t toput) {
			FD getLoc = index;
			off_t Location = BeginOfIndex + n*sizeof(off_t);
			lseek(getLoc.getfd(), Location, SEEK_SET);
			Write(getLoc.getfd(), &toput, sizeof(off_t));
		}

		typedef pair<hash_str, off_t> ele;
		//扩大index表，动态哈希表的扩大
		void Expand() {
			vector<ele> reinsert;
			FD rbucket = data;
			off_t reinsertLocation = ReadIndexGetLocat(next);
			lseek(rbucket.getfd(), reinsertLocation, SEEK_SET);
			off_t END, Empty;
			Read(rbucket.getfd(), &END, sizeof(off_t));
			Read(rbucket.getfd(), &Empty, sizeof(off_t));

			off_t now = lseek(rbucket.getfd(), 0, SEEK_CUR);
			END -= Empty;

			unsigned int hash;
			size_t len;
			off_t dataOffset;

			while(now < END) {
				Read(rbucket.getfd(), &hash, sizeof(unsigned int));
				Read(rbucket.getfd(), &len, sizeof(size_t));
				char keybuf[len+1];
				Read(rbucket.getfd(), keybuf, len);
				keybuf[len] = '\0';
				lseek(rbucket.getfd(), now+140, SEEK_SET);
				Read(rbucket.getfd(), &dataOffset, sizeof(off_t));
				reinsert.emplace_back(make_pair(hash_str(keybuf, hash), dataOffset));
				now = lseek(rbucket.getfd(), 0, SEEK_CUR);
			}

			lseek(rbucket.getfd(), reinsertLocation+sizeof(off_t), SEEK_SET);
			off_t CLEAR = BUCKET_SIZE-16;
			Write(rbucket.getfd(), &CLEAR, sizeof(off_t));

			off_t AllocNew = GetNewBucket();
			WriteIndexPut(next, AllocNew);
			WriteIndexPut(LenOfTable, GetNewBucket());
			++LenOfTable;
			if(LenOfTable == (8 << level)) {
				++level;
				next = 0;
			} else
				++next;
			for(auto & e : reinsert) {
				DoBucketInsert(e.first, e.second);
			}
		}

		//将key放入对应的桶中，data指向已经插入到data文件中的数据
		void DoBucketInsert(hash_str & key, off_t data) {
			FD FoundBucket = FindBucket(key);
			off_t END;
			size_t Empty;
			Read(FoundBucket.getfd(), &END, sizeof(off_t));
			off_t EmptyOffset = lseek(FoundBucket.getfd(), 0, SEEK_CUR);
			Read(FoundBucket.getfd(), &Empty, sizeof(size_t));
			if(Empty == 0 && level < MAX_LEVEL) {
				while(Empty == 0) {
					Expand();
					FoundBucket = FindBucket(key);
					Read(FoundBucket.getfd(), &END, sizeof(off_t));
					EmptyOffset = lseek(FoundBucket.getfd(), 0, SEEK_CUR);
					Read(FoundBucket.getfd(), &Empty, sizeof(size_t));
				}
				flush();
			} else if(Empty < 148*8 && level < MAX_LEVEL) {
				Expand();
				FoundBucket = FindBucket(key);
				Read(FoundBucket.getfd(), &END, sizeof(off_t));
				EmptyOffset = lseek(FoundBucket.getfd(), 0, SEEK_CUR);
				Read(FoundBucket.getfd(), &Empty, sizeof(size_t));
			} else if(level == MAX_LEVEL && Empty == 0) {
				unsigned int hash = key.gethash();
				unsigned int loc = hash & (UINT_MAX >> (30-level));
				if(loc < next)
					loc = hash & (UINT_MAX >> (29-level));
				off_t oldOfft = EmptyOffset-8;
				size_t oldSize = END-EmptyOffset+8;
				size_t OVERFLOW_SIZE = (oldSize)*2-16;
				off_t OVEREFLOW = GetSpace(OVERFLOW_SIZE);
				EmptyOffset = OVEREFLOW+sizeof(off_t);
				WriteIndexPut(loc, OVEREFLOW);

				push(EmptySpace(oldOfft, oldSize));
				FD wdata = data;
				lseek(wdata.getfd(), OVEREFLOW, SEEK_SET);
				OVEREFLOW += OVERFLOW_SIZE;
				Write(wdata.getfd(), &OVEREFLOW, sizeof(off_t));
				char BUF[oldSize];
				lseek(FoundBucket.getfd(), EmptyOffset+8, SEEK_SET);
				Read(FoundBucket.getfd(), BUF, oldSize);
				Write(wdata.getfd(), &oldSize, sizeof(size_t));
				Write(wdata.getfd(), BUF, oldSize);
				
				END = OVEREFLOW;
				Empty = OVERFLOW_SIZE - oldSize;
			}
			
			off_t BEGIN = lseek(FoundBucket.getfd(), END-Empty, SEEK_SET);

			unsigned int hash = key.gethash();
			size_t len = key.getstr().size();
			Write(FoundBucket.getfd(), &hash, sizeof(unsigned int));
			Write(FoundBucket.getfd(), &len, sizeof(size_t));
			Write(FoundBucket.getfd(), key.getcstr().c_str(), len);
			lseek(FoundBucket.getfd(), BEGIN+140, SEEK_SET);
			Write(FoundBucket.getfd(), &data, sizeof(off_t));
			Empty -= 148;
			lseek(FoundBucket.getfd(), EmptyOffset, SEEK_SET);
			Write(FoundBucket.getfd(), &Empty, sizeof(Empty));
		}

		//插入数据并返回offset
		off_t ItemInsert(item & toInsert) {
			off_t Space = GetSpace(toInsert.DataLevel()+2*sizeof(int));
			FD w = data;
			lseek(w.getfd(), Space, SEEK_SET);
			int dataLevel = toInsert.DataLevel();
			int datalen = toInsert.DataSize();
			Write(w.getfd(), &dataLevel, sizeof(int));
			Write(w.getfd(), &datalen, sizeof(int));
			Write(w.getfd(), toInsert.getData(), datalen);
			return Space;
		}

		DataManager(DataManager && mv);
		DataManager(const DataManager & cpy);
		const DataManager & operator = (const DataManager & cpy);

public:
	DataManager() : stat(false) {}
	DataManager(const string & IndexLocat) {
		DataManagerInit(IndexLocat);
	}
	void DataManagerInit(const string & IndexLocat) {
		if(!access(IndexLocat.c_str(), F_OK)) {
			if(!access(IndexLocat.c_str(), R_OK | W_OK)) {
				index = open_file(IndexLocat.c_str(), O_RDWR);
				if(!index.openSuccess()) {
					stat = false;
					return;
				}
				size_t dataPathLen;
				Read(index.getfd(), &dataPathLen, sizeof(size_t));

				//open Data file
				char buf[dataPathLen+1];
				Read(index.getfd(), buf, dataPathLen);
				buf[dataPathLen] = '\0';
				data = open_file(buf, O_RDWR);
				if(!data.openSuccess()) {
					index.Close();
					stat = FAILURE;
					return;
				}

				Read(data.getfd(), &DataEndOffset, sizeof(off_t));
				Read(index.getfd(), &level, sizeof(level));
				Read(index.getfd(), &next, sizeof(next));
				Read(index.getfd(), &LenOfTable, sizeof(LenOfTable));
				BeginOfIndex = dataPathLen + sizeof(size_t)*4;
				heapInit();
			} else {
				stat = FAILURE;
				perror("Permission denied: ");
			}
		} else {
			index = create_file(IndexLocat.c_str(), O_RDWR, 0600);
			if(!index.openSuccess()) {
				stat = FAILURE;
				return;
			}
			string DataLocat = IndexLocat + ".db";
			data = create_file(DataLocat.c_str(), O_RDWR, 0600);
			if(!data.openSuccess()) {
				stat = FAILURE;
				index.Close();
				unlink(IndexLocat.c_str());
				return;
			}
			level = 0;
			next = 0;
			LenOfTable = 4;
			size_t len = DataLocat.size();
			Write(index.getfd(), &len, sizeof(len));
			Write(index.getfd(), DataLocat.c_str(), DataLocat.size());
			Write(index.getfd(), &(level), sizeof(level));
			Write(index.getfd(), &(next), sizeof(next));
			Write(index.getfd(), &(LenOfTable), sizeof(LenOfTable));
			BeginOfIndex = DataLocat.size() + sizeof(size_t)*4;

			DataEndOffset = 16;
			Write(data.getfd(), &DataEndOffset, sizeof(off_t));
			off_t newPlace = GetSpace(sizeof(size_t));
			Write(data.getfd(), &newPlace, sizeof(off_t));
			lseek(data.getfd(), newPlace, SEEK_SET);
			size_t ZERO = 0;
			Write(data.getfd(), &ZERO, sizeof(size_t));

			off_t ZEROFD = GetNewBucket();
			Write(index.getfd(), &(ZEROFD), sizeof(off_t));
			ZEROFD = GetNewBucket();
			Write(index.getfd(), &(ZEROFD), sizeof(off_t));
			ZEROFD = GetNewBucket();
			Write(index.getfd(), &(ZEROFD), sizeof(off_t));
			ZEROFD = GetNewBucket();
			Write(index.getfd(), &(ZEROFD), sizeof(off_t));
		}
		pthread_rwlock_init(&rwlock, NULL);
	}

	~DataManager() {
		if(!stat)
			return;
		pthread_rwlock_wrlock(&rwlock);
		flush();
		pthread_rwlock_unlock(&rwlock);
		pthread_rwlock_destroy(&rwlock);
	}


	item operator[] (hash_str & key) {
		item ret;
		pthread_rwlock_rdlock(&rwlock);
		FD found(FindLocat(key));
		if(found.openSuccess()) {
			int dataLevel;
			int dataLen;
			Read(found.getfd(), &dataLevel, sizeof(int));
			Read(found.getfd(), &dataLen, sizeof(int));
			char buf[dataLen];
			Read(found.getfd(), buf, dataLen);
			ret = item(buf, dataLen);
		}
		pthread_rwlock_unlock(&rwlock);
		return ret;
	}

	int insert(hash_str & key, item & data) {
		pthread_rwlock_rdlock(&rwlock);
		FD found = FindLocat(key);
		if(found.openSuccess()) {
			pthread_rwlock_unlock(&rwlock);
			return ALREADY_EXISTS;
		}
		pthread_rwlock_unlock(&rwlock);
		pthread_rwlock_wrlock(&rwlock);
		off_t dataOffset = ItemInsert(data);
		DoBucketInsert(key, dataOffset);
		pthread_rwlock_unlock(&rwlock);
		return SUCCESS;
	}

	int remove(hash_str & key) {
		pthread_rwlock_rdlock(&rwlock);
		FD r = FindBucket(key);
		item ret;
		if(r.getfd() != -1) {
			size_t End, EmptySize;
			Read(r.getfd(), &End, sizeof(size_t));
			off_t EmptySizeOffset = lseek(r.getfd(), 0, SEEK_CUR);
			Read(r.getfd(), &EmptySize, sizeof(size_t));
			off_t now = lseek(r.getfd(), 0, SEEK_CUR);
			End -= EmptySize;
			char k[129];
			pthread_rwlock_unlock(&rwlock);
			pthread_rwlock_wrlock(&rwlock);
			while(now < End) {
				size_t keyLen;
				unsigned int ReadHash;
				Read(r.getfd(), &ReadHash, sizeof(ReadHash));
				Read(r.getfd(), &keyLen, sizeof(size_t));

				if(ReadHash == key.gethash()) {
					Read(r.getfd(), k, keyLen);
					k[keyLen] = '\0';
					if(key.getstr() == k) {
						off_t go;
						lseek(r.getfd(), now+140, SEEK_SET);
						Read(r.getfd(), &go, sizeof(off_t));
						
						FD rdump = data;
						lseek(rdump.getfd(), go, SEEK_SET);
						int datalevel;
						Read(rdump.getfd(), &datalevel, sizeof(int));
						push(EmptySpace(go, datalevel+sizeof(int)*2));

						off_t lastOne = End-148;
						lseek(rdump.getfd(), lastOne, SEEK_SET);
						char lastOneBuf[148];
						Read(rdump.getfd(), lastOneBuf, 148);
						lseek(r.getfd(), now, SEEK_SET);
						Write(r.getfd(), lastOneBuf, 148);
						lseek(r.getfd(), EmptySizeOffset, SEEK_SET);
						EmptySize += 148;
						Write(r.getfd(), &EmptySize, sizeof(size_t));
						pthread_rwlock_unlock(&rwlock);
						return SUCCESS;
					}
				}
				now = lseek(r.getfd(), now+148, SEEK_SET);
			}
		}
		pthread_rwlock_unlock(&rwlock);
		return FAILURE;
	}

	operator bool() {
		return stat;
	}

	bool isok() {
		return stat;
	}

	void DEBUG_printAll() {
		off_t indexReadOut [LenOfTable];
		FD indexRead(index), dataRead(data);
		lseek(indexRead.getfd(), BeginOfIndex, SEEK_SET);
		Read(indexRead.getfd(), &indexReadOut, sizeof(off_t)*LenOfTable);
		int count = 1;
		cout << "LEVEL: " << level << " NEXT: " << next << endl;
		for(int i = 0; i < LenOfTable; ++i) {
			cout << "Bucket " << i << " : " << endl;
			lseek(dataRead.getfd(), indexReadOut[i], SEEK_SET);
			off_t END, EmptySpace;
			Read(dataRead.getfd(), &END, sizeof(off_t));
			Read(dataRead.getfd(), &EmptySpace, sizeof(off_t));
			off_t Begin = lseek(dataRead.getfd(), 0, SEEK_CUR);

			cout << "Begin : " << indexReadOut[i] << " END : " << END << " EmptySpace : " << EmptySpace << endl;
			int Con = 16-(EmptySpace/148);
			cout << "Contain number : " << Con << endl;
			for(int i = 0; i < Con; ++i) {
				char buf[148];
				memset(buf, 0, 148);
				Read(dataRead.getfd(), buf, 148);
				int hash = *((int*)buf);
				size_t keylen = min(*((size_t*)(buf+4)), (size_t)128);
				string key(buf+12, buf+12+keylen);
				off_t dataoffset = *((off_t*)(buf+140));
				cout << "No." << count++ << " hashVal: " << hash << " keyLen: " << keylen << " key: " << key << " dataOffset: " << dataoffset << endl;
			}
			for(int i = 0; i < 16-Con; ++i)
				cout << "EMPTY……\n";
		}
	}
};

#endif
