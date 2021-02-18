#ifndef CLIENT_HPP_
#define CLIENT_HPP_
#include<string>
#include<vector>
#include<sstream>
#include<iostream>
#include <arpa/inet.h>
#include <algorithm>
#include <ctype.h>
#include "my_lib.hpp"
#include "type.hpp"
using namespace std;

class InputDeal {
public:
	enum {
		ILL = 4,
		TOOLARGEDATA = 3,
		TOOLARGEKEY = 2,
		PARAMETER_ERR = 1,
		TRUE = 0
	};
	operator void* () {
		return block;
	}

	InputDeal(int fd) {
		block = nullptr;
		string getl;
		getline(cin, getl);
		InputDealStr(getl);
	}

	void InputDealStr(const string & InputStr) {
		if(InputStr.empty()) {
			stat = ILL;
			return;
		}
		istringstream is(InputStr);
		vector<string> c;
		while(is) {
			string dump;
			is >> dump;
			if(dump.empty())
				continue;
			c.push_back(dump);
		}
		
		transform(c.front().begin(), c.front().end(), c.front().begin(), ::toupper);
		if(c.front() == "INSERT") {
			if(c.size() != 3) {
				stat = PARAMETER_ERR;
				return;
			}
			if(c[1].size() > 128) {
				stat = TOOLARGEKEY;
				return;
			}
			if(c[2].size() > MAXIMUM) {
				stat = TOOLARGEDATA;
				return;
			}
			int len = sizeof(int) + c[0].size() + 1 + c[1].size() + 1 + c[2].size();
			block_len = len;
			block = new char[len];
			char * loc = block;
			len -= sizeof(int);
			len = htonl(len);
			((int*)loc)[0] = len;
			loc += sizeof(int);
			memcpy(loc, c[0].c_str(), c[0].size());
			loc += c[0].size();
			loc[0] = ' ';
			++loc;
			memcpy(loc, c[1].c_str(), c[1].size());
			loc += c[1].size();
			loc[0] = ' ';
			++loc;
			memcpy(loc, c[2].c_str(), c[2].size());
			stat = TRUE;
			return;
		} else if (c.front() == "GET" || c.front() == "DELETE") {
			if(c.size() != 2) {
				stat = PARAMETER_ERR;
				return;
			}
			if(c[1].size() > 128) {
				stat = TOOLARGEKEY;
				return;
			}
			int len = sizeof(int) + c[0].size() + 1 + c[1].size();
			block_len = len;
			block = new char[len];
			char * loc = block;
			len -= sizeof(int);
			len = htonl(len);
			((int*)loc)[0] = len;
			loc += sizeof(int);
			memcpy(loc, c[0].c_str(), c[0].size());
			loc += c[0].size();
			loc[0] = ' ';
			++loc;
			memcpy(loc, c[1].c_str(), c[1].size());
			stat = TRUE;
			return;
		} else {
			stat = ILL;
		}
	}

	void SendComand(int serv) {
		if (stat == TRUE) {
			Write(serv, block, block_len);
		} else if(stat == ILL ) {
			errlog("REQUEST ILLEGAL");
		} else if (stat == TOOLARGEKEY) {
			errlog("Key too long");
		} else if (stat == TOOLARGEDATA) {
			errlog("unsupport data size");
		} else if (stat == PARAMETER_ERR) {
			errlog("parameter Error");
		} else {
			errlog("UnKnow Error");
		}
	}

	~InputDeal() { delete [] block; }
private:
	InputDeal();
	InputDeal(InputDeal && mv);
	InputDeal(const InputDeal & cpy);
	const InputDeal & operator = (InputDeal && mv);
	const InputDeal & operator = (const InputDeal & cpy);
	int block_len;
	char *block;
	int stat;
};

#endif