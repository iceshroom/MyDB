#ifndef SERVER_HPP_
#define SERVER_HPP_

#include "DataManager.hpp"
#include "LRU.hpp"
#include "my_lib.hpp"
#include <sstream>
#include <unordered_map>
#include <arpa/inet.h>
#include <algorithm>
#include <ctype.h>
#include <list>

using namespace std;

const static char NOTF[] = "NOT FOUND";

void Server(int clientfd);
static int ServReturn(const char * ret, int len, int fd);
static int deal_create_table(const string & tableName, istringstream & tableEle);

LRU LruDump;
DataManager DM;

list<int> FdList;
pthread_cond_t Cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t Mlock = PTHREAD_MUTEX_INITIALIZER;

class read_command {
	private:
		hash_str key;
		item data;

		enum {
			INSERT = 1,
			GET = 2,
			DELETE = 3,
			ILL = 4
		};
		int Command_type;

		read_command();
		read_command(read_command && mv);
		read_command(const read_command & cpy);
		const read_command & operator=(const read_command & cpy);
		const read_command & operator=(read_command && cpy);
	public:
		read_command(int fd) {
			int len;
			Read(fd, &len, sizeof(int));
			len = ntohl(len);
			if(len < 5 || len > 6 + 1 + KEY_LEN + 1 + MAXIMUM) {
				Command_type = ILL;
				return;
			}
			char buf[len+1];
			int readTestLen = Read(fd, buf, len);
			if(readTestLen < len) {
				Command_type = ILL;
				return;
			}
			buf[len] = '\0';

			int l = 0, r = 0;
			for(; r < len; ++r) {
				if(buf[r] == ' ')
					break;
			}
			if(r == len || r > 6) {
				Command_type = ILL;
				return;
			}
			
			string cmd(buf, buf+r);
			if(cmd == "INSERT") {
				Command_type = INSERT;
			} else if (cmd == "DELETE") {
				Command_type = DELETE;
			} else if (cmd == "GET") {
				Command_type = GET;
			} else {
				Command_type = ILL;
				return;
			}

			++r;
			l = r;
			for(; r < len; ++r) {
				if(buf[r] == ' ')
					break;
			}
			key = string(buf+l, buf+r);
			if(key.getcstr().empty() || key.getcstr().size() > MAXIMUM) {
				Command_type = ILL;
				return;
			}

			if(Command_type == INSERT) {
				if(r == len) {
					Command_type = ILL;
					return;
				}
				l = r+1;
				data = item(buf+l, len-l);
			}
		}

		int deal_comand(int connfd) {
			if(Command_type == INSERT) {
				return DM.insert(key, data);
			} else if(Command_type == GET){
				item find = DM[key];
				if(find.getData() != nullptr) {
					return ServReturn(find.getData(), find.DataSize(), connfd);
				} else {
					return ServReturn(NOTF, sizeof(NOTF), connfd);
				}
			} else if(Command_type == DELETE) {
				return DM.remove(key);
			} else {
				errlog("Illegal Command receive.");
				return FAILURE;
			}
		}
};

void Server(int clientfd) {
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	read_command Com(clientfd);
	Com.deal_comand(clientfd);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
}

static int ServReturn(const char * ret, int len, int fd) {
	uint32_t n =  htonl(len);
	Write(fd, &n, sizeof(uint32_t));
	Write(fd, ret, len);
	return SUCCESS;
}

#endif