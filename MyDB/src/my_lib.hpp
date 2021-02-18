#ifndef MY_LIB_H
#define MY_LIB_H

#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include<string>
#include "my_lib.hpp"
using namespace std;

int create_file(const char *path, int flag, mode_t mode);
int open_file(const char *path, int oflag);
size_t Read(int fd,  void * buf, size_t size);
size_t Write(int fd, const void * buf, size_t size);
void * Malloc(size_t size);
unsigned int power2(unsigned int x, unsigned int y);
unsigned int bkdr_hash(const char* key, size_t len);
void swap(void * lhs, void * rhs, size_t val_size);
int SetNonBlock(int fd);

enum {
	FAILURE = 0,
	SUCCESS = 1,
    ALREADY_EXISTS = -2
};

//print human-read-able string about errno or other custom str;
class PrintError {
private:
	pthread_mutex_t lock;
	int ErrOut;
	PrintError(PrintError && mv);
	PrintError(const PrintError & cpy);
	const PrintError & operator=(const PrintError & cpy);
	const PrintError & operator=(PrintError && mv);
public:
	PrintError(int fd = STDERR_FILENO) {
		ErrOut = fd;
		pthread_mutex_init(&lock, NULL);
	}
	PrintError(const string & logFile) {
		if(!access(logFile.c_str(), F_OK)) {
			ErrOut = open_file(logFile.c_str(), O_WRONLY);
		} else {
			ErrOut = create_file(logFile.c_str(), O_WRONLY, 0600);
		}
		pthread_mutex_init(&lock, NULL);
	}
	~PrintError() {
		if(ErrOut != STDERR_FILENO && ErrOut != STDOUT_FILENO)
			close(ErrOut);
		pthread_mutex_destroy(&lock);
	}
	void operator()(const string customStr) {
		pthread_mutex_lock(&lock);
		char endline = '\n';
		if(!customStr.empty())
		Write(ErrOut, customStr.c_str(), customStr.size());
		Write(ErrOut, &endline, 1);
		pthread_mutex_unlock(&lock);
	}
};

PrintError errlog;



int create_file(const char *path, int flag, mode_t mode) {
    int fd = open(path, O_CREAT | O_EXCL | flag, mode);
	if(fd == -1) {
		char buf[128];
		strerror_r(errno, buf, 128);
		errlog(string("create_file ERROR : ") + buf);
	}
    return fd;
}

int open_file(const char *path, int oflag) {
	int fd = open(path, oflag);
	if(fd == -1) {
                char buf[128];
		strerror_r(errno, buf, 128);
		errlog(string("open_file ERROR : ") + buf);
	}
	return fd;
}


size_t Read(int fd,  void * buf, size_t size) {
        int i = 0, num_r;
        while(i < size && (num_r = read( fd, buf, size))) {
                if(num_r == -1) {
                        char buf[128];
		        strerror_r(errno, buf, 128);
		        errlog(string("Read ERROR : ") + buf);
                        return(-1);
                }
                i += num_r;
        }
        return i;
}

size_t Write(int fd, const void * buf, size_t size) {
        int i = 0, num_w;
        while(i < size) {
            if ((num_w = write(fd,buf,size)) < 0) {
                char buf[128];
		strerror_r(errno, buf, 128);
	        errlog(string("Write ERROR : ") + buf);
                return(-1);
            } 
            if ( num_w == 0 ) {
                if( i < size )
                     continue;
                else 
                     break;
            }
            i+=num_w;
        }
        return i;
}

inline void * Malloc(size_t size) {
        void * to = malloc(size);
        if( to == NULL) {
                char buf[128];
		strerror_r(errno, buf, 128);
	        errlog(string("Malloc ERROR : ") + buf);
                abort();
        }
        return to;
}

inline unsigned int power2(unsigned int x, unsigned int y) {
        return x<<y;
}

unsigned int bkdr_hash(const char* str, size_t len) {
	unsigned int seed = 31; // 31 131 1313 13131 131313 etc.. 
	unsigned int hash = 0;
        for(int i = 0; i < len && str[i] != '\0'; ++i) {
		hash = hash * seed + str[i];
	}
	return hash;
}

inline void swap(void * lhs, void * rhs, size_t val_size) {
	char * dump = (char *)Malloc(val_size);
	memcpy(dump, lhs, val_size);
	memcpy(lhs, rhs, val_size);
	memcpy(rhs, dump, val_size);
	free(dump);
}

int SetNonBlock(int fd) {
	int flag = fcntl(fd, F_GETFL, 0);
	if(flag == -1) {
		errlog(string(strerror(errno)));
		return -1;
	}
	int ret = fcntl(fd, F_SETFL, flag | O_NONBLOCK);
	if(ret == -1)
		errlog(string(strerror(errno)));
	return ret;
}

#endif