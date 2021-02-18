#ifndef DBSERVER_HPP_
#define DBSERVER_HPP_

#include <fstream>
#include <vector>
#include <signal.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include "TcpConnect.hpp"
#include "Server.hpp"
using namespace std;

static vector<pthread_t> nchilds;
static int listenfd;
static int epfd;
void sigterm_handler(int sig);
void sigpipe_handler(int);
void * ThreadMain(void *);

string ReadConf;
string DataBaseLocation = "./mydb";
string ListenAddress = "127.0.0.1";
string port;

static bool need_term = false;

enum {
	ACCEPT = 1,
	CLIENT = 2,
	EMPTY = 0
};

struct EventData {
	int Fd;
	int Type;
	EventData(int fd, int type) : Fd(fd), Type(type) {};
};

int main() 
{
	ifstream is("./Server.conf");
	while(is >> ReadConf) {
		int l = ReadConf.find('=');
		string key = ReadConf.substr(0, l), value = ReadConf.substr(l+1);
		if(key == "DataBaseLocation") {
			DataBaseLocation = value;
		} else if(key == "ThreadNum") {
			int n = atoi(value.c_str());
			nchilds = vector<pthread_t>(n);
		} else if(key == "ListenAddress") {
			ListenAddress = value;
		} else if(key == "ListenPort") {
			port = value;
		}
	}
	is.close();

	DM.DataManagerInit(DataBaseLocation);
	listenfd = TcpListen(ListenAddress, port, nullptr);
	if(listenfd == -1 || SetNonBlock(listenfd) == -1)
		return -1;
	nchilds = vector<pthread_t>(GetNProcs());

	struct sigaction act;
	act.sa_handler = (&sigterm_handler);
	sigemptyset(&(act.sa_mask));
	act.sa_flags = SA_INTERRUPT;
	if(sigaction(SIGTERM, &act, NULL) < 0 || sigaction(SIGINT, &act, NULL) < 0) {
		errlog(string("sigaction Error : ") + strerror(errno));
		return -1;
	}
	act.sa_handler = &sigpipe_handler;
	if(sigaction(SIGPIPE, &act, NULL) < 0) {
		errlog(string("sigaction Error : ") + strerror(errno));
		return -1;
	}

	for(auto & ThreadId : nchilds) {
		pthread_create(&ThreadId, NULL, &ThreadMain, NULL);
	}
	
	epfd = epoll_create(100);
	epoll_event LisEv, CliEv;
	memset(&LisEv, 0, sizeof(epoll_event));
	memset(&CliEv, 0, sizeof(epoll_event));
	LisEv.data.ptr = (void *)(new EventData(listenfd, ACCEPT));
	LisEv.events = EPOLLIN;

	CliEv.events = (EPOLLIN | EPOLLRDHUP | EPOLLONESHOT);

	epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &LisEv);

	int max_ev = 100;
	int timeout = 1000000;
	int clientfd = -1;
	epoll_event buffer[max_ev];
	while(!need_term) {
		int ready = epoll_wait(epfd, buffer, max_ev, timeout);
		int roll = min(ready, max_ev);
		for(int i = 0; i < roll; ++i) {
			EventData * cur = ((EventData *)(buffer[i].data.ptr));
			switch ( cur->Type ) {
				case ACCEPT :
					clientfd = accept(listenfd, NULL, NULL);
					if(clientfd == -1) {
						errlog("Accept Error" + string(strerror(errno)));
						break;
					}
					CliEv.data.ptr = (void *)(new EventData(clientfd, CLIENT));
					if(epoll_ctl(epfd, EPOLL_CTL_ADD, clientfd, &CliEv) == -1)
						perror("Epoll_ctl");
					break;
				case CLIENT :
					if(buffer[i].events & EPOLLRDHUP) {
						epoll_ctl(epfd, EPOLL_CTL_DEL, cur->Fd, NULL);
						delete cur;
						close(buffer[i].data.fd);
					} else if (buffer[i].events & EPOLLIN) {
						pthread_mutex_lock(&Mlock);
						FdList.push_back(cur->Fd);
						pthread_cond_signal(&Cond);
						pthread_mutex_unlock(&Mlock);
						delete cur;
					}
					break;
				default :
					break;
			}
		}
	}
	cout << "Serv Terminal.\n";
	return 0;
}

void sigterm_handler(int sig) {
	for(auto & t : nchilds) {
		int n;
		pthread_cancel(t);
		if(pthread_mutex_trylock(&Mlock) == EBUSY) {
			//errlog("DEBUG: LOCKED");
			pthread_mutex_unlock(&Mlock);
		} else {
			//errlog("DEBUG: NOT LOCKED");
			pthread_mutex_unlock(&Mlock);
		}
		if( (n = pthread_join(t, NULL)) != 0) {
			errlog(strerror(n));
		}
	}
	need_term = true;
}

void sigpipe_handler(int) {
	return;
}

void * ThreadMain(void *) {
	int connectfd;
	SockAddr ClientAddr;
	sigset_t set;
	epoll_event CliEv;
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
	memset(&CliEv, 0, sizeof(epoll_event));
	CliEv.data.u32 = CLIENT;
	CliEv.events |= (EPOLLIN | EPOLLRDHUP | EPOLLONESHOT);
	sigemptyset(&set);
	sigaddset(&set, SIGTERM);
	sigaddset(&set, SIGINT);
	pthread_sigmask(SIG_BLOCK, &set, NULL);
	while(true) {
		pthread_mutex_lock(&Mlock);
		while(FdList.empty()) {
			pthread_cond_wait(&Cond, &Mlock);
		}
		connectfd = FdList.back();
		FdList.pop_back();
		pthread_mutex_unlock(&Mlock);
		Server(connectfd);
		CliEv.data.ptr = new EventData(connectfd, CLIENT);
		epoll_ctl(epfd, EPOLL_CTL_MOD, connectfd, &CliEv);
	}
	return NULL;
}



#endif