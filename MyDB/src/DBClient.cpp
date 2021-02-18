#include "TcpConnect.hpp"
#include "client.hpp"
#include <iostream>
#include <signal.h>
#include <sys/epoll.h>

void sigpipe_handler(int);
int ReceiveAndprintData(int servfd);

struct EventData {
	int Fd;
	int Type;
	EventData(int fd, int type) : Fd(fd), Type(type) {};
};

enum {
	SERVFD = 10,
	STDINFD = 11
};

int main(int argc, char ** argv) {
	if(argc != 3) {
		cout << "DBClient <hostname/IpAddress> <Service/port>\n";
		return 0;
	}

	struct sigaction act;
	act.sa_handler = (&sigpipe_handler);
	sigemptyset(&(act.sa_mask));
	act.sa_flags = 0;
	if(sigaction(SIGTERM, &act, NULL) < 0 || sigaction(SIGINT, &act, NULL) < 0) {
		errlog(string("sigaction Error : ") + strerror(errno));
		return -1;
	}

	int servfd = TcpConnect(argv[1], argv[2]);
	if(servfd == -1) {
		cout << "Connect Error: " << argv[1] << " : " << argv[2] << strerror(errno) << endl;
		return -1;
	}

	cout << "Connect: " << argv[1] << " : " << argv[2] << " success.\n";
	int epfd = epoll_create(2);

	struct epoll_event StdIn, ServFd;
	memset(&StdIn, 0, sizeof(struct epoll_event));
	memset(&ServFd, 0, sizeof(struct epoll_event));
	StdIn.events = EPOLLIN;
	StdIn.data.ptr = new EventData(STDIN_FILENO, STDINFD);
	ServFd.events = EPOLLIN | EPOLLRDHUP;
	ServFd.data.ptr = new EventData(servfd, SERVFD);
	epoll_ctl(epfd, EPOLL_CTL_ADD, STDIN_FILENO, &StdIn);
	epoll_ctl(epfd, EPOLL_CTL_ADD, servfd, &ServFd);
	
	struct epoll_event buf[2];
	int timeout = 1000000;
	memset(buf, 0, sizeof(buf));
	for(;;) {
		int n = epoll_wait(epfd, buf, 2, timeout);
		for(int i = 0; i < n; ++i) {
			EventData * cur = ((EventData *)(buf[i].data.ptr));
			if(cur->Type == STDINFD) {
				InputDeal c(STDIN_FILENO);
				c.SendComand(servfd);
				epoll_ctl(epfd, EPOLL_CTL_MOD, cur->Fd, &StdIn);
			} else if (cur->Type == SERVFD) {
				if(buf[i].events & EPOLLRDHUP) {
					cout << "Server terminal.\n";
					_exit(0);
				} else {
					ReceiveAndprintData(servfd);
					epoll_ctl(epfd, EPOLL_CTL_MOD, cur->Fd, &ServFd);
				}
			} else {
				errlog("Unspect Error");
			}
		}
	}
}

void sigpipe_handler(int) {
	cout << "Server terminal.\n";
	_exit(0);
}

int ReceiveAndprintData(int servfd) {
	uint32_t n;
	Read(servfd, &n, sizeof(uint32_t));
	n = ntohl(n);
	if(n < 0 || n > MAXIMUM) {
		errlog("Data receive len Error : " + to_string(n));
		return FAILURE;
	}
	char buf[n+1];
	int readlen = Read(servfd, buf, n);
	buf[n] = '\0';
	if(readlen < n) {
		errlog("Data receive to less");
		return FAILURE;
	}
		
	cout << "receive data :" << buf << endl;
	return SUCCESS;
}