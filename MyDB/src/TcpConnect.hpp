#ifndef TCP_CONNECT_HPP_
#define TCP_CONNECT_HPP_

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/sysinfo.h>
#include "my_lib.hpp"

struct SockAddr;
int TcpListen(const string & Addr, const string & port, socklen_t *addrlen);
int TcpConnect(const string & Addr, const string & port);
void Setsockopt(int fd, int level, int opt, const void * optval, socklen_t optlen);
void Listen(int fd);
int Accept(int sockfd, SockAddr & sa);

int GetNProcs();
static int NPROCS = -1; //Num of cpus

struct SockAddr {
	sockaddr_storage * SA;
	socklen_t len;
	SockAddr() : len(0){
		SA = new sockaddr_storage;
		memset(SA, 0, sizeof(sockaddr_storage));
	}
	~SockAddr() {
		delete SA;
	}
	SockAddr(SockAddr && mv) {
		SA = mv.SA;
		len = mv.len;
		mv.len = 0;
		mv.SA = nullptr;
	}
	SockAddr(const SockAddr & cpy) {
		SA = new sockaddr_storage;
		memcpy(SA, cpy.SA, sizeof(sockaddr_storage));
		len = cpy.len;
	}
	const SockAddr & operator=(SockAddr && mv) {
		SA = mv.SA;
		mv.SA = nullptr;
		return *this;
	}
	const SockAddr & operator=(const SockAddr & cpy) {
		SA = new sockaddr_storage;
		memcpy(SA, cpy.SA, sizeof(sockaddr_storage));
		len = cpy.len;
		return *this;
	}
};



int TcpListen(const string & Addr, const string & port, socklen_t *addrlen) {
	int listenfd, n;
	const int on = 1;
	struct addrinfo Hints, *Res, *SaveRes;

	memset(&Hints, 0, sizeof(struct addrinfo));
	Hints.ai_flags = AI_PASSIVE;
	Hints.ai_family = AF_UNSPEC;
	Hints.ai_socktype = SOCK_STREAM;

	if( (n = getaddrinfo(Addr.c_str(), port.c_str(), &Hints, &Res)) != 0 ) {
		if(n != EAI_SYSTEM)
			errlog(string("Tcp_listen ERROR : ") + gai_strerror(n));
		else {
			char buf[128];
			strerror_r(errno, buf, 128);
			errlog(string("Tcp_listen ERROR : ") + buf);
		}
	}
	SaveRes = Res;
	while(Res != NULL) {
		listenfd = socket(Res->ai_family, Res->ai_socktype, Res->ai_protocol);
		if(listenfd < 0)
			continue;
		Setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
		if(bind(listenfd, Res->ai_addr, Res->ai_addrlen) == 0) 
			break;
		close(listenfd);
	}

	if(listenfd < 0)
		errlog(string("Tcp_listen ERROR for Address: \"") + Addr + " : " + port + "\"");
	
	Listen(listenfd);;

	if(addrlen)
		*addrlen = Res->ai_addrlen;
	freeaddrinfo(SaveRes);
	return listenfd;
}

inline void Setsockopt(int fd, int level, int opt, const void * optval, socklen_t optlen) {
	if(setsockopt(fd, level, opt, optval, optlen) != 0) {
		char buf[128];
		strerror_r(errno, buf, 128);
		errlog(string("Setsockopt ERROR : ") + buf);
	}
}

void Listen(int fd) {
	if(listen(fd, GetNProcs()) < 0) {
		char buf[128];
		strerror_r(errno, buf, 128);
		errlog(string("Listen ERROR : ") + buf);
	}	
}

int GetNProcs() {
	if(NPROCS == -1)
		NPROCS = get_nprocs();
	return NPROCS;
}

int Accept(int sockfd, SockAddr & sa) {
	int retfd;
	if( (retfd = accept(sockfd, (sockaddr*)sa.SA, &(sa.len))) == -1 ) {
		errlog(string("Accept ERROR"));
	}
	return retfd;
}

int TcpConnect(const string & Addr, const string & port) {
	int sockfd = -1, n;
	struct addrinfo hints, *res, *ressave;
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if( (n = getaddrinfo(Addr.c_str(), port.c_str(), &hints, &res)) != 0 ) {
		errlog("TcpConnect Error for Address: \"" + Addr + " : " + port + "\"" );
		return -1;
	}
	ressave = res;
	while(res != NULL) {
		sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if(sockfd < 0) 
			continue;
		if(connect(sockfd, res->ai_addr, res->ai_addrlen) == 0)
			break;
		close(sockfd);
		sockfd = -1;
		res = res->ai_next;
	}

	if(ressave == NULL) {
		errlog("TcpConnect Error for Address: \"" + Addr + " : " + port + "\"" );
	} else
		freeaddrinfo(ressave);
	
	return sockfd;
}


#endif