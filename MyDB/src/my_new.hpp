#ifndef MY_NEW_
#define MY_NEW_

#include "MemBlockMap.hpp"
#include <new>
using namespace std;
static MemBlock dump;

void * operator new(size_t n) {
	if(n == 0)
		++n;
	while(true) {
		void * ret = dump[n];
		if(ret != nullptr) 
			return ret;
		new_handler Handler = get_new_handler();
		if(Handler)
			Handler();
		else
			throw bad_alloc();
	}
}

void * operator new[](size_t n) {
	if(n == 0)
		++n;
	while(true) {
		void * ret = dump[n];
		if(ret != nullptr) 
			return ret;
		new_handler Handler = get_new_handler();
		if(Handler)
			Handler();
		else
			throw bad_alloc();
	}
}

void operator delete(void * free) {
	if(free != nullptr)
		dump.ReturnMemBlock(free);
}

void operator delete[](void * free) {
	if(free != nullptr)
		dump.ReturnMemBlock(free);
}

#endif