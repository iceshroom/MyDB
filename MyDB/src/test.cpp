#include "LRU.hpp"
#include <iostream>

int main() {
	LRU test(10);
	int len = 20;
	
	for(int i = 0; i < len; ++i) {
		hash_str key = to_string(i);
		item data(string("Hello World!") + key.getcstr(), SHORT);
		test.insert(key, data);
	}

	for(int i = 0; i < 20; ++i) {
		hash_str key = to_string(i);
		item data = test[key];
		if(data.getData() != nullptr)
			cout << data.getData() << endl;
	}
	return 0;
}