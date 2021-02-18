#ifndef TYPE_HPP
#define TYPE_HPP

enum {
	NODATA = 0,
	SHORT = 64,
	INT = 128,
	LONG = 256,
	LLONG = 512,
	TABLEHEAD = 4096,
	MAXIMUM = 4096*4096
};

enum {
	KEY_LEN = 128
};

inline int getlevel(int size) {
	if(size == 0)
		return NODATA;
	else if(size > 0 && size <= SHORT)
		return SHORT;
	else if(size > SHORT && size <= INT)
		return INT;
	else if(size > INT && size <= LONG)
		return LONG;
	else if(size > LONG && size <= LLONG)
		return LLONG;
	else if(size > LLONG && size <= TABLEHEAD)
		return TABLEHEAD;
	else
		return MAXIMUM;
}

class item;
class FD;

class item {
private:
	int vlen;
	char * v;
	int level;

public:
	item(string data, int DataLevel) {
		v = new char[DataLevel];
		vlen = data.size();
		level = DataLevel;
		strncpy(v, data.c_str(), data.size());
	}

	item() {
		v = nullptr;
		vlen = 0;
		level = NODATA;
	}

	item(const char * ret, int n) {
		v = new char[n];
		level = getlevel(n);
		vlen = n;
		memcpy(v, ret, n);
	}

	item(const item & cpy) {
		v = new char[cpy.level];
		level = cpy.level;
		vlen = cpy.DataSize();
		memcpy(v, cpy.v, cpy.DataSize());
	}

	item(item && mv) {
		v = mv.v;
		mv.v = nullptr;
		level = mv.level;
		vlen = mv.vlen;
	}

	const item & operator=(item && mv) {
		delete [] v;
		v = mv.v;
		mv.v = nullptr;
		level = mv.level;
		vlen = mv.vlen;
		return *this;
	}

	~item() {
		delete [] v;
	}

	const item & operator=(const item & cpy) {
		v = new char[cpy.level];
		vlen = cpy.DataSize();
		level = cpy.level;
		memcpy(v, cpy.v, cpy.DataSize());
		return cpy;
	}

	int DataSize() const {
		return vlen;
	}

	int DataLevel() const {
		return level;
	}

	char * getData() {
		return v;
	}
};

class FD {
private:
	int fd;
public:

	FD() : fd(-1) {}

	FD(int n) {
		fd = n;
	}

	FD(FD && cpy) {
		fd = cpy.fd;
		cpy.fd = -1;
	}

	const FD & operator=(FD && cpy) {
		Close();
		fd = cpy.fd;
		cpy.fd = -1;
		return *this;
	}

	FD(const FD & cpy) {
		fd = dup(cpy.fd);
	}

	~FD() {
		if(fd != -1) {
			close(fd);
			fd = -1;
		}
	}
	
	const int & getfd() {
		return fd;
	}

	const FD & operator=(const FD & cpy) {
		Close();
		fd = dup(cpy.fd);
		return cpy;
	}

	bool openSuccess() {
		return fd != -1;
	}

	void Close() {
		if(fd != -1) {
			close(fd);
			fd = -1;
		}
	}
};

#endif