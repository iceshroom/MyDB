#ifndef POINT_VEC_
#define POINT_VEC_
#include "my_lib.hpp"

class PointVec {
private:
	typedef void * Point;
	Point * v;
	size_t len;
	size_t v_size;

	PointVec(const PointVec & cpy);
	const PointVec &  operator=(const PointVec & cpy);
public:
	PointVec() : v_size(4), len(0) {
		v = static_cast<Point *>(Malloc(sizeof(Point)*4));
	}
	~PointVec() {
		free(v);
	}
	PointVec(PointVec && cpy) {
		v = cpy.v;
		len = cpy.len;
		v_size = cpy.v_size;
		cpy.v = nullptr;
	}

	const PointVec & operator=(PointVec && cpy) {
		v = cpy.v;
		len = cpy.len;
		v_size = cpy.v_size;
		cpy.v = nullptr;
		return *this;
	}

	Point & operator[](size_t n) {
		return v[n];
	}

	Point & back() {
		return v[len-1];
	}

	bool empty() const {
		return len == 0;
	}

	size_t size() const {
		return len;
	}

	void push_back(Point n) {
		v[len] = n;
		++len;
		if(v_size == len) {
			Point * old = v;
			v_size<<=1;
			v = static_cast<Point *>(Malloc(sizeof(Point)*(v_size)));
			memcpy(v, old, len*sizeof(Point));
			free(old);
		}
	}

	void pop_back() {
		if(len == 0)
			return;
		--len;
		if(v_size > 4 && (v_size>>2) > len) {
			Point * old = v;
			v_size>>=1;
			v = static_cast<Point *>(Malloc(sizeof(Point)*(v_size)));
			memcpy(v, old, len*sizeof(Point));
			free(old);
		}
	}
};

#endif