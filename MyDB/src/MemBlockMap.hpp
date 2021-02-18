#ifndef MemBlock_HPP
#define MemBlock_HPP

#include "my_lib.hpp"
#include "PointVec.hpp"
#include <pthread.h>
#include <iostream>


class MemBlock {
private:
	typedef size_t MemBlock_KEY;
	static constexpr unsigned long long data_max_size = 1024*1024*4; //4MB	
	struct node{
    	char color; //'r' or 'b'
    	MemBlock_KEY key;
    	PointVec data;
    	node * parent;
    	node * left;
    	node * right;
	};

	node * root;
    node * NIL;
	int size;
	size_t CurrentStoreSize;
	size_t AllocSize;
	pthread_mutex_t lock;

	void left_rotate(node * x) {
		node * y = x->right;
		x->right = y->left;
		if(y->left != NIL)
			y->left->parent = x;
		y->parent = x->parent;
		if(x->parent == NIL)
			root = y;
		else if(x == x->parent->left)
			x->parent->left = y;
		else x->parent->right = y;
		y->left = x;
		x->parent = y;
	}

	void right_rotate(node * x) {
		node * y = x->left;
		x->left = y->right;
		if(y->right != NIL)
			y->right->parent = x;
		y->parent = x->parent;
		if(x->parent == NIL)
			root = y;
		else if(x == x->parent->left)
			x->parent->left = y;
		else x->parent->right = y;
		y->right = x;
		x->parent = y;
	}

	void insert_fixup(node * z) {
		node * y;
		while(z->parent->color == 'r') {
			if(z->parent == z->parent->parent->left) {
				y = z->parent->parent->right;
				if(y->color == 'r') {
					z->parent->color = 'b';
					y->color = 'b';
					z->parent->parent->color = 'r';
					z = z->parent->parent;
					continue;
				} else if (z == z->parent->right) {
					z = z->parent;
					left_rotate(z);
				}
				z->parent->color = 'b';
				z->parent->parent->color = 'r';
				right_rotate(z->parent->parent);
			} else {
				y = z->parent->parent->left;
				if(y->color == 'r') {
					z->parent->color = 'b';
					y->color = 'b';
					z->parent->parent->color = 'r';
					z = z->parent->parent;
					continue;
				} else if (z == z->parent->left) {
					z = z->parent;
					right_rotate(z);
				}
				z->parent->color = 'b';
				if(z->parent->parent == NIL) return;
				z->parent->parent->color = 'r';
				left_rotate(z->parent->parent);
			}
		}
		NIL->color = 'b';
		root->color = 'b';
	}

	void delete_fixup(node * x) {
		node * w;
		while(x != root && x->color == 'b'){
			if(x == x->parent->left) {
				w = x->parent->right;
				if(w->color == 'r'){
					w->color = 'b';
					x->parent->color = 'r';
					left_rotate(x->parent);
					w = x->parent->right;
				}
				if(w->left->color == 'b' && w->right->color == 'b') {
					w->color = 'r';
					x = x->parent;
					continue;
				} else if (w->right->color == 'b') {
					w->left->color = 'b';
					w->color = 'r';
					right_rotate(w);
					w = x->parent->right;
				}
				w->color = x->parent->color;
				x->parent->color = 'b';
				w->right->color = 'b';
				left_rotate( x->parent);
				x = root;
			} else {
				w = x->parent->left;
				if(w->color == 'r'){
					w->color = 'b';
					x->parent->color = 'r';
					right_rotate(x->parent);
					w = x->parent->left;
				}
				if(w->right->color == 'b' && w->left->color == 'b') {
					w->color = 'r';
					x = x->parent;
					continue;
				} else if (w->left->color == 'b') {
					w->right->color = 'b';
					w->color = 'r';
					left_rotate(w);
					w = x->parent->left;
				}
				w->color = x->parent->color;
				x->parent->color = 'b';
				w->left->color = 'b';
				right_rotate( x->parent);
				x = root;
			}
		}
		NIL->parent = NIL;
		x->color = 'b';
	}

	node * findout(MemBlock_KEY key) {
		node * x = root;
		while(x != NIL) {
			if(key < x->key)
				x = x->left;
			else if(key > x->key)
				x = x->right;
			else
				break;
		}
		return x;
	}

    void transplant(node * u, node * v) {
		if(u->parent == NIL)
			root = v;
		else if(u == u->parent->left)
			u->parent->left = v;
		else u->parent->right = v;
		v->parent = u->parent;
	}

    node * tree_minimum(node * x) {
		while(x->left != NIL)
			x = x->left;
		return x;
	}


	size_t MemBlock_getsize() {
		return size;
	}

	node * make_MemBlock_node(MemBlock_KEY key) {
		node * newnode = (node *)Malloc(sizeof(node));
		newnode->color = 'r';
		newnode->data = PointVec();
		newnode->key = key;
		newnode->left = NIL;
		newnode->right = NIL;
		newnode->parent = NIL;
		return newnode;
	}

	void free_MemBlock_node(node * to_free) {
			PointVec & del = to_free->data;
			while(!del.empty()) {
				free(del.back());
				del.pop_back();
			}
			to_free->data.~PointVec();
			free(to_free);
	}

	void MemBlock_insert(void * Insert) {
		Insert = static_cast<char*>(Insert)-16;
		size_t l = (static_cast<size_t*>(Insert))[0], r = (static_cast<size_t*>(Insert))[1];
		if(l != r)
			abort();
		++size;
		CurrentStoreSize += l;
		node * to_insert = findout(l);
		if(to_insert != NIL) {
			to_insert->data.push_back(Insert);
			return;
		}
		to_insert = make_MemBlock_node(l);
		to_insert->data.push_back(Insert);
		node * y = NIL;
		node * x = root;
		while(x != NIL) {
			y = x;
		if(to_insert->key < x->key)
				x = x->left;
			else
				x = x->right;
		}
		to_insert->parent = y;
		if(y == NIL)
			root = to_insert;
		else if(to_insert->key < y->key)
			y->left = to_insert;
		else y->right = to_insert;
		insert_fixup(to_insert);
		return;
	}
    
    void * MemBlock_getBlock(MemBlock_KEY key_to_find) {
		key_to_find += 16;
    	node * Find = findout(key_to_find);
		void * ret = nullptr;
    	if(Find != NIL && !Find->data.empty()) {
			ret = Find->data.back();
			Find->data.pop_back();
			CurrentStoreSize -= key_to_find;
			--size;
		}
		if(ret == nullptr) {
			ret = Malloc(key_to_find);
			AllocSize += key_to_find;
			(static_cast<size_t*>(ret))[0] = key_to_find;
			(static_cast<size_t*>(ret))[1] = key_to_find;
		}
		if(ret != nullptr) {
			ret = static_cast<char*>(ret)+16;
		}
		return ret;
    }
    
    size_t MemBlock_count(MemBlock_KEY find) {
    	const PointVec & Find = findout(find)->data;
    	return Find.size();
    }
    
    int _count(node * here) {
    	if(here != NIL) {
    		return _count(here->right) + _count(here->left) + 1;
    	}
    	return 0;
    }
    
    int MemBlock_debug_countnode() {
    	return _count(root);
    }

	void shrink(node * here) {
		if(here != NIL) {
			PointVec & to = here->data;
			int n = (to.size()/3)<<1;
			for(int i = 0; i < n; ++i) {
				free(to.back());
				to.pop_back();
			}
			shrink(here->left);
			shrink(here->right);
		}
	}
    
    void _destroy(node * here) {
    	if(here != NIL) {
    		_destroy(here->left);
    		_destroy(here->right);
    		free_MemBlock_node(here);
    	}
    }

	

	MemBlock(const MemBlock & cpy);
	const MemBlock & operator=(const MemBlock & cpy);
	MemBlock(MemBlock && mv);
public:
	MemBlock() {
		AllocSize = 0;
		CurrentStoreSize = 0;
		pthread_mutex_init(&lock, NULL);
		NIL = make_MemBlock_node(0);
		NIL->color = 'b';
		root = NIL;
		size = 0;
	}
	~MemBlock() {
		pthread_mutex_lock(&lock);
    	_destroy(root);
    	free_MemBlock_node(NIL);
		pthread_mutex_unlock(&lock);
		pthread_mutex_destroy(&lock);
    }

	void * operator[] (size_t n) {
		pthread_mutex_lock(&lock);
		void * ret = MemBlock_getBlock(n);
		pthread_mutex_unlock(&lock);
		return ret;
	}

	void ReturnMemBlock(void * block) {
		if(block == nullptr)
			return;
		pthread_mutex_lock(&lock);
		MemBlock_insert(block);
		size_t AllocOutSize = AllocSize - CurrentStoreSize;
		if(AllocOutSize < (AllocSize >> 2)) 
			shrink(root);
		pthread_mutex_unlock(&lock);
	}

};

#endif