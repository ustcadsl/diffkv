CC=g++
LD_LIBRARY_ROCKSDB=$(shell pwd)/../../dep/rocksdb/include/
LD_LIBRARY_DiffKV= $(shell pwd)/../../include/
INCLUDES= -I ./ -I $(LD_LIBRARY_ROCKSDB) -I $(LD_LIBRARY_DiffKV)
CFLAGS=-std=c++11 -g -Wall -pthread
LDFLAGS=$(shell pwd)/../../build/libdiffkv.a $(shell pwd)/../../dep/rocksdb/librocksdb.a -lzstd -lpthread -ltbb -llz4 -lbz2 -lhiredis -lpebblesdb -ltitan -lrocksdb -lsnappy -lz -ldl
SUBDIRS=core db redis
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)
.PHONY: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
		done
	$(RM) $(EXEC)



