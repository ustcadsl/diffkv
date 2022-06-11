CC=g++
CFLAGS=-std=c++11 -c -g -Wall -fgnu-tm
INCLUDE_LIBRARY_ROCKSDB=-I $(shell pwd)/../../../dep/rocksdb/include/ -I $(shell pwd)/../../../dep/rocksdb/
INCLUDE_LIBRARY_DiffKV =-I $(shell pwd)/../../../include/
INCLUDES= -I ../ $(INCLUDE_LIBRARY_ROCKSDB) $(INCLUDE_LIBRARY_DiffKV)
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

