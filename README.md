## Differentiated Key-Value Storage Management for Balanced I/O Performance
### Introduction
Modern key-value (KV) stores adopt the LSM-tree as the core data structure
for managing KV pairs, but suffer from high write and read amplifications.
Existing LSM-tree optimizations often make design trade-offs and are unable to
simultaneously achieve high performance in writes, reads, and scans.  To
resolve the design tensions, we propose `DiffKV`, which builds on KV
separation to carefully manage the ordering for keys and values.  `DiffKV`
manages keys using the conventional LSM-tree with *fully-sorted ordering*
(within each level of the LSM-tree), while managing values with 
*partially-sorted ordering* with respect to the fully-sorted ordering of keys
in a coordinated way for preserving high scan performance.  We further propose
fine-grained KV separation to differentiate KV pairs by size, so as to realize
balanced performance under mixed workloads. Experimental results show that
`DiffKV` can simultaneously achieve nearly the best performance in all aspects 
among existing LSM-tree-optimized KV stores.

### Publications
* Yongkun Li, Zhen Liu, Patrick P. C. Lee, Jiayu Wu, Yinlong Xu, Yi Wu, Liu Tang, Qi Liu, Qiu Cui. Differentiated Key-Value Storage Management for Balanced I/O Performance

### Overview
The prototype is written in C++ based on [Titan](https://github.com/tikv/titan) (A RocksDB Plugin to Reduce Write Amplification).  
And we also commit our implementation to mainstream in [Titan](https://docs.pingcap.com/tidb/stable/titan-overview).

#### Minimal Requirement
Minimal setup to test the prototype:
* Ubuntu 18.04 LTS


## Build and Test
### Requirements
* Tools:
    * [CMake](https://cmake.org/download/) 3.14.5+
* Libraries:
    * [boost](https://www.boost.org/users/download/) 1.65.0+(required)
    * [gflags](https://gflags.github.io/gflags/) 2.0+ (required for testing and benchmark code)
    * [zlib](http://www.zlib.net/) 1.2.8+ (optional)
    * [bzip2](http://www.bzip.org/) 1.0.6+ (optional)
    * [lz4](https://github.com/lz4/lz4) r131+ (optional)
    * [snappy](http://google.github.io/snappy/) 1.1.3+ (optional)
    * [zstandard](http://www.zstd.net) 0.5.1+ (optional)
### Clone repository
```
$ git clone --recursive https://github.com/ustcadsl/diffkv.git
```
### Build library
```
# Build rocksdb
$ cd dep/rocksdb
$ make static_lib -j

# Build diffkv
$ mkdir -p build && cd build
$ cmake .. -DROCKSDB_DIR=$(pwd)/../dep/rocksdb  -DCMAKE_BUILD_TYPE=Release 
$ make diffkv -j
```


### Build bench tools
Install pebblesDB
* [PebblesDB](https://github.com/utsaslab/pebblesdb)

generate workloads by YCSB-C, the C++ version of YCSB
* [YCSB-C](https://github.com/basicthinker/YCSB-C)  

```
$ cd bench_tools/YCSB-C
$ sudo apt-get install libtbb-dev
$ mkdir -p build && cd build 
$ cmake .. && make -j
```
### Run tests with ycsbc
See help message
```
$ ./ycsbc -help
# output
Usage: ./ycsbc [options]
Options:
  -threads n: execute using n threads (default: 1)
  -db dbname: specify the name of the DB (rocksdb/pebblesdb/titan/diffkv)
  -phase load/run: load the database or run the benchamark
  -dbfilename path: specify the path of the database (make sure path exist
  -configpath path: specify the path of the config file (templetes config fi-
                    les in configDir directory )
  -P propertyfile: load properties from the given file. Multiple files can be
                   specified, and will be processed in the order specified
```
### Load the database
```
# load database
$ ./ycsbc -db diffkv -dbfilename #path -threads 16 -P workloads/workloadpareto1KB100GB.spec  -phase load -configpath configDir/diffkv_config.ini
```
### Run benchmarks based on the database
```
$ ./ycsbc -db diffkv -dbfilename #path -threads 16 -P workloads/workloadpareto1KBcorea100GB.spec  -phase run -configpath configDir/diffkv_config.ini

```
### Configuration instructions
```
# sepBeforeFlush: control whether open the KV separation
# smallThresh: equivalent to value_small in paper
# midThresh: equivalent to value_large in paper
# gcRatio: equivalent to gc_threshold in paper
# runGC: control whether to enable gc
# levelMerge: equivalent to compaction-triggered merge in paper
# rangeMerge: equivalent to scan-optimized merge in paper
# lazyMerge: equivalent to lazy merge in paper
# maxSortedRuns: equivalent to max_sorted_run in paper

```
