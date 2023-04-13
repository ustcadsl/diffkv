#1. specify the version of cmake
cmake_minimum_required(VERSION 3.10)

#2. define the name of project
project(ycsbc)

#3. specify c++ compiler version
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "-std=c++11 -g -Wall -O2 -pthread ")

#4. head file path
include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/core
  ${PROJECT_SOURCE_DIR}/../../include
  ${PROJECT_SOURCE_DIR}/../../dep/rocksdb/include
  ${PROJECT_SOURCE_DIR}/../../dep/rocksdb
  )

#5. third path link path
link_directories(
  ${PROJECT_SOURCE_DIR}/../../dep/rocksdb
  ${PROJECT_SOURCE_DIR}/../../build
  )

#6. source code file path
file(GLOB MAIN_FILE
  ${PROJECT_SOURCE_DIR}/ycsbc.cc
  )
file(GLOB SOURCE_FILE
  ${PROJECT_SOURCE_DIR}/core/*.cc
  ${PROJECT_SOURCE_DIR}/db/*.cc
  )

#7. define the executable
add_executable(${PROJECT_NAME} ${MAIN_FILE} ${SOURCE_FILE})
target_link_libraries(${PROJECT_NAME}
  pthread
  pebblesdb
  rocksdb
  diffkv
  snappy
  tbb
  zstd
  lz4
  bz2
  z
  dl
  )
