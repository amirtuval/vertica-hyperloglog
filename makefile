############################
# Vertica Analytic Database
#
# Makefile to build example user defined functions
#
# To run under valgrind:
#   make RUN_VALGRIND=1 run
#
# Copyright (c) 2005 - 2012 Vertica, an HP company
############################

## Set to the location of the SDK installation
SDK_HOME?=/opt/vertica/sdk

CXX?=g++
CXXFLAGS:=$(CXXFLAGS) -I $(SDK_HOME)/include -I mysql-hyperloglog/libmysqlhll -I mysql-hyperloglog/libmysqlhll/cpp-hyperloglog/include -I mysql-hyperloglog/libmysqlhll/base64 -g -Wall -Wno-unused-value -shared -fPIC 

ifdef OPTIMIZE
## UDLs should be compiled with compiler optimizations in release builds
CXXFLAGS:=$(CXXFLAGS) -O3
endif

## Set to the desired destination directory for .so output files
BUILD_DIR?=$(abspath build)

## Set to a valid temporary directory
TMPDIR?=/tmp

all: libverticahll

$(BUILD_DIR)/.exists:
	test -d $(BUILD_DIR) || mkdir -p $(BUILD_DIR)
	touch $(BUILD_DIR)/.exists

libverticahll: $(BUILD_DIR)/libverticahll.so

$(BUILD_DIR)/libverticahll.so: AggregateFunctions/*.cpp ScalarFunctions/*.cpp $(SDK_HOME)/include/Vertica.cpp $(SDK_HOME)/include/BuildInfo.h $(BUILD_DIR)/.exists
	$(CXX) $(CXXFLAGS) -o $@ AggregateFunctions/*.cpp ScalarFunctions/*.cpp $(SDK_HOME)/include/Vertica.cpp

clean:
	rm -f $(BUILD_DIR)/*.so 
