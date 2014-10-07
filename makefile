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
CXXFLAGS:=$(CXXFLAGS) -I $(SDK_HOME)/include -g -Wall -Wno-unused-value -shared -fPIC 

ifdef OPTIMIZE
## UDLs should be compiled with compiler optimizations in release builds
CXXFLAGS:=$(CXXFLAGS) -O3
endif

## Set to the desired destination directory for .so output files
BUILD_DIR?=$(abspath build)

## Set to a valid temporary directory
TMPDIR?=/tmp

## Set to the path to 
BOOST_INCLUDE ?= /usr/include
CURL_INCLUDE ?= /usr/include
ZLIB_INCLUDE ?= /usr/include
BZIP_INCLUDE ?= /usr/include

all: AggregateFunctions

$(BUILD_DIR)/.exists:
	test -d $(BUILD_DIR) || mkdir -p $(BUILD_DIR)
	touch $(BUILD_DIR)/.exists

###
# Aggregate Functions
###
AggregateFunctions: $(BUILD_DIR)/AggregateFunctions.so

$(BUILD_DIR)/AggregateFunctions.so: AggregateFunctions/*.cpp $(SDK_HOME)/include/Vertica.cpp $(SDK_HOME)/include/BuildInfo.h $(BUILD_DIR)/.exists
	$(CXX) $(CXXFLAGS) -o $@ AggregateFunctions/*.cpp $(SDK_HOME)/include/Vertica.cpp

clean:
	rm -f $(BUILD_DIR)/*.so 
