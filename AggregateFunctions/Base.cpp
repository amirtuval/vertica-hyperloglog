/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: Average
 *
 */

#include <time.h> 
#include <sstream>
#include <iostream>

#include "SerializedHyperLogLog.hpp"

#include "Base.h"

int resultLength() {
    return ceil(pow(2, HLL_BIT_WIDTH) / 3.0) * 4 + 20;
}

void HllAggregateFunctionBase::initAggregate(
    ServerInterface &srvInterface, 
    IntermediateAggs &aggs) {
}
    
void updateStringFromHll(VString& str, SerializedHyperLogLog* hll) {
    char* cstr = hll->toString(str.data()); 
    str.copy(cstr, strlen(cstr) + 1); // +1 to include the \0 in the VString data
}   

SerializedHyperLogLog* hllFromStr(const VString& str) {
    if (str.isNull()) {
        return NULL;
    }

    const char* cstr = str.data();
    bool needFree = false;
    if (cstr[str.length() - 1] != '\0') {
        needFree = true;
        char* newcstr = (char*)malloc(str.length() + 1);
        strncpy(newcstr, str.data(), str.length());
        newcstr[str.length()] = '\0';
        cstr = newcstr;
    }

    SerializedHyperLogLog* result = SerializedHyperLogLog::fromString(cstr);

    if (needFree) {
        free((void*)cstr);
    }

    return result;
}   

void mergeTwoHlls(ServerInterface &srvInterface, const VString& hllStr1, const VString& hllStr2, VString& result) {
    SerializedHyperLogLog* hll = hllFromStr(hllStr1);
    SerializedHyperLogLog* hll2 = hllFromStr(hllStr2);

    if (hll == NULL) {
        hll = hll2;
        hll2 = NULL;
    }

    if (hll2 != NULL)
      hll->merge(*hll2);

    result.copy(hllStr1);

    if (hll != NULL)
        updateStringFromHll(result, hll);

    if (hll != NULL)
        delete hll;
    if (hll2 != NULL)
        delete hll2;
}

void HllAggregateFunctionBase::aggregate(
    ServerInterface &srvInterface, 
    BlockReader &argReader, 
    void* state) {
    try {
        do {
            const VString &input = argReader.getStringRef(0);
            if (!input.isNull()) {
                addItem(state, input);
            }
        } while (argReader.next());
    } catch(exception& e) {
        // Standard exception. Quit.
        vt_report_error(0, "Exception while processing aggregate: [%s]", e.what());
    }
}

void HllAggregateFunctionBase::combine(
    ServerInterface &srvInterface, 
    IntermediateAggs &aggs, 
    MultipleIntermediateAggs &aggsOther) {
    try {
        VString& result = aggs.getStringRef(0);
        SerializedHyperLogLog* hll = hllFromStr(result);

        // Combine all the other intermediate aggregates
        do {
            SerializedHyperLogLog* currentHll = hllFromStr(aggsOther.getStringRef(0));
            if (hll == NULL) {
                hll = currentHll;
                currentHll = NULL;
            } else if (currentHll != NULL) {
                hll->merge(*currentHll);
            }

            if (currentHll != NULL)
                delete currentHll;
        } while (aggsOther.next());

        if (hll != NULL) {
            updateStringFromHll(result, hll);
            delete hll;
        }

    } catch(exception& e) {
        // Standard exception. Quit.
        vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
    }
}

void HllAggregateFunctionBase::terminate(
    ServerInterface &srvInterface, 
    BlockWriter &resWriter, 
    IntermediateAggs &aggs) {
    try {
        VString& agg = aggs.getStringRef(0);

        onTerminate(agg, resWriter);
    } catch(exception& e) {
        // Standard exception. Quit.
        vt_report_error(0, "Exception while computing aggregate output: [%s]", e.what());
    }
}

int HllAggregateFunctionBase::estimate(const VString& hllStr) {
    SerializedHyperLogLog* hll = hllFromStr(hllStr);

    if (hll == NULL)
        return 0;

    int result = hll->estimate();
    delete hll;

    return result;
}

void SimpleHllAggregateFunctionBase::addItem(void* hll, const VString& item) {
    SerializedHyperLogLog* phll = (SerializedHyperLogLog*)hll;
    phll->add(item.data(), item.length());
}

void MergeHllAggregateFunctionBase::addItem(void* hll, const VString& item) {
    SerializedHyperLogLog* phll = (SerializedHyperLogLog*)hll;
    SerializedHyperLogLog* newHll = hllFromStr(item);

    if (newHll != NULL) {
        phll->merge(*newHll);
        delete newHll;
    }
}

void* HllAggregateFunctionBase::createAggregateState() {
    return (void*)new std::map<char*, SerializedHyperLogLog*>();
}

void  HllAggregateFunctionBase::updateResultFromState(IntermediateAggs& intAggs, void* state) {
    try {
        SerializedHyperLogLog* hll = (SerializedHyperLogLog*)state;    
        VString &result = intAggs.getStringRef(0);
        SerializedHyperLogLog* currentHll = hllFromStr(result);

        if (currentHll != NULL) {
            hll->merge(*currentHll);
            delete currentHll;
        }
        updateStringFromHll(result, hll);
    } catch(exception& e) {
        // Standard exception. Quit.
        vt_report_error(0, "Exception while processing aggregate: [%s]", e.what());
    }
}

void  HllAggregateFunctionBase::deleteAggregateState(void* state) {
    std::map<char*, SerializedHyperLogLog*>& map = *(std::map<char*, SerializedHyperLogLog*>*)state;
    for (std::map<char*, SerializedHyperLogLog*>::iterator it=map.begin(); it!=map.end(); ++it) {
        delete it->second;
    }
        
    delete &map;
}

void* HllAggregateFunctionBase::getAggregateState(void* state, char* aggPtr) {
    std::map<char*, SerializedHyperLogLog*>& map = *(std::map<char*, SerializedHyperLogLog*>*)state;
    std::map<char*, SerializedHyperLogLog*>::iterator it = map.find(aggPtr);
    SerializedHyperLogLog* hll;
    if (it == map.end()) {
        hll = new SerializedHyperLogLog(HLL_BIT_WIDTH);
        map[aggPtr] = hll;
    } else {
        hll = it->second;
    }

    return (void*)hll;
}