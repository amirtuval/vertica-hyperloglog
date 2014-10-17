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
        return new SerializedHyperLogLog(HLL_BIT_WIDTH);
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

    hll->merge(*hll2);

    result.copy(hllStr1);
    updateStringFromHll(result, hll);

    delete hll;
    delete hll2;
}

void HllAggregateFunctionBase::aggregate(
    ServerInterface &srvInterface, 
    BlockReader &argReader, 
    IntermediateAggs &aggs) {
    try {
        SerializedHyperLogLog hll(HLL_BIT_WIDTH);

        do {
            const VString &input = argReader.getStringRef(0);
            if (!input.isNull()) {
                addItem((void*)&hll, input);
            }
        } while (argReader.next());

        VString &result = aggs.getStringRef(0);
        SerializedHyperLogLog* currentHll = hllFromStr(result);
        hll.merge(*currentHll);
        delete currentHll;
        updateStringFromHll(result, &hll);
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
            hll->merge(*currentHll);
            delete currentHll;
        } while (aggsOther.next());

        updateStringFromHll(result, hll);
        delete hll;
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
    phll->merge(*newHll);
    delete newHll;
}