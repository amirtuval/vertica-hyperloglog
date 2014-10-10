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

void HllAggregateFunctionBase::initAggregate(
    ServerInterface &srvInterface, 
    IntermediateAggs &aggs) {
}
    
SerializedHyperLogLog* hllFromStr(const VString& str) {
    if (str.isNull()) {
        return new SerializedHyperLogLog(HLL_BIT_WIDTH);
    }

    return SerializedHyperLogLog::fromString(str.data());
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
                hll.add(input.data(), input.length());
            }
        } while (argReader.next());

        VString &result = aggs.getStringRef(0);
        SerializedHyperLogLog* currentHll = hllFromStr(result);
        hll.merge(*currentHll);
        delete currentHll;
        result.copy(hll.toString(result.data()));
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
            SerializedHyperLogLog* currentHll = SerializedHyperLogLog::fromString(aggsOther.getStringRef(0).data());
            hll->merge(*currentHll);
            delete currentHll;
        } while (aggsOther.next());

        result.copy(hll->toString(result.data()));
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