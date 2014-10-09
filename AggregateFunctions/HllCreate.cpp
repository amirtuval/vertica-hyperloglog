/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: Average
 *
 */

#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

#include "constants.hpp"
#include "SerializedHyperLogLog.hpp"

using namespace Vertica;
using namespace std;

class HllCreate : public AggregateFunction {
    
    virtual void initAggregate(ServerInterface &srvInterface, 
                       IntermediateAggs &aggs) {
    }
    
    void aggregate(ServerInterface &srvInterface, 
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
            hll.toString(result.data());
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing aggregate: [%s]", e.what());
        }
    }

    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs, 
                         MultipleIntermediateAggs &aggsOther) {
        try {
            VString& result = aggs.getStringRef(0);
            SerializedHyperLogLog* hll = new SerializedHyperLogLog(HLL_BIT_WIDTH);

            // Combine all the other intermediate aggregates
            do {
                SerializedHyperLogLog* currentHll = SerializedHyperLogLog::fromString(aggsOther.getStringRef(0).data());
                hll->merge(*currentHll);
                delete currentHll;
            } while (aggsOther.next());

            hll->toString(result.data());
            delete hll;
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
        }
    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &resWriter, 
                           IntermediateAggs &aggs) {
        try {
            // Metadata about the type (to allow creation)
            VString& agg = aggs.getStringRef(0);
            VString &out = resWriter.getStringRef();
            out.copy(agg.data());
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while computing aggregate output: [%s]", e.what());
        }
    }

    InlineAggregate()
};


class HllCreateFactory : public AggregateFunctionFactory {
    private:
        int mMaxResultLength;

    public:

    HllCreateFactory() {
        mMaxResultLength = ceil(pow(2, HLL_BIT_WIDTH) / 3.0) * 4 + 20;
    }

    virtual void getPrototype(ServerInterface &srvInterface, 
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType) {
        argTypes.addVarchar();
        returnType.addVarchar();
    }

    // Provide return type length/scale/precision information (given the input
    // type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvInterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes) {
        outputTypes.addVarchar(mMaxResultLength);
    }

    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes, 
                                      SizedColumnTypes &intermediateTypeMetaData) {
        intermediateTypeMetaData.addVarchar(mMaxResultLength);
    }

    // Create an instance of the AggregateFunction
    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface) { 
        return vt_createFuncObject<HllCreate>(srvInterface.allocator); 
    }

};

RegisterFactory(HllCreateFactory);

