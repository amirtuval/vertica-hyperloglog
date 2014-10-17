#ifndef __BASE_H
#define __BASE_H

#include "Vertica.h"
#include "constants.hpp"

using namespace Vertica;
using namespace std;

int resultLength();

class HllAggregateFunctionBase : public AggregateFunction {
protected:
    int estimate(const VString& hllStr);

private:
    virtual void onTerminate(VString& hllStr, BlockWriter &resWriter) = 0;
    virtual void addItem(void* hll, const VString& item) = 0;

    virtual void initAggregate(
        ServerInterface &srvInterface, 
        IntermediateAggs &aggs);
    
    virtual void aggregate(
        ServerInterface &srvInterface, 
        BlockReader &argReader, 
        IntermediateAggs &aggs);

    virtual void combine(
        ServerInterface &srvInterface, 
        IntermediateAggs &aggs, 
        MultipleIntermediateAggs &aggsOther);

    virtual void terminate(
        ServerInterface &srvInterface, 
        BlockWriter &resWriter, 
        IntermediateAggs &aggs);

    InlineAggregate()
};

class SimpleHllAggregateFunctionBase : public HllAggregateFunctionBase {
    virtual void addItem(void* hll, const VString& item);
};

class MergeHllAggregateFunctionBase : public HllAggregateFunctionBase {
    virtual void addItem(void* hll, const VString& item);
};

template <class T>
class HllAggregateFunctionBaseFactory : public AggregateFunctionFactory {

protected:
    int mMaxResultLength;

public:

    HllAggregateFunctionBaseFactory() {
        mMaxResultLength = resultLength();
    }

    virtual void setReturnType(ColumnTypes &returnType) = 0;

    virtual void getPrototype(
        ServerInterface &srvInterface, 
        ColumnTypes &argTypes, 
        ColumnTypes &returnType) {
        argTypes.addVarchar();
        setReturnType(returnType);
    }

    virtual void getIntermediateTypes(
        ServerInterface &srvInterface,
        const SizedColumnTypes &inputTypes, 
        SizedColumnTypes &intermediateTypeMetaData) {
        intermediateTypeMetaData.addVarchar(mMaxResultLength);
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface) { 
        return vt_createFuncObject<T>(srvInterface.allocator); 
    }

};

#endif