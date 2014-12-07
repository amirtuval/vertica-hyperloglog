#ifndef __BASE_H
#define __BASE_H

#include "Vertica.h"
#include "constants.hpp"

using namespace Vertica;
using namespace std;

int resultLength();

#define InlineAggregateWithState() \
    virtual void aggregateArrs(ServerInterface &srvInterface, void **dstTuples,\
                               int doff, const void *arr, int stride, const void *rcounts,\
                               int rcstride, int count, IntermediateAggs &intAggs,\
                               std::vector<int> &intOffsets, BlockReader &arg_reader) {\
        void* state = createAggregateState(); \
        char *arg = const_cast<char*>(static_cast<const char*>(arr));\
        std::map<char*, bool> foundAggs; \
        const uint8 *rowCountPtr = static_cast<const uint8*>(rcounts);\
        for (int i=0; i<count; ++i) {\
            vpos rowCount = *reinterpret_cast<const vpos*>(rowCountPtr);\
            char *aggPtr = static_cast<char *>(dstTuples[i]) + doff;\
            foundAggs[aggPtr] = true; \
            updateCols(arg_reader, arg, rowCount, intAggs, aggPtr, intOffsets);\
            aggregate(srvInterface, arg_reader, getAggregateState(state, aggPtr));\
            arg += rowCount * stride;\
            rowCountPtr += rcstride;\
        }\
        arg = const_cast<char*>(static_cast<const char*>(arr));\
        for (int i=0; i<count; ++i) {\
            vpos rowCount = *reinterpret_cast<const vpos*>(rowCountPtr);\
            char *aggPtr = static_cast<char *>(dstTuples[i]) + doff;\
            updateCols(arg_reader, arg, rowCount, intAggs, aggPtr, intOffsets);\
            if (foundAggs.find(aggPtr) != foundAggs.end()) { \
                updateResultFromState(intAggs, getAggregateState(state, aggPtr)); \
                foundAggs.erase(aggPtr); \
                if (foundAggs.size() == 0) \
                    break; \
            } \
            arg += rowCount * stride;\
            rowCountPtr += rcstride;\
        } \
        deleteAggregateState(state); \
    }\

class HllAggregateFunctionBase : public AggregateFunction {
protected:
    int estimate(const VString& hllStr);

private:
    virtual void onTerminate(VString& hllStr, BlockWriter &resWriter) = 0;
    virtual void addItem(ServerInterface &srvInterface, void* hll, const VString& item) = 0;

    virtual void initAggregate(
        ServerInterface &srvInterface, 
        IntermediateAggs &aggs);
    
    void* createAggregateState();
    void* getAggregateState(void* state, char* aggPtr);
    void  updateResultFromState(IntermediateAggs& intAggs, void* state);
    void  deleteAggregateState(void* state);

    virtual void aggregate(
        ServerInterface &srvInterface, 
        BlockReader &argReader, 
        void* state);

    virtual void combine(
        ServerInterface &srvInterface, 
        IntermediateAggs &aggs, 
        MultipleIntermediateAggs &aggsOther);

    virtual void terminate(
        ServerInterface &srvInterface, 
        BlockWriter &resWriter, 
        IntermediateAggs &aggs);

    InlineAggregateWithState()
};

class SimpleHllAggregateFunctionBase : public HllAggregateFunctionBase {

private:
    virtual void* createNewHll() = 0;
    virtual void addItem(ServerInterface &srvInterface, void* hll, const VString& item);
    
protected:
    virtual void* createHll();
    virtual void* createLegacyHll();
};

class MergeHllAggregateFunctionBase : public HllAggregateFunctionBase {
    virtual void addItem(ServerInterface &srvInterface, void* hll, const VString& item);
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