/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: Average
 *
 */

#include <time.h> 
#include <sstream>
#include <iostream>

#include "Base.h"

class HllCreateLegacy : public SimpleHllAggregateFunctionBase {
    
    virtual void onTerminate(VString& hllStr, BlockWriter &resWriter) {
        VString &out = resWriter.getStringRef();
        out.copy(hllStr);
    }

    virtual void* createNewHll() { return createLegacyHll(); }
};


class HllCreateLegacyFactory : public HllAggregateFunctionBaseFactory<HllCreateLegacy> {
    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes) {
        outputTypes.addVarchar(mMaxResultLength);
    }

    virtual void setReturnType(ColumnTypes& returnType) {
        returnType.addVarchar();
    }

};

RegisterFactory(HllCreateLegacyFactory);

