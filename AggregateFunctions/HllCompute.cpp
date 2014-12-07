/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: Average
 *
 */

#include <time.h> 
#include <sstream>
#include <iostream>

#include "Base.h"

class HllCompute : public SimpleHllAggregateFunctionBase {
    
    virtual void onTerminate(VString& hllStr, BlockWriter &resWriter) {
        resWriter.setInt(estimate(hllStr));
    }

    virtual void* createNewHll() { return createHll(); }
};


class HllComputeFactory : public HllAggregateFunctionBaseFactory<HllCompute> {
    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes) {
        outputTypes.addInt();
    }

    virtual void setReturnType(ColumnTypes& returnType) {
    	returnType.addInt();
	}
};

RegisterFactory(HllComputeFactory);

