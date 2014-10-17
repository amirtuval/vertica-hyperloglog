/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- C++ -*- */
/* 
 *
 * Description: Example User Defined Scalar Function: Add 2 ints
 *
 * Create Date: Apr 29, 2011
 */
#include "Vertica.h"

using namespace Vertica;

int resultLength();
void mergeTwoHlls(ServerInterface &srvInterface, const VString& hllStr1, const VString& hllStr2, VString& result);

class exception;
using namespace Vertica;

/*
 * This is a simple function that adds two integers and returns the result
 */
class HllMerge2 : public ScalarFunction
{
public:

    /*
     * This method processes a block of rows in a single invocation.
     *
     * The inputs are retrieved via argReader
     * The outputs are returned via resWriter
     */
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &argReader,
                              BlockWriter &resWriter)
    {
        try {
            // Basic error checking
            if (argReader.getNumCols() != 2)
                vt_report_error(0, "Function only accept 2 arguments, but %zu provided", 
                                argReader.getNumCols());

            // While we have inputs to process
            do {
                const VString& a = argReader.getStringRef(0);
                const VString& b = argReader.getStringRef(1);

                mergeTwoHlls(srvInterface, a, b, resWriter.getStringRef());

                resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class HllMerge2Factory : public ScalarFunctionFactory
{
protected:
    int mMaxResultLength;

public:

    HllMerge2Factory() {
        mMaxResultLength = resultLength();
    }

    // return an instance of Add2Ints to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { return vt_createFuncObject<HllMerge2>(interface.allocator); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addVarchar();
        argTypes.addVarchar();
        returnType.addVarchar();
    }

    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes) {
        outputTypes.addVarchar(mMaxResultLength);
    }
};

RegisterFactory(HllMerge2Factory);
