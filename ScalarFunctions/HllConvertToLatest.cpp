/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- C++ -*- */
/* 
 *
 * Description: Example User Defined Scalar Function: Add 2 ints
 *
 * Create Date: Apr 29, 2011
 */
#include "Vertica.h"

using namespace Vertica;

void convertToLatest(const VString& src, VString& dest);
int resultLength();

/*
 * This is a simple function that adds two integers and returns the result
 */
class HllConvertToLatest : public ScalarFunction
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
            if (argReader.getNumCols() != 1)
                vt_report_error(0, "Function only accept 1 argument, but %zu provided", 
                                argReader.getNumCols());

            // While we have inputs to process
            do {
                convertToLatest(argReader.getStringRef(0), resWriter.getStringRef());

                resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class HllConvertToLatestFactory : public ScalarFunctionFactory
{
protected:
    int mMaxResultLength;

public:

    HllConvertToLatestFactory() {
        mMaxResultLength = resultLength();
    }

    // return an instance of Add2Ints to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { return vt_createFuncObject<HllConvertToLatest>(interface.allocator); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addVarchar();
        returnType.addVarchar();
    }

    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes) {
        outputTypes.addVarchar(mMaxResultLength);
    }
};

RegisterFactory(HllConvertToLatestFactory);
