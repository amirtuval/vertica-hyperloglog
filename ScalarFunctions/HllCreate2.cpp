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
void* createHll();
void deleteHll(void* hll);
void addItemToHll(void* hll, const VString& item);
void getHllString(void* hll, VString& result);

class exception;

/*
 * This is a simple function that adds two integers and returns the result
 */
class HllCreate2 : public ScalarFunction
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
          const SizedColumnTypes& typeMetadata = argReader.getTypeMetaData();
          for(unsigned int i = 0; i < typeMetadata.getColumnCount(); i++) {
            if (!typeMetadata.getColumnType(i).isVarchar()){
              vt_report_error(0, "Function only accept arguments of type varchar");
            }
          }

            // While we have inputs to process
            do {
              void* hll = createHll();

              for(unsigned int i = 0; i < argReader.getNumCols(); i++) {
                const VString& item = argReader.getStringRef(i);

                addItemToHll(hll, item);
              }

              getHllString(hll, resWriter.getStringRef());
              deleteHll(hll);

              resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class HllCreate2Factory : public ScalarFunctionFactory
{
protected:
    int mMaxResultLength;

public:

    HllCreate2Factory() {
        mMaxResultLength = resultLength();
    }

    // return an instance of Add2Ints to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { return vt_createFuncObject<HllCreate2>(interface.allocator); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addVarchar();
    }

    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes) {
        outputTypes.addVarchar(mMaxResultLength);
    }
};

RegisterFactory(HllCreate2Factory);
