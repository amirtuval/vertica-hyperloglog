CREATE OR REPLACE LIBRARY libverticahll as '/home/amir/workspace/vertica-hyperloglog/build/libverticahll.so';

Create or replace aggregate function HLL_COMPUTE as language 'C++' NAME 'HllComputeFactory' Library libverticahll ;
Create or replace aggregate function HLL_CREATE as language 'C++' NAME 'HllCreateFactory' Library libverticahll;
Create or replace aggregate function HLL_CREATE_LEGACY as language 'C++' NAME 'HllCreateLegacyFactory' Library libverticahll;
Create or replace aggregate function HLL_MERGE as language 'C++' NAME 'HllMergeFactory' Library libverticahll;
Create or replace aggregate function HLL_MERGE_COMPUTE as language 'C++' NAME 'HllMergeComputeFactory' Library libverticahll;
Create or replace function HLL_MERGE2 as language 'C++' NAME 'HllMerge2Factory' Library libverticahll;
Create or replace function HLL_CONVERT_TO_LATEST as language 'C++' NAME 'HllConvertToLatestFactory' Library libverticahll NOT FENCED;
Create or replace function HLL_CREATE2 as language 'C++' NAME 'HllCreate2Factory' Library libverticahll NOT FENCED;