drop library libverticahll cascade;

CREATE LIBRARY libverticahll as '/vagrant/vertica-hyperloglog/build/libverticahll.so';

Create aggregate function HLL_COMPUTE as language 'C++' NAME 'HllComputeFactory' Library libverticahll;
Create aggregate function HLL_CREATE as language 'C++' NAME 'HllCreateFactory' Library libverticahll;
Create aggregate function HLL_MERGE as language 'C++' NAME 'HllMergeFactory' Library libverticahll;
Create aggregate function HLL_MERGE_COMPUTE as language 'C++' NAME 'HllMergeComputeFactory' Library libverticahll;