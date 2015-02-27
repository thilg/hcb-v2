##hcb-v2 Chapter7 errata
=====================

###Data random access using Java client APIs recipe
  1. First follow the "Getting started with Apache HBase" recipe to create an HBase table names "test", before executing the "gradle executeHBaseClient" command for the "Data random access using Java client APIs" recipe. 

### Running MapReduce jobs on HBase recipe
  1. First create two HBase tables named "HDI" and "HDIResult" as follows, before executing the "gradle executeHDIDataUpload" command for the "Running MapReduce jobs on HBase" recipe.
```
$ hbase shell
hbase(main):001:0> create 'HDI','ByCountry'
hbase(main):002:0> create 'HDIResult','data'
```
  2. Use the following command to execute the MapReduce computation.


