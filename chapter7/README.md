hcb-v2 Chapter7 errata
=====================

* Create an HBase table named "HDI", before executing the "gradle executeHDIDataUpload" command for the "Running MapReduce jobs on HBase" recipe.
$ hbase shell
hbase(main):001:0> create 'HDI','ByCountry'

* Follow the "Getting started with Apache HBase" recipe to create an HBase table names "test", before executing the "gradle executeHBaseClient" command for the "Data random access using Java client APIs" recipe. 


