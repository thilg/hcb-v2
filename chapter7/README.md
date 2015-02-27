## Chapter7 errata
-----------------------

#### "Data random access using Java client APIs" recipe
  * First follow the "Getting started with Apache HBase" recipe to create an HBase table names "test", before executing the "gradle executeHBaseClient" command for the "Data random access using Java client APIs" recipe. 

#### "Running MapReduce jobs on HBase" recipe
  * First create two HBase tables named "HDI" and "HDIResult" as follows, before executing the "gradle executeHDIDataUpload" command for the "Running MapReduce jobs on HBase" recipe.
```
$ hbase shell
hbase(main):001:0> create 'HDI','ByCountry'
hbase(main):002:0> create 'HDIResult','data'
```
  * Execute the `gradle build uberjar` command inside the chpater7 folder to create a jar with all the dependencies.
```
$ gradle build uberjar
```
  * Issue the following command from the chapter7 folder to execute the MapReduce computation. Substitute the values for the Zookeeper Quorum (defaults to `localhost`) and root Znode for HBase (defaults to `/hbase-unsecure`).
```
$ hadoop jar build/libs/hcb-c7-samples-uber.jar \
chapter7.hbase.AverageGINByCountryCalculator \
(One or more servers from Zookeeper Quorum) (root znode for HBase)
```


