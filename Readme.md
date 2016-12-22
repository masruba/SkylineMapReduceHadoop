This repository contanins the implementation of the paper 

[Parallel computation of skyline and reverse skyline queries using mapreduce]
(http://dl.acm.org/citation.cfm?id=2556580)
Dataset: http://www.cs.ucr.edu/~iabsa001/files/gsod_readme.txt
The code is tested in Cloudera VM.

This readme file explains how to run source code in Cloudera VM. Some of the path 
in the source are hard coded. In order to run locally, you need to update the path
appropriately.

## Instructions

Sample commands to Run Range.java to compute min max value
------------------------------------------------------------
```
mkdir range_classes
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d range_classes/ Range.java
jar -cvf range.jar -C range_classes/ .
hadoop fs -rmr output
hadoop jar range.jar org.myorg.Range /user/cloudera/range/input /user/cloudera/range/output
hadoop fs -cat /user/cloudera/range/output/part-00000
```

Sample commands to Run Skyline.java to compute Local Skyline
------------------------------------------------------------
```
mkdir skyline_classes
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d skyline_classes/ Skyline.java
jar -cvf Skyline.jar -C skyline_classes/ .
hadoop fs -rmr /user/cloudera/range/output5000
hadoop jar Skyline.jar org.myorg.Skyline data5000.txt /user/cloudera/range/output5000
hadoop fs -cat /user/cloudera/range/output5000/part-00000
```

Sample commands to Run GlobalSkyline.java to compute Local Skyline:
------------------------------------------------------------
```
hadoop fs -cat /user/cloudera/range/output5000/vpn* >vpn
hadoop fs -cat /user/cloudera/range/output5000/filter* >filter
mkdir global_skyline_classes
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d global_skyline_classes/ GlobalSkyline.java
jar -cvf GlobalSkyline.jar -C global_skyline_classes/ .
hadoop fs -rmr /user/cloudera/range/globaloutput5000
hadoop jar GlobalSkyline.jar org.myorg.GlobalSkyline /user/cloudera/range/output5000/part* /user/cloudera/range/globaloutput5000
hadoop fs -cat /user/cloudera/range/globaloutput5000/part-00000
```
