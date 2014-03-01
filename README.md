AISImport
=========

This [MapReduce](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html) job imports raw AIS ([Automatic Identification System](http://www.marinecadastre.gov/AIS/default.aspx)) data and partitions it based on time into Hadoop/HDFS.

### Data Preparation

Import the [Miami Port Sample GeoDatabase](ftp://ftp.csc.noaa.gov/temp/MarineCadastre/AIS.SampleData.zip) into ArcMap.

Add `CSVToolbox.pyt` (Thanks [@pheede](https://twitter.com/pheede)) to ArcMap and export the Broadcast layer into a local TSV file (this will take a while)

![TSVToolbox](https://dl.dropboxusercontent.com/u/2193160/AISImportTSVToolbox.png)

Put the TSV file in HDFS:

```
$ hadoop fs -put broadcast.tsv broacast.tsv
```

Export the `Voyage` table in the GeoDatabase into a local dbf file.

![VoyageExport](https://dl.dropboxusercontent.com/u/2193160/AISImportExportVoyage.png)

Put the DBF file in HDFS

```
$ hadoop fs -put voyage.dbf voyage.dbf
```

The import job use the voyage records to extract the ship draught and augments each associated record with that information.

Prepare HDFS to import the data. All the data will be partitioned in time using the Hadoop file system in the form `/ais/YYYY/MM/DD/HH/xxxxxxx` where `YYYY` is the year, `MM` is the month, `DD` is the day, `HH` is the hour and `xxxxxxx` is a [UUID](http://en.wikipedia.org/wiki/Universally_unique_identifier) string.

```
$ sudo -u hdfs hdfs -mkdir /ais
$ sudo -u hdfs hdfs -chmod a+rw /ais
```

### Build

This project uses [maven](http://maven.apache.org/) to build it and depends on the [Shapefile project](https://github.com/mraad/Shapefile).

```
$ mvn clean package
```

### Import The Data

```
$ hadoop jar target/AISImport-1.1-job.jar broadcast.tsv output
```

This will launch a map reduce job and based on each record timestamp value, that record will be appended to the appropriate file whose path was previously discussed. This partitioning is possible to to the reduce nature of the job and the [MutipleOutputs API](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/lib/output/MultipleOutputs.html)

### Analyzing The Data

This temporal partitioning is on purpose, as it maps (pun intended) beautifully to an externally [partitioned Hive table](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-PartitionedTables).

```
$ hive -f create-table.hql
```

To add the HDFS paths as hive partitions:

```
$ hadoop fs -ls -R /ais | awk -f alter-table.awk > /tmp/tmp.hql
$ hive -f /tmp/tmp.hql
```

Open a Hive shell, or better an [Impala](http://www.cloudera.com/content/cloudera/en/products-and-services/cdh/impala.html) shell and start analyzing:

```
$ impala-shell -r
[cloudera.localdomain:21000] > select hour,count(hour) from ais where year=2009 and month=1 group by hour order by hour limit 24;
Query: select hour,count(hour) from ais where year=2009 and month=1 group by hour order by hour limit 24
+------+-------------+
| hour | count(hour) |
+------+-------------+
| 0    | 58099       |
| 1    | 56452       |
| 2    | 54895       |
| 3    | 54998       |
| 4    | 53428       |
| 5    | 52946       |
| 6    | 52486       |
| 7    | 53219       |
| 8    | 55741       |
| 9    | 58349       |
| 10   | 58712       |
| 11   | 58305       |
| 12   | 58000       |
| 13   | 58082       |
| 14   | 57395       |
| 15   | 56932       |
| 16   | 57902       |
| 17   | 57362       |
| 18   | 57872       |
| 19   | 58795       |
| 20   | 59099       |
| 21   | 58509       |
| 22   | 58778       |
| 23   | 59205       |
+------+-------------+
Returned 24 row(s) in 6.49s
```

