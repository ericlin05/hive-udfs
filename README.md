# Simple Hive UDF Examples

## Compile

```
mvn compile
```

## Test

```
mvn test
```

## Build
```
mvn package
```

## Run

```
Put JAR file hive-udfs-1.0-SNAPSHOT.jar into HDFS, for example, under /user/hive/jars
Start Hive CLI or Beeline:

%> hive

hive> SELECT * FROM test;
+---------+---------+--+
| test.a  | test.b  |
+---------+---------+--+
| test    | {1:3}   |
+---------+---------+--+

hive> CREATE TEMPORARY FUNCTION hello_world as 'com.example.UDFHelloWorldExample' USING JAR 'hdfs:///user/hive/jars/hive-udfs-1.0-SNAPSHOT.jar';
hive> SELECT hello_world(a) FROM test;
+-------------------+--+
|       _c0         |
+-------------------+--+
| Hello there, test |
+-------------------+--+

hive> CREATE TEMPORARY FUNCTION explode_map as 'com.example.udtf.ExplodeMap' USING JAR 'hdfs:///user/hive/jars/hive-udfs-1.0-SNAPSHOT.jar';

hive> SELECT explode_map(b) FROM test limit 100;
+------+--------+--+
| key  | value  |
+------+--------+--+
| 1    | 3      |
+------+--------+--+
1 row selected (15.762 seconds)


hive> SELECT * FROM test3;
+----------+----------+----------+--+
| test3.a  | test3.b  | test3.c  |
+----------+----------+----------+--+
| 1        | 2        | 3        |
| 1        | 2        | 4        |
| 1        | 2        | 5        |
| 1        | 1        | 5        |
| 1        | 1        | 3        |
| 4        | 5        | 6        |
| 4        | 5        | 7        |
+----------+----------+----------+--+

hive> CREATE TEMPORARY FUNCTION group_map as 'com.example.udtf.ExplodeMap' USING JAR 'hdfs:///user/hive/jars/hive-udfs-1.0-SNAPSHOT.jar';

hive> select a, group_map(c, b) from test3 group by a;
+----+----------------+--+
| a  |      _c1       |
+----+----------------+--+
| 1  | {3:2,4:2,5:2}  |
| 4  | {6:5,7:5}      |
+----+----------------+--+
```