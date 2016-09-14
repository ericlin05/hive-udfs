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
hive> SELECT helloworld(a) FROM test;
+-------------------+--+
|       _c0         |
+-------------------+--+
| Hello there, test |
+-------------------+--+

hive> CREATE TEMPORARY FUNCTION udtf_test as 'com.example.udtf.ExplodeMap' USING JAR 'hdfs:///user/hive/jars/hive-udfs-1.0-SNAPSHOT.jar';

hive> SELECT udtf_test(b) FROM test limit 100;
+------+--------+--+
| key  | value  |
+------+--------+--+
| 1    | 3      |
+------+--------+--+
1 row selected (15.762 seconds)
```