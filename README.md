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
Start Hive CLI or Beeline:

%> hive
hive> ADD JAR /path/to/assembled.jar;
hive> CREATE TEMPORATY FUNCTION helloworld as 'com.example.UDFHelloWorldExample';
hive> SELECT helloworld(column1) FROM table_name limit 100;

```
