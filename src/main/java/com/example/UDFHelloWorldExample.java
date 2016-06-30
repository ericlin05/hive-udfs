package com.example;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.util.logging.Logger;

@Description(
  name="UDFHelloWorldExample",
  value="returns 'hello {word}', where {word} is the parameter passed into the UDF",
  extended="SELECT hello('world') from <table> limit 1;"
  )
public class UDFHelloWorldExample extends UDF {
  
  private final static Logger log = Logger.getLogger(UDFHelloWorldExample.class.getName());
  public Text evaluate(Text input) {
    if(input == null) return null;
    log.info("Test logger, it can be found in the mapper log");
    return new Text("Hello " + input.toString());
  }
}
