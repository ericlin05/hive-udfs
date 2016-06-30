package com.example;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class UDFHelloWorldExampleTest {
  
  @Test
  public void testUDF() {
    UDFHelloWorldExample example = new UDFHelloWorldExample();
    Assert.assertEquals("Hello world", example.evaluate(new Text("world")).toString());
  }
  
  @Test
  public void testUDFNullCheck() {
    UDFHelloWorldExample example = new UDFHelloWorldExample();
    Assert.assertNull(example.evaluate(null));
  }
}
