package com.example.udtf;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

/**
 * Returns one row for each key-value pair from the input map with two columns in each row: one for the key and another for the value
 *
 * This class provides basically the same functionality explode(map) function does in Hive's build in function
 *
 * Author: ericlin
 */
public class ExplodeMap extends GenericUDTF
{
    MapObjectInspector mapOI = null;

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException
    {
        if (args.length != 1) {
            throw new UDFArgumentException("explode_map() takes only one argument");
        }

        if (args[0].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentException("explode_map() takes a map as a parameter");
        }

        mapOI = (MapObjectInspector) args[0];
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("key");
        fieldNames.add("value");

        fieldOIs.add(mapOI.getMapKeyObjectInspector());
        fieldOIs.add(mapOI.getMapValueObjectInspector());

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    Object forwardObj[] = new Object[2];

    @Override
    public void process(Object[] o) throws HiveException
    {
        Map<?, ?> map = mapOI.getMap(o[0]);
        for (final Map.Entry<?, ?> entry: map.entrySet()) {
            forwardObj[0] = entry.getKey();
            forwardObj[1] = entry.getValue();
            forward(forwardObj);
        }
    }

    @Override
    public String toString()
    {
        return "explode_map";
    }
}
