package com.example.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * This class will group key value pairs and return a Map based on group by fields
 *
 * For example, query "SELECT id, GROUP_MAP(cid, eid) FROM table GROUP BY id"
 *
 * Will be able to return the following values:
 *
 * 1 -> {1:1,1:2,1:3,4:5}
 *
 * @author Eric Lin
 */
public final class GroupMap extends UDAF {

    public static class IntegerKeyIntegerValueEvaluator implements UDAFEvaluator {
        private HashMap<Integer, Integer> buffer;

        public IntegerKeyIntegerValueEvaluator() {
            super();
            init();
        }

        public void init()
        {
            buffer = new HashMap<Integer, Integer>();
        }

        /**
         * The parameters are the same parameters that are passed when function is called in Hive query
         *
         * @param key Integer
         * @param value Integer
         * @return Boolean
         */
        public boolean iterate(Integer key, Integer value)
        {
            if(!buffer.containsKey(key)) {
                buffer.put(key, value);
            }

            return true;
        }

        /**
         * Function called when separated jobs are done on different data nodes
         *
         * @return HashMap
         */
        public HashMap<Integer, Integer> terminatePartial()
        {
            return buffer;
        }

        /**
         * Function called when merging all data result calculated from all data notes
         *
         * @param another HashMap
         * @return Boolean
         */
        public boolean merge(HashMap<Integer, Integer> another)
        {
            //null might be passed in case there is no input data.
            if (another == null) {
                return true;
            }

            for(Integer key : another.keySet()) {
                if(!buffer.containsKey(key)) {
                    buffer.put(key, another.get(key));
                }
            }

            return true;
        }

        /**
         * Return the result back to the query
         *
         * @return HashMap
         */
        public HashMap<Integer, Integer> terminate()
        {
            if (buffer.size() == 0) {
                return null;
            }

            return buffer;
        }
    }

    public static class StringKeyIntegerValueEvaluator implements UDAFEvaluator {
        private HashMap<String, Integer> buffer;

        public StringKeyIntegerValueEvaluator() {
            super();
            init();
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hive.ql.exec.UDAFEvaluator#init()
         */
        public void init() {
            buffer = new HashMap<String, Integer>();
        }

        /**
         * @param key String
         * @param value Integer
         * @return boolean
         */
        public boolean iterate(String key, Integer value) {
            if(!buffer.containsKey(key)) {
                buffer.put(key, value);
            }

            return true;
        }

        /**
         * @return HashMap<String, Integer>
         */
        public HashMap<String, Integer> terminatePartial() {
            return buffer;
        }

        /**
         * @param another HashMap<String, Integer>
         * @return boolean
         */
        public boolean merge(HashMap<String, Integer> another) {
            if(another == null) {
                return true;
            }

            for(final String key: another.keySet()) {
                iterate(key, another.get(key));
            }

            return true;
        }

        /**
         * @return HashMap<String, Integer>
         */
        public HashMap<String, Integer> terminate() {
            if(buffer.size() == 0) {
                return null;
            }

            return buffer;
        }
    }

    public static class StringKeyArrayValueEvaluator implements UDAFEvaluator {
        private HashMap<String, ArrayList<Integer>> buffer;

        public StringKeyArrayValueEvaluator() {
            super();
            init();
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.hive.ql.exec.UDAFEvaluator#init()
         */
        public void init() {
            buffer = new HashMap<String, ArrayList<Integer>>();
        }

        /**
         * @param key String
         * @param value ArrayList<Integer>
         * @return boolean
         */
        public boolean iterate(String key, ArrayList<Integer> value) {
            if(!buffer.containsKey(key)) {
                buffer.put(key, copy(value));
            } else {
                final ArrayList<Integer> current = buffer.get(key); // get the current list
                final Set<Integer> another = new LinkedHashSet<Integer>(current); // removes dupes and maintains ordering
                another.addAll(copy(value)); // add the incoming value to the set

                current.clear(); // clear the current list
                current.addAll(another); // add the de-duped ordered list back in

                buffer.remove(key);		  // make sure the current object has been removed
                buffer.put(key, current); // put it back into the buffer
            }

            return true;
        }

        /**
         * @return HashMap<String, ArrayList<Integer>>
         */
        public HashMap<String, ArrayList<Integer>> terminatePartial() {
            return buffer;
        }

        /**
         * @param another HashMap<String, ArrayList<Integer>>
         * @return boolean
         */
        public boolean merge(HashMap<String, ArrayList<Integer>> another) {
            if(another == null) {
                return true;
            }

            for(final String key: another.keySet()) {
                iterate(key, another.get(key));
            }

            return true;
        }

        /**
         * @return HashMap<String, ArrayList<Integer>>
         */
        public HashMap<String, ArrayList<Integer>> terminate() {
            if(buffer.size() == 0) {
                return null;
            }

            return buffer;
        }

        /**
         * @param list ArrayList<Integer>
         * @return ArrayList<Integer>
         */
        public ArrayList<Integer> copy(ArrayList<Integer> list) {
            return new ArrayList<Integer>(list);
        }
    }
}
