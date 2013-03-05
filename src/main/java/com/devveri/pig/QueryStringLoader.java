package com.devveri.pig;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hakanilter
 */
public class QueryStringLoader extends LoadFunc
{
    public static final String KEY_TIMESTAMP = "timestamp";

    private static Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(QueryStringLoader.class);
    }

    private RecordReader reader;
    private TupleFactory tupleFactory;

    public QueryStringLoader() {
        tupleFactory = TupleFactory.getInstance();
    }

    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    public void prepareToRead(RecordReader reader, PigSplit pigSplit) throws IOException {
        this.reader = reader;
    }

    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    public Tuple getNext() throws IOException
    {
        Tuple tuple = null;

        try {
            if (reader.nextKeyValue()) {
                Text logString = (Text) reader.getCurrentValue();
                if (logString != null) {
                    String log = logString.toString();
                    Map<String, String> map = tokenize(log);
                    tuple = tupleFactory.newTuple(Collections.singletonList(map));
                }
            }
        } catch (InterruptedException e) {
            // add more information to the runtime exception condition.
            int errCode = 6018;
            throw new ExecException("Error while reading input", errCode, PigException.REMOTE_ENVIRONMENT, e);
        }
        return tuple;
    }

    private Map<String,String> tokenize(String text)
    {
        Map<String, String> map = new HashMap<String, String>();
        int pos = text.indexOf(" ");
        if (pos > 0) {
            map.put(KEY_TIMESTAMP, text.substring(0,pos));
            text = text.substring(pos+1);
        } else {
            pos = text.indexOf("\t");
            if (pos > 0) {
                map.put(KEY_TIMESTAMP, text.substring(0,pos));
                text = text.substring(pos+1);
            }
        }

        String[] keyValues = text.split("&");
        for(int i=0; i<keyValues.length; i++ ){
            String[] keyValue = keyValues[i].split("=");
            try {
                keyValue[1] = keyValue[1].trim();
                if( !keyValue[1].equals("") ){
                    map.put(keyValue[0], URLDecoder.decode(keyValue[1], "UTF-8"));
                }
            } catch(Exception e) {
                LOG.error("Error on parsing string", e);
            }
        }

        return map;
    }
}
