package com.devveri.pig;

import static com.devveri.pig.QueryStringLoader.KEY_TIMESTAMP;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Map;

/**
 * @author hakanilter
 */
public class QueryStringStorage extends StoreFunc
{
    private RecordWriter writer;

    //@SuppressWarnings("unchecked")
    public void putNext(Tuple tuple) throws IOException
    {
        if (tuple == null || tuple.size() == 0) {
            throw new IOException("Tuple is null");
        }
        try {
            Map<String, String> keyValues = null;
            for (Object obj : tuple.getAll()) {
                if (obj instanceof Map) {
                    keyValues = (Map<String, String>) obj;
                    break;
                }
            }
            if (keyValues != null && keyValues.size() > 0) {
                String timestamp = keyValues.get(KEY_TIMESTAMP);
                keyValues.remove(KEY_TIMESTAMP);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                baos.write(getQueryString(keyValues).getBytes("UTF-8"));
                writer.write(timestamp, new Text(baos.toByteArray()));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    public OutputFormat getOutputFormat() throws IOException {
        return new TextOutputFormat<LongWritable, Text>();
    }

    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    private String getQueryString(Map<String, String> map)
    {
        StringBuffer sb = new StringBuffer("");
        String[] keys = map.keySet().toArray(new String[map.size()]);
        Arrays.sort(keys, String.CASE_INSENSITIVE_ORDER);
        for (String key : keys) {
            sb.append(key);
            sb.append("=");
            try {
                sb.append( URLEncoder.encode(map.get(key), "UTF-8") );
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            sb.append("&");
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
}
