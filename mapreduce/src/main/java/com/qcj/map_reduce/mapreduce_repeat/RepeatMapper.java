package com.qcj.map_reduce.mapreduce_repeat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RepeatMapper extends Mapper<LongWritable,Text,Text,Text> {
    Text mkey = new Text();
    Text mvalue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //String[] reads = value.toString().trim().split(",");
        String[] reads = value.toString().split(",");
        String key1 = reads[0];
        String key2 = reads[reads.length-1];
        String keys = key1 + "\t" + key2;
        mkey.set(keys);
        String values;
        if(reads.length == 3){
            values = reads[1];
        }else{
            values = "";
        }
        mvalue.set(values);
        context.write(mkey,mvalue);
    }
}
