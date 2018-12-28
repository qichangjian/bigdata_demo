package com.qcj.map_reduce.mapreduce_course_max_min_avg;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CourseMapper extends Mapper<LongWritable, Text,Text, Text> {
    Text mkey = new Text();
    Text mvalue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //String[] reads = value.toString().trim().split(",");
        String[] reads = value.toString().split(",");
        String keys = reads[0];
        String values = reads[reads.length-1].trim();
        mkey.set(keys);
        mvalue.set(values);
        context.write(mkey,mvalue);
    }
}
