package com.qcj.map_reduce.mapreduce_removeduplicate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RemoveMapper extends Mapper<LongWritable,Text,Text,Text> {
    Text line = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        line = value;
        context.write(line,new Text(""));//这样一行相同的就在一个分组中
    }
}
