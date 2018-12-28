package com.qcj.map_reduce.mapreduce_repeat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RepeatReduce extends Reducer<Text,Text,Text,Text> {
    Text vout = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=0;
        StringBuilder stringBuilder = new StringBuilder();
        //values只能遍历一次，因为底层是指针的操作
        for (Text v:values) {
            count ++;
            stringBuilder.append(v.toString()).append(",");
        }
        if(count >= 2){
            String svalue = count + "\t" + stringBuilder.toString().substring(0,stringBuilder.length()-1);//去掉最后的，
            vout.set(svalue);
            context.write(key,vout);
        }
    }
}
