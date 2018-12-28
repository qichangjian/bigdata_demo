package com.qcj.map_reduce.mapreduce_course_max_min_avg;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CourseReducer extends Reducer<Text,Text,Text, Text> {
    int max = 0;
    int min = 1000;
    int number = 0;
    int count = 0;
    double avg = 0;
    Text rkey = new Text();
    Text rvalue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text v:values) {
            int score = Integer.parseInt(v.toString());
            number ++;
            count += score;
            if(score < min){
                min = score;
            }
            if(max < score){
                max = score;
            }
        }
        avg = count/number;

        rkey.set("最大值：");
        rvalue.set(String.valueOf(max));
        context.write(rkey,rvalue);

        rkey.set("最小值：");
        rvalue.set(String.valueOf(min));
        context.write(rkey,rvalue);

        rkey.set("平均值：");
        rvalue.set(String.valueOf(avg));
        context.write(rkey,rvalue);


    }
}
