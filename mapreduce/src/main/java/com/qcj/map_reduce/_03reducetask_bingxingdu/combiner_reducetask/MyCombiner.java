package com.qcj.map_reduce._03reducetask_bingxingdu.combiner_reducetask;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner优化组件
 * maptask -- combiner -- reducetask
 * maptask输出传到了combiner :前两个泛型的输入 作为 后两个泛型的输出
 *
 * 实现和reducetask的实现一样，实际开发过程直接使用reducer的类作为combiner
 *
 *   combiner不适用场景：求平均值
 */
public class MyCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    /**
     * 这个函数的调用频率是一组调用一次
     *
     * 一组：相同的key一组
     * 这里的combiner对应的是  一个maptask的数据（部分数据）
     *       对maptask的输出结果先做一个局部合并，以前直接到shuffle了
     * reduce对应的是所有的maptask的数据
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v:values) {
            sum += v.get();
        }
        //向reducetask发送
        context.write(key,new IntWritable(sum));
    }
}
