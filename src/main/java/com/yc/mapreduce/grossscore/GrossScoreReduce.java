package com.yc.mapreduce.grossscore;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @auther wcc
 * @create 2019-10-11 15:15
 * <p>
 * 计算结果
 * wcc:90 90 90
 * hm:91 91 91
 * cz:92 92 92
 * <p>
 * 只需要加起来就可以
 */
public class GrossScoreReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
