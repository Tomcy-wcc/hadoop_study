package com.yc.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description 合并计算结果
 * @auther wcc
 * @create 2019-10-11 9:26
 */

//计算结果

/**
 * 1:1 2:1 3:1
 * 1:1 2:1 3:1
 * 3:1 1:1 2:1
 * 4:1 5:1 6:1
 * <p>
 * 1（key）：1 1 1（values）
 * 2：1 1 1
 * 3：1 1 1
 * 4：1
 * 5：1
 * 6：1
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //存储每个key所对应的次数
        int sum = 0;
        //遍历key对应的Iterable
        for (IntWritable v : values) {
            sum+=v.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
