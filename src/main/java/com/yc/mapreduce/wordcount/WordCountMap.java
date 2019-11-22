package com.yc.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @auther wcc
 * @create 2019-10-10 9:39
 */

/**
 * 1 2 3（VALUEIN - 值（行数据）） 0（KEYIN - 偏移量）
 * 1 2 3
 * 3 1 2
 * 4 5 6
 *
 * MapReduce要求被传输的数据要被序列化，默认的序列化方式为AVRO
 * KEYIN - 偏移量
 * VALUEIN - 行数据
 * KEYOUT - 输出的key
 * VALUEOUT - 输出的value
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strings = value.toString().split(" ");
        //计算结果
        /**
         * 1:1 2:1 3:1
         * 1:1 2:1 3:1
         * 3:1 1:1 2:1
         * 4:1 5:1 6:1
         */

        for(String s : strings){
            context.write(new Text(s), new IntWritable(1));
        }

    }
}
