package com.yc.mapreduce.grossscore;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Description 求总分
 * @auther wcc
 * @create 2019-10-11 15:13
 */

/**
 * math.txt
 * wcc 90
 * cz 92
 * hm 91
 *
 * english.txt
 * wcc 90
 * cz 92
 * hm 91
 *
 * chinese.txt
 * wcc 90
 * cz 92
 * hm 91
 *
 */
public class GrossScoreMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strings = value.toString().split(" ");
        //key --> name
        //value --> score
        context.write(new Text(strings[0]), new IntWritable(Integer.valueOf(strings[1])));
    }
}
