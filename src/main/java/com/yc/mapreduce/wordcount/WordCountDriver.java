package com.yc.mapreduce.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description 统计每个字符串出现的次数
 * @auther wcc
 * @create 2019-10-11 9:55
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //向MapReduce申请一个任务用于执行程序
        Job job = Job.getInstance();
        job.setJarByClass(WordCountDriver.class);
        //设置Map
        job.setMapperClass(WordCountMap.class);

        //设置Reduce
        job.setReducerClass(WordCountReduce.class);

        //设置Map的执行结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reduce的执行结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入文件
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/wcinput"));

        //设置输出的路径（要求不存在）
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/results/wordcount"));

        //提交任务，等待结束
        job.waitForCompletion(true);
    }
}
