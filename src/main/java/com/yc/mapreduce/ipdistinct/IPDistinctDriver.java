package com.yc.mapreduce.ipdistinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description IP去重
 * @auther wcc
 * @create 2019-10-11 11:51
 */
public class IPDistinctDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JobName");
        job.setJarByClass(IPDistinctDriver.class);
        job.setMapperClass(IPDistinckMap.class);
        job.setReducerClass(IPDistinctReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/txt/ip.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/results/ipdistict"));

        if (!job.waitForCompletion(true))
            return;
    }
}
