package com.yc.mapreduce.grossscore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @auther wcc
 * @create 2019-10-11 15:28
 */
public class GrossScoreDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JobName");
        job.setJarByClass(GrossScoreDriver.class);
        job.setMapperClass(GrossScoreMap.class);
        job.setReducerClass(GrossScoreReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/txt/score"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/results/grossscore"));

        if (!job.waitForCompletion(true))
            return;

    }
}
