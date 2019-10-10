package com.yc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
/**
 * @Description HDFS测试
 * @auther wcc
 * @create 2019-10-09 16:30
 */
public class HDFSTest {

    private Configuration conf;

    private FileSystem fs;

    @Before
    public void before() throws URISyntaxException, IOException, InterruptedException {
        //1 获取文件系统
        conf = new Configuration();
        conf.set("dfs.replication", "1");
        fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "root");
    }

    /**
     * HDFS文件上传(测试参数优先级)
     * fs.create(new Path("/pom1111.txt"));
     *
     * 相当于：hadoop fs -put 本地文件系统路径 HDFS文件系统路径
     *
     */
    @Test
    public void testCopyFromLocalFIle() throws IOException, URISyntaxException, InterruptedException {
        //2 上传文件
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/pom1111.txt"));
        FileInputStream fileInputStream = new FileInputStream("C:\\Users\\wcc\\IdeaProjects\\hadoop_study\\pom.xml");
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, conf);
        //3 关闭资源
        fileInputStream.close();
        fsDataOutputStream.close();
        System.out.println("over");
    }

    /**
     * HDFS下载文件
     * fs.open(new Path("/pom22.txt"));
     *
     * 相当于：Hadoop fs -get HDFS文件系统路径
     */
    @Test
    public void testDownLoadFile() throws IOException, URISyntaxException, InterruptedException {
        //下载文件
        FSDataInputStream fsDataInputStream = fs.open(new Path("/pom22.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("a.txt");
        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, conf);
        //关流
        fsDataInputStream.close();
        fileOutputStream.close();
        System.out.println("over");
    }

    /**
     * 删除文件或目录
     * fs.delete(new Path("/user"), true)
     * true 删除该路径上的文件，如果该文件是一个目录的话，则递归删除该目录
     * pathString :hdfs://hadoop01:9000/pom1111.txt（绝对路径） 或者 /pom1111.txt（相对路径）
     *
     * 相当于：Hadoop fs -rmr(递归删除文件) | -rm(删除文件) HDFS文件系统路径
     */
    @Test
    public void testDeleteFile() throws URISyntaxException, IOException, InterruptedException {
        fs.delete(new Path("hdfs://hadoop01:9000/pom1111.txt"), true);
        System.out.println("over");
    }

    /**
     * 获取文件列表
     *
     * 相当于：Hadoop fs -ls HDFS文件系统路径
     */
    @Test
    public void testFileList() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            System.out.println(listFiles.next());
        }
    }

    /**
     * 关闭文件系统
     * @throws IOException
     */
    @After
    public void after() throws IOException {
        fs.close();
    }


}
