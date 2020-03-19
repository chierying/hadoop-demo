package cn.zb.mapreduce.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 应用主类
 */
public class MySecondSortApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://ubuntu1:9000");

        Job job = Job.getInstance(conf, "MySecondSortApp");
        job.setJarByClass(MySecondSortApp.class);
        job.setMapperClass(MyMapper.class);
        // 设置自定义分区
        job.setPartitionerClass(MyPartitioner.class);
        // 设置自定义分组
        job.setGroupingComparatorClass(MyGroupComparator.class);
        job.setReducerClass(MyReducer.class);

        // 设置Map任务输出类型。如果Map方法和Reduce方法输出类型一致，可以省略。
        job.setMapOutputKeyClass(MyKeyPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置Reduce任务输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置数据在HDFS中的输入和输出目录
        FileInputFormat.addInputPath(job, new Path("/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
