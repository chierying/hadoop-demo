package cn.zb.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 单词计数
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 获取程序执行时传入的参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage wordcount <in>[<in>...] <out>");
            System.exit(2);
        }

        // 构建任务对象
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        //设置输出结果的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            // 设置需要统计的文件的输入路径
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // 设置统计结果的输出路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        // 提交任务给Hadoop集群
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 自定义Mapper
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 根据空格、制表符、换行、回车分割字符串
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            // 循环输出每个单词与数量
            while (stringTokenizer.hasMoreTokens()) {
                this.word.set(stringTokenizer.nextToken());
                context.write(this.word, one);
            }
        }
    }

    /**
     * 自定义Reducer内部类
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 统计单词的总数
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            // 输出统计结果
            context.write(key, result);
        }
    }

}
