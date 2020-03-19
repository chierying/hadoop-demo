package cn.zb.mapreduce.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper
 */
public class MyMapper extends Mapper<LongWritable, Text, MyKeyPair, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将一行数据按 \t\n\r\f分割。也可以加入一个参数，指定分割符。
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

        // 从一行输入中取得2个字段
        String first = stringTokenizer.nextToken();
        String second = stringTokenizer.nextToken();

        // 构造自定义Key
        MyKeyPair myKeyPair = new MyKeyPair();
        myKeyPair.setFirst(first);
        myKeyPair.setSecond(Integer.parseInt(second));

        // 设置输出类型Value
        IntWritable outValue = new IntWritable();
        outValue.set(Integer.parseInt(second));

        // 输出<key,value>对
        context.write(myKeyPair, outValue);
    }
}
