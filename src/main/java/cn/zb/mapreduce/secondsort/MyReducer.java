package cn.zb.mapreduce.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 定义Reducer类
 */
public class MyReducer extends Reducer<MyKeyPair, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(MyKeyPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 定义Text类型的输出Key
        Text outKey = new Text();
        // 循环输出<Key,Value>对
        for (IntWritable value : values) {
            outKey.set(key.getFirst());
            context.write(outKey, value);
        }
    }
}
