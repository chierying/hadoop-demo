package cn.zb.mapreduce.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区类
 */
public class MyPartitioner extends Partitioner<MyKeyPair, IntWritable> {
    /**
     * 自定义分区字段
     *
     * @param myKeyPair     自定义的Key类型
     * @param intWritable   value
     * @param numPartitions 分区数量（等于Reduce数量）
     * @return 分区编号
     */
    public int getPartition(MyKeyPair myKeyPair, IntWritable intWritable, int numPartitions) {
        // 将第一个字段作为分区字段
        return (myKeyPair.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
