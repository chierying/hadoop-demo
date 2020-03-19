package cn.zb.mapreduce.secondsort;

import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组类
 */
public class MyGroupComparator extends WritableComparator {

    protected MyGroupComparator() {
        // 指定<key,value>对中key类型，true为创建该类型的实例，若不指定类型将报空值错误。
        super(MyKeyPair.class, true);
    }


    /**
     * 重写compare方法，以第一个字段进行分组。若值相同，则会被分为一组。
     */
    public int compare(MyKeyPair a, MyKeyPair b) {
        return a.getFirst().compareTo(b.getFirst());
    }
}
