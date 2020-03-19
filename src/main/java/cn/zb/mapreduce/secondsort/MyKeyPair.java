package cn.zb.mapreduce.secondsort;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义组合Key
 */
@Data
public class MyKeyPair implements WritableComparable<MyKeyPair> {
    /**
     * 第一个排序字段
     */
    private String first;
    /**
     * 第二个排序字段
     */
    private int second;

    /**
     * 实线比较
     */
    public int compareTo(MyKeyPair o) {
        // 默认升序排列
        int result = this.first.compareTo(o.first);
        if (result != 0) {
            // 若第一个字段不相等，则返回结果。
            return result;
        } else {
            // 若第一个字段相等，则比较第二个字段，且降序。
            return -Integer.valueOf(this.second).compareTo(Integer.valueOf(o.second));
        }
    }

    /**
     * 序列化对象中的字段
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);
    }

    /**
     * 反序列化对象中的字段
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }
}
