package mapreduce.phonedata;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 上行，下行以及总流量，输出用
 */
public class Flow implements Writable {

    private long upFlow;
    private long downFlow;

    public Flow() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    // 总流量其实不必定义成字段，定义成"属性"即可
    public long getSumFlow() {
        return getUpFlow() + getDownFlow();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(getUpFlow());
        out.writeLong(getDownFlow());
        out.writeLong(getSumFlow());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setUpFlow(in.readLong());
        setDownFlow(in.readLong());
        in.readLong(); // 丢弃总流量的值
    }

    @Override
    public String toString() {
        return getUpFlow() + "\t" + getDownFlow() + "\t" + getSumFlow();
    }
}
