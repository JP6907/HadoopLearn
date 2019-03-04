package mapreduce.flow;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据类型要实现hadoop的序列化接口
 */
public class FlowBean implements Writable {

    private int upFlow;
    private int downFlow;
    private int totalFlow;
    private String phone;

    //必须要有无参构造函数，mapreduce先通过无参构造函数反射生成对象，在调用反序列化函数恢复数据
    public FlowBean() {
    }

    public FlowBean(String phone, int upFlow, int downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.phone = phone;
        this.totalFlow = upFlow + downFlow;
    }

    public String getPhone() {
        return phone;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "  " + downFlow + "  " + totalFlow;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(totalFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.totalFlow = dataInput.readInt();
    }
}
