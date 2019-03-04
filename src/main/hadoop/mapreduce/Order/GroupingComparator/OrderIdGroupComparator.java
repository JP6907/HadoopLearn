package mapreduce.Order.GroupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupComparator extends WritableComparator {

    //不然compare方法无法获取正确的对象
    public OrderIdGroupComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;

        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
