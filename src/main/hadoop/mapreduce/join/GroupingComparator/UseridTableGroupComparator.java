package mapreduce.join.GroupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UseridTableGroupComparator extends WritableComparator {

    public UseridTableGroupComparator() {
        super(JoinBean.class,true);
    }

    //使userid相同的作为一组
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        JoinBean bean1 = (JoinBean) a;
        JoinBean bean2 = (JoinBean) b;
        return bean1.getUserId().compareTo(bean2.getUserId());
    }
}
