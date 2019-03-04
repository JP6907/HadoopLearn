package mapreduce.join.GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class UseridPartitioner extends Partitioner<JoinBean, NullWritable> {

    public int getPartition(JoinBean joinBean, NullWritable nullWritable, int numPartitions) {

        return (joinBean.getUserId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
