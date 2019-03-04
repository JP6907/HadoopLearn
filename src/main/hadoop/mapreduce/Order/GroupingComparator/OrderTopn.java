package mapreduce.Order.GroupingComparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderTopn {

    public static class OrderTopnMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable>{

        OrderBean orderBean = new OrderBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            orderBean.set(fields[0],fields[1],fields[2],Float.parseFloat(fields[3]),Integer.parseInt(fields[4]));
            k.set(fields[0]);

            context.write(orderBean,NullWritable.get());
        }
    }

    /**
     * 同一组订单数据，按照价格排序好
     * [o1,900]
     * [o1,400]
     * [o1,600]
     * [o1,200]
     */
    public static class OrderTopnReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{

        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            int topn = context.getConfiguration().getInt("topn",3);
            int i=0;
            for (NullWritable nullWritable : values){
                context.write(key,nullWritable);
                if(++i==topn)
                    return;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.setInt("topn",2);

        Job job = Job.getInstance(conf);
        job.setJarByClass(OrderTopn.class);
        job.setNumReduceTasks(2);

        job.setPartitionerClass(OrderidPartitioner.class);
        job.setGroupingComparatorClass(OrderIdGroupComparator.class);

        job.setMapperClass(OrderTopnMapper.class);
        job.setReducerClass(OrderTopnReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/orderData.txt");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/order_out2/");
        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);
    }

}
