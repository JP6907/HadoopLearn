package mapreduce.Order.Simple;

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
import java.util.ArrayList;
import java.util.Collections;

public class OrderTopN {

    public static class OrderTopNMapper extends Mapper<LongWritable, Text,Text,OrderBean>{
        OrderBean orderBean = new OrderBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            orderBean.set(fields[0],fields[1],fields[2],Float.parseFloat(fields[3]),Integer.parseInt(fields[4]));
            k.set(fields[0]);

            // 从这里交给maptask的kv对象，会被maptask序列化后存储，所以不用担心覆盖的问题
            context.write(k,orderBean);
        }
    }

    public static class OrderTopNReducer extends Reducer<Text, OrderBean,OrderBean, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {

            int topn = context.getConfiguration().getInt("topn",3);

            ArrayList<OrderBean> list = new ArrayList<OrderBean>();
            // reduce task提供的values迭代器，每次迭代返回给我们的都是同一个对象，只是set了不同的值
            for(OrderBean bean : values){
                // 构造一个新的对象，来存储本次迭代出来的值
                // 直接存迭代值会覆盖已经存进去的值
                OrderBean order = new OrderBean();
                order.set(bean.getOrderId(),bean.getUserId(),bean.getPdtName(),bean.getPrice(),bean.getNumber());
                list.add(order);
            }

            Collections.sort(list);

            for(int i=0;i<topn;i++){
                context.write(list.get(i),NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.setInt("topn",2);

        Job job = Job.getInstance(conf);
        job.setJarByClass(OrderTopN.class);
        job.setNumReduceTasks(2);

        job.setMapperClass(OrderTopNMapper.class);
        job.setReducerClass(OrderTopNReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/orderData.txt");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/order_out1/");
        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);
    }
}
