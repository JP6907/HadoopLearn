package mapreduce.join.GroupingComparator;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class OrderJoin {

    public static class OrderJoinMapper extends Mapper<LongWritable, Text,JoinBean, NullWritable>{

        String fileName = null;
        JoinBean bean = new JoinBean();
        Text k = new Text();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if(fileName.startsWith("order")){
                bean.set(fields[0], fields[1], "NULL", -1, "NULL", "order");
            }else{
                bean.set("NULL", fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], "user");
            }
            k.set(bean.getUserId());
            context.write(bean,NullWritable.get());
        }

    }

    public static class OrderJoinReducer extends Reducer<JoinBean, NullWritable, JoinBean,NullWritable>{

        @Override
        protected void reduce(JoinBean bean, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            JoinBean userBean = new JoinBean();
            //第一个为user表，后面的都是orderbiao
            int i=0;
            try {
                for(NullWritable nullWritable : values){
                    if(i==0){
                        BeanUtils.copyProperties(userBean,bean);
                    }else{
                        bean.setUserName(userBean.getUserName());
                        bean.setUserAge(userBean.getUserAge());
                        bean.setUserFriend(userBean.getUserFriend());

                        context.write(bean,NullWritable.get());
                    }
                    i++;
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(OrderJoin.class);

        job.setMapperClass(OrderJoinMapper.class);
        job.setReducerClass(OrderJoinReducer.class);
        job.setMapOutputKeyClass(JoinBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(3);

        job.setPartitionerClass(UseridPartitioner.class);
        job.setGroupingComparatorClass(UseridTableGroupComparator.class);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/orderJoinData/");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/orderJoin_out2/");

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);

    }
}
