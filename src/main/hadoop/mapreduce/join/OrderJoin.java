package mapreduce.join;

import jdk.nashorn.internal.runtime.regexp.JoniRegExp;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
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

    public static class OrderJoinMapper extends Mapper<LongWritable, Text,Text, JoinBean>{

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
            context.write(k,bean);
        }

    }

    public static class OrderJoinReducer extends Reducer<Text,JoinBean,JoinBean,NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<JoinBean> values, Context context) throws IOException, InterruptedException {

            ArrayList<JoinBean> orderList = new ArrayList<JoinBean>();  //一个user可能对应多个order
            JoinBean userBean = new JoinBean();
            try {
                for (JoinBean bean : values) {
                    if("order".equals(bean.getTableName())) {
                        JoinBean newBean = new JoinBean();
                        BeanUtils.copyProperties(newBean, bean);
                        orderList.add(newBean);
                    }else{
                        BeanUtils.copyProperties(userBean,bean);
                    }
                }

                //拼接
                for(JoinBean order : orderList){
                    order.setUserName(userBean.getUserName());
                    order.setUserAge(userBean.getUserAge());
                    order.setUserFriend(userBean.getUserFriend());

                    context.write(order,NullWritable.get());
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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinBean.class);
        job.setOutputKeyClass(JoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/orderJoinData/");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/orderJoin_out/");

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);

    }
}
