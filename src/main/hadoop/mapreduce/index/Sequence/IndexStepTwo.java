package mapreduce.index.Sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexStepTwo {

    /**
     * 读取：hello-c.txt	4
     * 转化成：hello  c.txt-->4
     *
     * 读取上一个mr程序的输出，Sequence格式
     * 输入不再是行号，行内容
     * 直接时k-v对
     */
    public static class IndexStepTwoMapper extends Mapper<Text, IntWritable,Text,Text>{

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] fields = key.toString().split("-");
            //reduce输出的结果都是以 \t 分割 ，而不是空格
            context.write(new Text(fields[0]),new Text(fields[1]+"-->"+value));
        }
    }


    public static class IndexStepTwoReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text text : values){
                sb.append(text.toString()).append("\t");
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStepOne.class);
        job.setNumReduceTasks(1);

        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // job.setInputFormatClass(TextInputFormat.class); 默认的输入组件
        job.setInputFormatClass(SequenceFileInputFormat.class);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/index_out11/");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/index_out22/");

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);
    }
}
