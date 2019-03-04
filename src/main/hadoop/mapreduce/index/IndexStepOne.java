package mapreduce.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexStepOne {

    public static class IndexStepOneMapper extends Mapper<LongWritable,Text,Text, IntWritable>{

        // 产生 <hello-文件名，1>
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取数据切片所在文件名
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();

            String[] words = value.toString().split(" ");
            for(String word : words){
                context.write(new Text(word+"-"+fileName),new IntWritable(1));
            }
        }
    }

    public static class IndexStepOneReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for(IntWritable value : values){
                count += value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStepOne.class);
        job.setNumReduceTasks(3);

        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/index/");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/index_out1");

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);
    }
}
