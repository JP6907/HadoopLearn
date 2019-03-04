package mapreduce.wordcount.skew;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount_Random2 {

    public static class WordCountMapper extends Mapper<Text,IntWritable,Text, IntWritable>{

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String k = key.toString().split("\001")[0];
            context.write(new Text(k),new IntWritable(value.get()));
        }
    }

    public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable v : values){
                count += v.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount_Random2.class);

        job.setMapperClass(WordCount_Random2.WordCountMapper.class);
        job.setReducerClass(WordCount_Random2.WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setNumReduceTasks(1);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/wordcount_out1/");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/wordcount_out2/");

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);

    }
}
