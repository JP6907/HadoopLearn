package mapreduce.wordcount.skew;

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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class WordCount_Random1 {

    public static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

        Random random = new Random();
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        int numReduceTasks = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            numReduceTasks = context.getNumReduceTasks();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            for (String word : words){
                k.set(word + "\001" + random.nextInt(numReduceTasks));
                context.write(k,v);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Iterator<IntWritable> itr = values.iterator();
            while(itr.hasNext()){
                IntWritable value = itr.next();
                count += value.get();
            }
            context.write(key,new IntWritable(count));

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount_Random1.class);

        job.setMapperClass(WordCount_Random1.WordCountMapper.class);
        job.setReducerClass(WordCount_Random1.WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(3);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/wordcount.dat");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/wordcount_out1/");

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);

    }
}
