package mapreduce.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendStepOne {

    /**
     * 将A:B,C,D,E分解成：
     * <B,A> <C,A> <D,A>...
     */
    public static class FriendStepOneMapper extends Mapper<LongWritable, Text , Text ,Text>{
        Text k = new Text();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(":");
            String[] friends = fields[1].split(",");
            v.set(fields[0]);
            for (String f: friends){
                k.set(f);
                context.write(k,v);
            }
        }
    }

    /**
     * key相同的会分到一组，例如：
     * <C,A><C,B><C,E><C,F><C,G>…
     *
     * 输出:
     * Key:C
     * value: [ A, B, E, F, G ]
     *
     * C是这些用户的好友
     */
    public static class FriendStepOneReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text text : values){
                sb.append(text.toString()).append(",");
            }
            context.write(key,new Text(sb.substring(0,sb.length()-1)));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FriendStepOne.class);
        job.setNumReduceTasks(3);

        job.setMapperClass(FriendStepOneMapper.class);
        job.setReducerClass(FriendStepOneReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/friends.txt");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/friend_out1/");

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);
    }
}
