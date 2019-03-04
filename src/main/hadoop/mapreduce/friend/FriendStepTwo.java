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

public class FriendStepTwo {

    /**
     * 输入：
     * Key:C
     * value: [ A, B, E, F, G ]
     * 表示C为以上所有人的共同好友
     * 可以得出value中两两之间的共同好友为C
     */
    public static class FriendStepTwoMapper extends Mapper<LongWritable, Text,Text,Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String[] persons = fields[1].split(",");
            for(int i=0;i<persons.length;i++){
                for(int j=i+1;j<persons.length;j++){
                    //确保A-B 和 B-A为同一个
                    String s = persons[i].compareTo(persons[j])<0?(persons[i]+"-"+persons[j]):(persons[j]+"-"+persons[i]);
                    context.write(new Text(s),new Text(fields[0]));
                }
            }
        }
    }

    /**
     * 输入：
     * A-B C D E
     */
    public static class FriendStepTwoReducer extends Reducer<Text,Text,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text v : values){
                sb.append(v.toString()).append(" ");
            }
            context.write(key,new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FriendStepOne.class);
        job.setNumReduceTasks(1);

        job.setMapperClass(FriendStepTwoMapper.class);
        job.setReducerClass(FriendStepTwoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/friend_out1/");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/friend_out2/");

        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);
    }
}
