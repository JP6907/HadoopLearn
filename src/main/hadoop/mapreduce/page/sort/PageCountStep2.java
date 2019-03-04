package mapreduce.page.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageCountStep2 {

    public static class PageCountStep2Mapper extends Mapper<LongWritable, Text,PageCount, NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fileds = value.toString().split("\t");
            PageCount pageCount = new PageCount(fileds[0],Integer.parseInt(fileds[1]));

            context.write(pageCount,NullWritable.get());
        }
    }

    /**
     * reducer程序会自动按照接收的数据的key进行排序
     */
    public static class PageCountStep2Reducer extends Reducer<PageCount,NullWritable,PageCount,NullWritable>{

        @Override
        protected void reduce(PageCount key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountStep1.class);

        job.setMapperClass(PageCountStep2Mapper.class);
        job.setReducerClass(PageCountStep2Reducer.class);
        job.setMapOutputKeyClass(PageCount.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(PageCount.class);
        job.setOutputValueClass(NullWritable.class);

        ///点和下划线开头的文件会自动过滤
        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/pagecount");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/pagesort");
        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);
    }
}
