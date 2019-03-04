package mapreduce.page.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitter {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        /**
         * 外部传参给mapper或reducer程序的几种方法
         */
        /**
         * 方法一：加载classpath下的xml配置文件
         */
        Configuration conf = new Configuration();
        conf.addResource("myConf.xml");
        /**
         * 方法二：通过代码设置参数
         */
        //conf.setInt("top.n", 3);
        //conf.setInt("top.n", Integer.parseInt(args[0]));

        /**
         * 方法三：通过属性配置文件获取参数
         */
		/*Properties props = new Properties();
		props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("topn.properties"));
		conf.setInt("top.n", Integer.parseInt(props.getProperty("top.n")));*/

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitter.class);

        job.setMapperClass(PageTopnMapper.class);
        job.setReducerClass(PageTopnReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        Path inPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/pageRequest.dat");
        Path outPath = new Path("/home/zjp/Documents/HadoopLearn/src/main/resources/pageTopN");
        FileInputFormat.setInputPaths(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

        job.waitForCompletion(true);
    }
}
