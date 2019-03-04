package mapreduce.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * JobSubmitter只是一个提交工具，用于提交打包好的jar包程序
 *
 * 如果在hadoop集群的某台机器启动这个job提交客户端的话
 * config就不需要指定fs.defaultFS mapred.framework.name
 *
 * 会读取配置文件中的这些属性
 * 同时mapred-site.xml中配置了mr程序运行方式 yarn/local
 *
 * 因为集群上用hadoop jar ***.jar com.***.*** 命令启动客户端main方法时，
 * hadoop jar这个命令会将所在机器上hadoop安装目录中的jar包和配置文件加入到运行时的classpath中
 *
 *
 */
public class JobSubmitter {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {

        // 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份,否则会出现权限问题
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();

        // 1、 配置运行环境
        //设置job运行时要访问的默认文件系统，如果在集群机器上提交则不用配置
        conf.set("fs.defaultFS", "hdfs://hdp-01:9000");
        //conf.set("fs.defaultFS", "file:///");
        //设置job提交到哪去运行   本地模式：local
        conf.set("mapreduce.framework.name", "local");
        //如果在集群机器上提交则不用配置
        conf.set("yarn.resourcemanager.hostname", "hdp-04");
        //如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
        //conf.set("mapreduce.app-submission.cross-platform","true");

        Job job = Job.getInstance(conf);

        //设置jar包位置

        ///方法一：本地
        //只设置Submitter提交到yarn集群后会找不到mapper和reducer，因为没打包到jar包中去（在本地运行可以这样设置）
        job.setJarByClass(JobSubmitter.class);

        ///方法二：本地/yarn
        // 2、封装参数：jar包所在的位置,必须将整个工程打包成jar包，（通过运行本程序提交到yarn集群需指定jar包位置）
        //job.setJar("/home/zjp/Documents/HadoopLearn/out/artifacts/HadoopLearn_jar/HadoopLearn.jar");

        // 3、封装参数： 本次job所要调用的Mapper实现类、Reducer实现类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4、封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outPath = new Path("/wordcount/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"),conf,"root");

        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        //必须是org.apache.hadoop.mapreduce.lib.input.FileInputFormat;包
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, outPath);  // 注意：输出路径必须不存在

        // 5、封装参数：想要启动的reduce task的数量
        job.setNumReduceTasks(2);

        //true表示将运行进度等信息及时输出给用户
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:-1);
    }
}
