package hdfs.HA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HAHdfsClient {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();
        /**
         * 要连接高可用的集群，并不是指定某一台机器的地址，而是指定集群的dfs.nameservices的地址，hdp24
         * 这样会自动连接集群中某台可用的机器，否则只能连接指定机器，没有利用好高可用的特点
         * 必须把core-site.xml文件和hdfs-site.xml文件放到项目路径中
         */
        FileSystem fs = FileSystem.get(new URI("hdfs://hdp24"),conf,"root");
        fs.copyFromLocalFile(new Path("/home/zjp/Documents/IDMGCExt.crx"),new Path("/test/"));
        fs.close();
    }
}
