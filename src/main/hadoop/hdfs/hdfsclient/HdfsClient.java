package hdfs.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;

public class HdfsClient {
    public static void main(String args[]) throws Exception {
        /**
         * Configuration参数对象的机制：
         *    构造时，会加载jar包中的默认配置 xx-default.xml
         *    再加载 用户配置xx-site.xml  ，覆盖掉默认参数
         *    构造完成之后，还可以conf.set("p","v")，会再次覆盖用户配置文件中的参数值
         */
        // new Configuration()会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml等文件
        Configuration conf = new Configuration();
        // 指定本客户端上传文件到hdfs时需要保存的副本数为：3
        conf.set("dfs.replication","3");
        // 指定本客户端上传文件到hdfs时切块的规格大小：64M
        conf.set("dfs.blocksize","64m");

        // 构造一个访问指定HDFS系统的客户端对象: 参数1:——HDFS系统的URI，参数2：——客户端要特别指定的参数，参数3：客户端的身份（用户名）
        FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000/"),conf,"root");
        // 上传一个文件到HDFS中
        fs.copyFromLocalFile(new Path("/home/zjp/Documents/IDMGCExt.crx"),new Path("/test/"));
        fs.close();
    }

    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "64m");

        fs = FileSystem.get(new URI("hdfs://hdp-01:9000/"), conf, "root");
    }

    @Test
    public void testGet() throws Exception{
        fs.copyToLocalFile(new Path("/hadoop-root-datanode.pid"),new Path("~/Documents/tmp/"));
        fs.close();
    }

    @Test
    public void testPut() throws Exception{
        fs.copyFromLocalFile(new Path("/home/zjp/Documents/boost/test/test_boost.cpp"),new Path("/yyy/test_boost.cpp"));
        fs.close();
    }

    /**
     * 在hdfs内部移动文件\修改名称
     */
    @Test
    public void testRename() throws Exception{
        fs.rename(new Path("/VERSION"),new Path("/xxx/VERSION"));
        fs.close();
    }

    /**
     * 在hdfs中创建文件夹
     */
    @Test
    public void testMkdir() throws Exception{
        fs.mkdirs(new Path("/yyy"));
        fs.close();
    }

    /**
     * 在hdfs中删除文件或文件夹
     */
    @Test
    public void testRm() throws Exception{
        fs.delete(new Path("/test"),true);
        fs.close();
    }


    /**
     * 查询hdfs指定目录下的文件信息
     */
    @Test
    public void testLs() throws Exception{
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path("/"), true);
        while (itr.hasNext()){
            LocatedFileStatus status = itr.next();
            System.out.println("文件全路径:"+status.getPath());
            System.out.println("块大小:"+status.getBlockSize());
            System.out.println("副本数量:"+status.getReplication());
            System.out.println("块信息:"+ Arrays.toString(status.getBlockLocations()));
            System.out.println("--------------------------------------------");
        }
        fs.close();
    }


    /**
     * 查询hdfs指定目录下的文件和文件夹信息
     */
    @Test
    public void testLs2() throws Exception{
        FileStatus[] list = fs.listStatus(new Path("/"));
        for (FileStatus status : list){
            System.out.println("文件全路径:"+status.getPath());
            System.out.println("块大小:"+status.getBlockSize());
            System.out.println("副本数量:"+status.getReplication());
            System.out.println("--------------------------------------------");
        }
        fs.close();
    }

    @Test
    public void testReadData() throws Exception{
        FSDataInputStream in = fs.open(new Path("/test.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(in,"utf-8"));
        String line = null;
        while((line=br.readLine())!=null){
            System.out.println(line);
        }
        br.close();
        in.close();
        fs.close();
    }

    /**
     * 读取hdfs中文件的指定偏移量范围的内容
     * @throws Exception
     */
    @Test
    public void testRandomReadData() throws Exception{
        FSDataInputStream in = fs.open(new Path("/test.dat"));
        //读取的起始位置
        in.seek(10);  //指定的是字节的偏移量
        //读取10个字节
        byte[] buf = new byte[10];
        in.read(buf);
        System.out.println(new String(buf));
        in.close();
        fs.close();

    }

    @Test
    public void testWriteData() throws Exception{
        FSDataOutputStream out = fs.create(new Path("/aa.jpg"), false);
        FileInputStream in = new FileInputStream("/home/zjp/Pictures/desktop.jpg");
        byte[] buf = new byte[1024];
        int len = 0;
        while((len=in.read(buf))!=-1){
            out.write(buf,0,len);
        }
        in.close();
        out.close();
        fs.close();
    }
}
