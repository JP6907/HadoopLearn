package zookeeper.distributeSystem;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class TimeQueryServer {

    ZooKeeper zk = null;

    //构造zk客户端连接
    public void connectZK() throws Exception{
        zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-04:2181", 2000, null);
    }

    //注册服务器信息
    public void registerServerInfo(String hostname,String port) throws Exception{
        Stat stat = zk.exists("/servers", false);
        if(stat==null){
            zk.create("/servers",null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String create = zk.create("/servers/server",(hostname+":"+port).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname+"服务器向zk注册信息成功，注册的节点为："+create);

    }


    public static void main(String[] args) throws Exception {

        TimeQueryServer timeQueryServer = new TimeQueryServer();
        //构造zk客户端连接
        timeQueryServer.connectZK();
        //注册服务器信息
        timeQueryServer.registerServerInfo(args[0],args[1]);
        //开启业务线程开始处理业务
        new TimeQueryService(Integer.parseInt(args[1])).start();
    }
}
