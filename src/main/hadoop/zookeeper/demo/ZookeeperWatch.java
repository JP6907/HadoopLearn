package zookeeper.demo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import javax.xml.ws.RequestWrapper;
import java.util.List;

public class ZookeeperWatch {

    ZooKeeper zk = null;

    @Before
    public void init() throws Exception{
        zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-04:2181", 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState()== Event.KeeperState.SyncConnected &&
                    watchedEvent.getType()== Event.EventType.NodeDataChanged){
                    System.out.println("数据发生变化......");
                    System.out.println(watchedEvent.getPath()); // 收到的事件所发生的节点路径
                    System.out.println(watchedEvent.getType()); // 收到的事件的类型

                    //监听只会持续到发生一次变化,必须再次设置监听
                    try {
                        zk.getData(watchedEvent.getPath(),true,null);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else if(watchedEvent.getState()== Event.KeeperState.SyncConnected &&
                    watchedEvent.getType() == Event.EventType.NodeChildrenChanged){

                    /**
                     * NodeChildrenChanged只会监听create/rmr事件，set事件不会监听
                     */

                    System.out.println(watchedEvent.getPath());
                    System.out.println("子节点发生变化!!!");
                    try {
                        zk.getChildren(watchedEvent.getPath(), true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }
        });
    }

    @Test
    public void testGetWatch() throws Exception{
        //默认使用init中配置的Watcher，而且只设置了/java节点的监听
        byte[] data = zk.getData("/java", true, null);
        byte[] data1 = zk.getData("/aaa", true, null);

        //设置子节点变化监听
        List<String> children = zk.getChildren("/java", true);


        System.out.println(new String(data,"UTF-8"));

        //监听线程是一个守护进程，会随着主线程的结束而结束
        //必须让主线程保持存活，才能收到监听事件
        Thread.sleep(Long.MAX_VALUE);

    }
}
