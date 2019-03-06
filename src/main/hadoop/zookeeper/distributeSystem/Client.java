package zookeeper.distributeSystem;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Client {

    //在线的服务器列表
    private volatile ArrayList<String> onlineServers = new ArrayList<String>();

    ZooKeeper zk = null;

    public void connectZK() throws Exception{
        zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-04:2181", 2000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState()== Event.KeeperState.SyncConnected &&
                    watchedEvent.getType() == Event.EventType.NodeChildrenChanged){
                    try {
                        // 事件回调逻辑中，再次查询zk上的在线服务器节点即可，查询逻辑中又再次注册了子节点变化事件监听
                        getOnlineServers();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    public void getOnlineServers() throws Exception{
        //children里面没有保存全路径
        List<String> children = zk.getChildren("/servers", true);
        ArrayList<String> servers = new ArrayList<String>();

        for(String child : children){
            byte[] data = zk.getData("/servers/" + child, false, null);
            String serverInfo = new String(data);
            servers.add(serverInfo);
        }

        onlineServers = servers;
        System.out.println("查询了一次zk，当前的服务器有："+servers);
    }

    public void sendRequest() throws Exception{
        Random random = new Random();
        while (true){
            try {
                //挑选一台在线的服务器
                int nextInt = random.nextInt(onlineServers.size());
                String server = onlineServers.get(nextInt);
                String hostname = server.split(":")[0];
                int port = Integer.parseInt(server.split(":")[1]);

                System.out.println("本次挑选的服务器为："+server);

                Socket socket = new Socket(hostname,port);
                OutputStream outputStream = socket.getOutputStream();
                InputStream inputStream = socket.getInputStream();

                outputStream.write("hello server".getBytes());
                outputStream.flush();

                byte[] buf = new byte[256];
                int read = inputStream.read(buf);
                System.out.println("服务器相应数据："+new String(buf,0,read));

                outputStream.close();
                inputStream.close();
                socket.close();

                Thread.sleep(2000);
            }catch (Exception e){ //在这里处理异常，client如果连接到已经下线的服务器，就会重新试图重新连接其它服务器，如果在mian抛出异常，则程序直接终止
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Client client = new Client();
        //构造zk连接
        client.connectZK();
        //查询在线服务器列表
        client.getOnlineServers();
        //向服务器发送查询请求
        client.sendRequest();
    }
}
