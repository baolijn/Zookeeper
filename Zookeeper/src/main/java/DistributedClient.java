import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DistributedClient {

    private static final String connectString = "40.125.168.208:2181,40.125.214.178:2181,139.219.100.98:2181";
    private static final int sessionTimeout = 2000;
    private static final String parentNode = "/servers";
    private volatile List<String> serverList;
    private ZooKeeper zk = null;


    public void getConnect() throws Exception {

        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {
                try {
                    //重新更新服务器列表，并且注册了监听
                    getServerList();

                } catch (Exception e) {
                }
            }
        });

    }

    public void getServerList() throws Exception {

        // 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zk.getChildren(parentNode, true);

        // 先创建一个局部的list来存服务器信息
        List<String> servers = new ArrayList<String>();
        for (String child : children) {
            // child只是子节点的节点名
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        // 把servers赋值给成员变量serverList，已提供给各业务线程使用
        serverList = servers;

        //打印服务器列表
        System.out.println(serverList);

    }

    public void handleBussiness() throws InterruptedException {
        System.out.println("client start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }




    public static void main(String[] args) throws Exception {

        // 获取zk连接
        DistributedClient client = new DistributedClient();
        client.getConnect();
        // 获取servers的子节点信息（并监听），从中获取服务器信息列表
        client.getServerList();

        // 业务线程启动
        client.handleBussiness();

    }

}
