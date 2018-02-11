import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DistributedServer {
    private static final String connectString = "40.125.168.208:2181,40.125.214.178:2181,139.219.100.98:2181";
    private static final int sessionTimeout = 2000;
    private static final String parentNode = "/servers";

    private static ZooKeeper zk = null;

    public void getConnect() throws Exception {

        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent event) {
                // 收到事件通知后的回调函数（应该是我们自己的事件处理逻辑）
                System.out.println(event.getType() + "---" + event.getPath());
                try {
                    zk.getChildren("/", true);
                } catch (Exception e) {
                }
            }
        });

    }

    public void registerServer(String hostname) throws Exception {

        String create = zk.create(parentNode + "/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online.." + create);

    }

    public void handleBussiness(String hostname) throws InterruptedException {
        System.out.println(hostname + "start working.....");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {

        // 获取zk连接
        DistributedServer server = new DistributedServer();
        server.getConnect();
        Stat stat = zk.exists("/servers", false);
        if(stat == null){
            zk.create("/servers", "servername".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 利用zk连接注册服务器信息
        server.registerServer(args[0]);

        // 启动业务功能
        server.handleBussiness(args[0]);

    }

}
