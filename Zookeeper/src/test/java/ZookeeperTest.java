import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperTest {
    private static String connectString = "40.125.168.208:2181,40.125.214.178:2181,139.219.100.98:2181";
    private static final int SESSION_TIMEOUT = 30000;

    public static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperTest.class);

    private Watcher watcher =  new Watcher() {

        public void process(WatchedEvent event) {
            System.out.println("process : " + event.getType());
            try {
                zooKeeper.getChildren("/", true);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    };

    private ZooKeeper zooKeeper;

    @Before
    public void connect() throws IOException {
        zooKeeper  = new ZooKeeper(connectString, SESSION_TIMEOUT, watcher);
    }

    @After
    public void close() {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetChildNodes() throws KeeperException, InterruptedException {
        String result = null;
        List<String> nodes = zooKeeper.getChildren("/", true);
        for (String node:nodes){
            System.out.println(node);
        }
        LOGGER.info("GetChildNodes result : {}", result);
       // Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void testCreate() {
        String result = null;
        try {
            result = zooKeeper.create("/eden", "eddeen".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        System.out.println(result);
    }

    @Test
    public void testGetData() {
        String result = null;
        try {
            byte[] bytes = zooKeeper.getData("/eden", null, null);
            result = new String(bytes);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        System.out.println(result);
    }

    @Test
    public void testDelete() {
        try {
            zooKeeper.delete("/eden", -1);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
    }



    @Test
    public void testGetDataWatch() {
        String result = null;
        try {
            byte[] bytes = zooKeeper.getData("/eden", new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println(event.getType());
                }
            }, null);
            result = new String(bytes);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        System.out.println(result);
    }

    @Test
    public void testExists() {
        Stat stat = null;
        try {
            stat = zooKeeper.exists("/eden", false);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertNotNull(stat);
        LOGGER.info("exists result : {}", stat.getCzxid());
    }

    @Test
    public void testSetData() {
        Stat stat = null;
        try {
            stat = zooKeeper.setData("/eden", "I like you".getBytes(), -1);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertNotNull(stat);
        System.out.println(stat.getVersion());
    }
}
