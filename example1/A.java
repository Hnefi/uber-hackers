import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;

public class A {
    
    static String myPath = "/ece419";
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. A zkServer:clientPort");
            return;
        }

        ZkConnector zkc = new ZkConnector();
        try {
            zkc.connect(args[0]);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        ZooKeeper zk = zkc.getZooKeeper();

        try {
            System.out.println("Sleeping...");
            Thread.sleep(10000);

            System.out.println("Creating " + myPath);
            ZkPacket p = new ZkPacket("hello", 5, 10);

            zkc.create(
                myPath,         // Path of znode
                p,           // Test sending data
                CreateMode.PERSISTENT   // Znode type, set to Persistent.
                );
        } catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
    }
}
