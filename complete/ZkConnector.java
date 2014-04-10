import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.io.IOException;

public class ZkConnector implements Watcher {

    // ZooKeeper Object
    ZooKeeper zooKeeper;

    // To block any operation until ZooKeeper is connected. It's initialized
    // with count 1, that is, ZooKeeper connect state.
    CountDownLatch connectedSignal = new CountDownLatch(1);
    
    // ACL, set to Completely Open
    protected static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

    // List of node names used in the system
    public static String primaryJobTrackerPath = "/primaryJobTracker";
    public static String activeJobPath = "/activeJobs";
    public static String completedJobPath = "/completedJobs";
    public static String primaryFileServerPath = "/primaryFS";
    public static String workerPoolPath = "/workerPool";
    public static String workerIDPath = "/worker";

    /**
     * Connects to ZooKeeper servers specified by hosts.
     */
    public void connect(String hosts) throws IOException, InterruptedException {

        zooKeeper = new ZooKeeper(
                hosts, // ZooKeeper service hosts
                5000,  // Session timeout in milliseconds
                this); // watcher - see process method for callbacks
	    connectedSignal.await();
    }

    /**
     * Closes connection with ZooKeeper
     */
    public void close() throws InterruptedException {
	    zooKeeper.close();
    }

    /**
     * @return the zooKeeper
     */
    public ZooKeeper getZooKeeper() {
        // Verify ZooKeeper's validity
        if (null == zooKeeper || !zooKeeper.getState().equals(States.CONNECTED)) {
	        throw new IllegalStateException ("ZooKeeper is not connected.");
        }
        return zooKeeper;
    }

    protected Stat exists(String path, Watcher watch) {
        
        Stat stat = null;
        try {
            stat = zooKeeper.exists(path, watch);
        } catch(Exception consumed) {
            // do nothing
        }
        
        return stat;
    }

    protected KeeperException.Code create (String path, byte[] byteData, CreateMode mode) {
        try{
            zooKeeper.create(path, byteData, acl, mode);
        } catch(KeeperException e) {
            return e.code();
        } catch (Exception e) {
            return KeeperException.Code.SYSTEMERROR;
        }
        return KeeperException.Code.OK;
    }

    protected KeeperException.Code create(String path, String data, CreateMode mode) {
        
        byte[] byteData = null;
        try {
            if(data != null) {
                byteData = data.getBytes();
            }
        } catch(Exception e) {
            return KeeperException.Code.SYSTEMERROR;
        }
        
        return create(path, byteData, mode);
    }

    protected KeeperException.Code create(String path, ZkPacket packet, CreateMode mode){
        return create(path, packet.asBytes(), mode); 
    }

    protected List<String> getChildren(String path,boolean watch) {
       List<String> ret = null;
       try {
           ret = zooKeeper.getChildren(path,watch);
       } catch (Exception e) {
           System.err.println("ZkConnector received exception: "
                   + e.getMessage() + " in getChildren()");
       }
       return ret;
    }

    protected List<String> getChildren(String path,Watcher watcher) {
        List<String> ret = null;
        try {
            ret = zooKeeper.getChildren(path,watcher);
        } catch (Exception e) {
            System.err.println("ZkConnector received exception: "
                    + e.getMessage() + " in getChildren() with attempting to set watch.");
        }
        return ret;
    }

    public int getNumChildren(String path) {
        List<String> listOfChildren = getChildren(path,false);
        return listOfChildren.size();
    }

    protected KeeperException.Code delete(String path, int version) {
        try { 
            zooKeeper.delete(path,version);
        } catch(KeeperException e) {
            return e.code();
        } catch(InterruptedException x) {
            Thread.currentThread().interrupt(); // propagate
        }
        return KeeperException.Code.OK;
    }

    public void process(WatchedEvent event) {
        // release lock if ZooKeeper is connected.
        if (event.getState() == KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }

    public ZkPacket getPacket(String path, boolean watch, Stat stat){
        byte[] byteData = null;
        try{       
            byteData = zooKeeper.getData(path, watch, stat);
        } catch(Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
        return ZkPacket.asPacket(byteData);
    } 
}

