import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;
import java.util.List;

import java.lang.Integer;
import java.lang.String;

/* Thread which is responsible for updating the id->md5 maps that store the current id's and 
 * all outstanding id's. Spawned as soon as we become primary, and runs forever...
 * - note: this daemon thread does not ever change any nodes, but it keeps a watch on the active
 *   jobs node's children (when a new node is created, we wake up and check the partition ID of the
 *   node just added, and add to the map if not already there).
 * - note2: Same procedure for watching the jobs completed node. When a result is done, add that to
 *   the map.
 */
class IDDaemonThread implements Runnable
{
    String activeJobPath;
    String completedJobPath;
    ZkConnector zkc;
    ConcurrentHashMap<String,Integer>activeIDMap = null;
    ConcurrentHashMap<String,Integer>completedIDMap = null;
    Watcher activeWatcher;
    Watcher completedWatcher;

    public IDDaemonThread(ConcurrentHashMap<String,Integer>activeM,
                          ConcurrentHashMap<String,Integer>completedM,
                          ZkConnector z,
                          String apath,
                          String cpath) { // called from root main() method
        activeIDMap = activeM;
        completedIDMap = completedM;
        zkc = z;
        activeJobPath = apath;
        completedJobPath = cpath;
        
        activeWatcher = new Watcher() { // Anonymous Watcher that only touches the active jobs node.
            @Override
            public void process(WatchedEvent event) {
                handleActiveNodeEvent(event);
            } 
        };
        
        completedWatcher = new Watcher() { // Anonymous Watcher that only touches the completed job node.
            @Override
            public void process(WatchedEvent event) {
                handleCompletedNodeEvent(event);
            } 
        };
    }

    public void run() {
        // just block until anything happens....
        waitForChildEvent(activeJobPath,activeWatcher);
        waitForChildEvent(completedJobPath,completedWatcher);

        while(!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(10000); // don't overload this system with parallelism
            } catch (InterruptedException x) { 
                Thread.currentThread().interrupt(); // propagate to top level, thread killed.
            }
        }
    }

    // Create a watch on this path's CHILDREN. Will not set a watch if second arg is null
    private void waitForChildEvent(String nodePath,Watcher watchToSet) {
        zkc.getChildren(nodePath,watchToSet);
    }

    /* Processing function for the active jobs node */
    private void handleActiveNodeEvent(WatchedEvent event) { 
        String wokenPath = event.getPath();
        EventType wokenType = event.getType();
        if (wokenPath.equalsIgnoreCase(activeJobPath)) { // watch triggered on right node.
            if(wokenType == EventType.NodeChildrenChanged) {
                // get list of children and iterate through it to check for new id's not in the map
                List<String> children = zkc.getChildren(activeJobPath,false);
                if (children == null) { 
                    // all partitions may be deleted..... re-enable watch
                    waitForChildEvent(activeJobPath,activeWatcher);
                } else {
                    for (String elem : children) {
                        // string process the node to get the id that was assigned
                        String[] parts = elem.split("/");
                        String partContainingID = null;
                        for (String idx : parts ) {
                            System.out.println("Current split element is: " + idx);
                            if (idx.contains(":")) { // this is the part that has the id
                                partContainingID = idx;
                                break;
                            }
                        }
                        String[] parts2 = partContainingID.split(":");
                        String jid = parts2[0]; // id always is before the ':'
                        Integer jobID = new Integer(jid);

                        if (!activeIDMap.containsValue(jobID)) { // this is a new job ID, need to add it in the map
                            // grab data from this node
                            ZkPacket nodeData = zkc.getPacket(elem,false,null); //TODO: Confirm what stat to pass???
                            String md5Key = nodeData.md5;
                            Integer oldValue = activeIDMap.put(md5Key,jobID);
                            if (oldValue != null) {
                                System.err.println("Sanity check failed!!!!! MD5 " + md5Key + " previously mapped to jobID in ACTIVE: "
                                        + oldValue.toString() + " even though we are currently assigning it to jobID in ACTIVE: "
                                        + jobID.toString());
                            }
                        }
                    }
                    // re-enable watch as well
                    waitForChildEvent(activeJobPath,activeWatcher);
                }
            }
        }
    }
    
    /* processing for the complete jobs node */
    private void handleCompletedNodeEvent(WatchedEvent event) {
        String wokenPath = event.getPath();
        EventType wokenType = event.getType();
        if (wokenPath.equalsIgnoreCase(completedJobPath)) { // watch triggered on right node.
            if(wokenType == EventType.NodeChildrenChanged) {
                // get list of children and iterate through it to check for new id's not in the map
                List<String> children = zkc.getChildren(completedJobPath,false);
                if (children == null) { 
                    // all partitions may be deleted..... re-enable watch
                    waitForChildEvent(completedJobPath,completedWatcher);
                } else {
                    for (String elem : children) {
                        // node's name is the job id, no processing needed
                        Integer jobID = new Integer(elem);

                        if (!activeIDMap.containsValue(jobID)) { // this is a new job ID, need to add it in the map
                            // grab data from this node
                            ZkPacket nodeData = zkc.getPacket(elem,false,null); //TODO: Confirm what stat to pass???
                            String md5Key = nodeData.md5;
                            Integer oldValue = completedIDMap.put(md5Key,jobID);
                            if (oldValue != null) {
                                System.err.println("Sanity check failed!!!!! MD5 " + md5Key + " previously mapped to jobID in CMP: "
                                        + oldValue.toString() + " even though we are currently assigning it to jobID in CMP: "
                                        + jobID.toString());
                            }
                        }
                    }
                    // re-enable watch as well
                    waitForChildEvent(completedJobPath,completedWatcher);
                }
            }
        }
    }
}

/* Template threads which are created upon a new job getting mapped into the ZK system. They sleep on the particular
 * partition they are assigned to using watches and wait for the workers to pass the results in.
 */
class PartitionThread implements Runnable 
{
    ZkConnector zkc;
    ZkPacket packet;
    String activeJobPath;
    String completedPath;
    String thisNodePath;
    Integer jobID;
    Watcher partitionWatcher;

    public PartitionThread(ZkConnector z, ZkPacket p,String s,String cpath,Integer i) {
        zkc = z;
        packet = p;
        activeJobPath = s;
        completedPath = cpath;
        jobID = i;

        thisNodePath = activeJobPath + "/" + jobID.toString() + ":" + Integer.toString(packet.partId);

        partitionWatcher = new Watcher() { // Anonymous Watcher that only touches this partition.
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            } 
        };
    }

    public void run() {
        // Connect to the ZK and create a bunch of sub-partition nodes which have the partition/thread/md5 in the ZkPacket.
        System.out.println(Thread.currentThread().getName() + " creating its persistent partition node in ZK with path: " + thisNodePath);

        Code ret = zkc.create(thisNodePath,packet,CreateMode.PERSISTENT); // jobs remain until completed
        if (ret == Code.OK) {
            System.out.println("Partition node with md5:" + packet.md5 + " and partition#: " + packet.partId + " now created in ZK.");
        } // TODO: Possibly check for node already exists errors??

        waitForFinished(thisNodePath);

        while(!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(10000); // don't overload this system with parallelism
            } catch (InterruptedException x) { 
                Thread.currentThread().interrupt(); // propagate to top level, thread killed.
            }
        }
    }

    private void waitForFinished(String partitionNodePath) {
        // Set a watch on this node's path for any new children being made.
        zkc.getChildren(partitionNodePath,partitionWatcher);
    }

    private void handleEvent(WatchedEvent event) {
        // This watch was set on a getChildren() call so it should wake up when children are created or removed.
        String wokenPath = event.getPath();
        EventType wokenType = event.getType();
        if (wokenPath.equalsIgnoreCase(thisNodePath)) { // watch triggered on right node.
            if(wokenType == EventType.NodeChildrenChanged) {
                // check immediately for finished node
                String finishedNodePath = thisNodePath + "/finished";
                Stat finStat = zkc.exists(finishedNodePath,null);
                if (finStat != null) { 
                    // this job is finished! get the result packet and create a persistent results node inside ZK
                    ZkPacket workerResults = zkc.getPacket(finishedNodePath,false,finStat); // TODO: check if this is the wrong stat???

                    if (workerResults.password != null) { // TODO: Fix this since we actually should create/update results after all partitions finish.
                        String newCompletedJobPath = completedPath + "/" + jobID.toString();
                        Code ret = zkc.create(newCompletedJobPath,workerResults,CreateMode.PERSISTENT);
                        if (ret == Code.OK) {
                            System.out.println("New finished result created for job id: " + jobID.toString());
                        }
                    }

                    // now delete the ephemeral 'taken' node as well as the partition itself
                    String takenPath = thisNodePath + "/taken";
                    Code ret = zkc.delete(takenPath,-1); // -1 matches any version number
                    if (ret != Code.OK) {
                        System.err.println("Error code of type: " + ret.intValue() + " when deleting ephemeral 'taken' node.");
                    }
                    ret = zkc.delete(finishedNodePath,-1);
                    if (ret != Code.OK) {
                        System.err.println("Error code of type: " + ret.intValue() + " when deleting persistent 'finished' node.");
                    }
                    ret = zkc.delete(thisNodePath,-1);
                    if (ret != Code.OK) {
                        System.err.println("Error code of type: " + ret.intValue() + " when deleting persistent partition node.");
                    }

                    // this thread's work is done
                    Thread.currentThread().interrupt();
                    return;
                } else {
                    // this job is not yet finished, re-enable the watch
                    waitForFinished(thisNodePath);
                }
            }
        }
    }
}

/* Smart threads which are created on a new ClientDriver connection, these do all of the actual tracking
 * of requests.
 */
class RequestHandler implements Runnable
{
    Socket sock = null;
    Integer currentID;
    ZkConnector zkc;
    String activeJobPath = "/activeJobs";
    String completedJobPath = "/completedJobs";
    String workerPath = "/workerPool";

    // Maps which are used to check the ID validity.
    ConcurrentHashMap<String,Integer> activeIDMap = null;
    ConcurrentHashMap<String,Integer> completedIDMap = null;

    public RequestHandler(Socket s,ZkConnector connector,Integer idx,
                          ConcurrentHashMap<String,Integer>aid,
                          ConcurrentHashMap<String,Integer>cid) {
        sock = s;
        zkc = connector;
        currentID = idx;
        activeIDMap = aid;
        completedIDMap = cid;
    }

    public void run() {
        // Read first packet from the Client Driver and process.
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
            oos = new ObjectOutputStream(sock.getOutputStream());
            oos.flush();
            ois = new ObjectInputStream(sock.getInputStream());
        } catch (IOException x) {
            System.err.println("IOException when RequestHandler trying to open streams to ClientDriver.");
        }

        ClientRequest incomingRequest = null;
        try {
            incomingRequest = (ClientRequest) ois.readObject(); // blocking
        } catch (Exception e) {
            System.err.println("Error in Client Request Handler attempting to get first communication from client: "
                    + e.getMessage() );
        }

        if ( incomingRequest instanceof ClientNewJobRequest ) {
            
            String incomingMD5 = incomingRequest.md5;
            
            // 1. First need to parse incoming request md5 and assign this job a unique id.
            //      - handle duplicate id's by looking up md5->id in a concurrenthashmap, which
            //      is created and updated by a sub-daemon thread (which always runs as long as this primary
            //      machine is active).
            while ( activeIDMap.containsValue(currentID) || completedIDMap.containsValue(currentID) ) {
                // this id is already in the system somewhere. Assign a new one and try again.
                currentID = new Integer(currentID.intValue()+1);
            }
            
            // Make a bunch of new partition watcher threads that do the rest of the work for us.
            
            Stat stat = zkc.exists(workerPath,null); // no watch
            if (stat == null) { // then only create one partition
                ZkPacket toPartition = new ZkPacket(incomingMD5,null,1,1,null,null,null);
                Thread partThread = new Thread(new PartitionThread(zkc,toPartition,activeJobPath,completedJobPath,currentID),"PartitionThread1");
                partThread.start();
            } else { // get number of workers currently connected, make that many partitions
                int numPartitions = zkc.getNumChildren(workerPath);
                if (numPartitions > 20) { numPartitions = 20; } // impose limit on # threads to make
                
                // spawn ALL DEM THREADS
                for (int i = 1;i<=numPartitions;i++) {
                    ZkPacket toPartition = new ZkPacket(incomingMD5,null,i,numPartitions,null,null,null);
                    Thread iterThread = new Thread(new PartitionThread(zkc,toPartition,activeJobPath,completedJobPath,currentID),"PartitionThread"+i);
                    iterThread.start();
                }
            }

            //TODO: Send packet back to ClientDriver with job id that was submitted.

        } else if ( incomingRequest instanceof ClientJobQueryRequest ) {
            // TODO: Handle job status requests (connect to zookeeper, check if already completed or in-flight, return).
        } else {
            System.err.println("Un-recognized ClientRequest (it is not one of the two checked-for subclasses). Dropping it.");
        }
    }
}

/* Code which interacts with the Zookeeper to implement a client-facing job committer & progress tracking
 * service. Backed up & can function either as primary or secondary server.
 * Primary functionality: Take new client "tasks" which are essentially brute-force MD5 cracks, and create
 * a tree-structure in Zookeeper that worker threads interface with to get certain partitions of a filesystem.
 */
public class JobTracker {

    /* Root zookeeper path for who is the primary jobtracker */
    String primaryPath = "/primaryJobTracker";
    String activeJobPath = "/activeJobs";
    String completedJobPath = "/completedJobs";
    ServerSocket listeningSock = null;
    ZkConnector zkc;
    Watcher watcher;
    boolean isPrimary = false;
    Integer serverPort = null;

    // Maps for ID's that are already in the system, and ID's that are already completed. Maintained by a daemon
    // thread which is spawned as soon as this JobTracker becomes primary. 
    ConcurrentHashMap<String,Integer> activeIDMap = null;
    ConcurrentHashMap<String,Integer> completedIDMap = null;

    // Random generator which assigns the "start id" that each RequestHandler will first use.
    Random idGen = new Random();

    public void main(String[] args) {

        String hosts = null;
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort serverPort");
            return;
        } else {
            hosts = args[0];
            serverPort = new Integer(args[1]);
        }

        /* Setup and overall workflow for a job tracker:
         *  1. Check connectivity to ZK, if not up then return an error and gracefully exit.
         *  2. Start up and try to become the primary Jobtracker - but if we can't do that then chill and become the backup.
         *  3. Once we are the primary, accept incoming requests from ClientDrivers and work with the znodes that store active
         *     and currently processing jobs.
         */
        zkc = new ZkConnector();

        try {
            // parse cmd args and create a new connection to zk
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            } 
        };

        //2: Try to become the primary JobTracker. Can't block on accepting new connections if we are backup.
        while(!isPrimary) { 
            try {
                Thread.sleep(5000); // so we dont spam zk repeatedly
            } catch(Exception consumed) {} 
            becomePrimary(); 
        }

        // Ensure that both the active and completed job path nodes exist.
        createJobNodes();

        // Getting here means we are primary. Initialize hashmaps and get ready to begin accepting connections.
        activeIDMap = new ConcurrentHashMap<String,Integer>();
        completedIDMap = new ConcurrentHashMap<String,Integer>();
        // Now start ID mapper daemon thread.
        Thread idDaemonThread = new Thread(new IDDaemonThread(activeIDMap,completedIDMap,zkc,activeJobPath,completedJobPath),"IDDaemonThread");
        idDaemonThread.start();

        // open up a server socket to block on for client connections
        try {
            listeningSock = new ServerSocket(serverPort.intValue());
        } catch (IOException x) {
            System.err.println("IOException when JobTracker trying to open server port: " + x.getMessage() );
        }

        /* Block on server socket for new connection from ClientDriver */
        while(true) { // as long as we are alive, we are primary
            Socket recvSocket = null;
            try {
                recvSocket = listeningSock.accept();
            } catch (IOException x) {
                System.err.println("IOException when JobTracker trying to accept new connection"
                        + x.getMessage());
            }
            // create a handler thread that does all of the handling operations
            Thread t = new Thread(
                       new RequestHandler(recvSocket,zkc,new Integer(idGen.nextInt()),activeIDMap,completedIDMap),
                       "RequestHandlerThread");
            t.start();
        }
    }

    private void becomePrimary() {
        Stat stat = zkc.exists(primaryPath,watcher);
        if (stat == null) {
            System.out.println("JobTracker with thread ID# " + Thread.currentThread().getId()
                    + " trying to become the primary JobTracker");
            // make zkpacket that holds this primary ip+port to connect on
            ZkPacket primaryPack = null;
            try {
                primaryPack = new ZkPacket(null,null,0,0,serverPort,InetAddress.getLocalHost(), null);
            } catch(UnknownHostException x) {
                System.err.println("JobTracker encountered UnknownHostException: " + x.getMessage()
                        + " when trying to getLocalHost() in becomePrimary().");
            }

            Code ret = zkc.create(primaryPath,primaryPack,CreateMode.EPHEMERAL); // need ephemeral so backup wakes up.
            if (ret == Code.OK) {
                System.out.println("JobTracker with thread ID# " + Thread.currentThread().getId()
                        + " is now primary JobTracker!");
                isPrimary = true; // now we can begin to accept incoming connections from ClientDrivers
            }
        }
    }

    // This happens when we see an event on the watched node (only use this watcher to determine primary and
    // secondary jobtrackers).
    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if (path.equalsIgnoreCase(primaryPath)) {
            if (type == EventType.NodeDeleted) { // old primary dies
                System.out.println("Old primary deleted, now try to become primary!");
                becomePrimary();
            }
            if (type == EventType.NodeCreated) {
                System.out.println(primaryPath + "created, this msg printed from handleEvent()");
                try {
                    Thread.sleep(2000);
                } catch (Exception consumed) {}
                becomePrimary(); // re-enable watch if spurious trigger
            }
        }
    }

    // Happens once on becoming primary. If these nodes exist, don't bother doing anything.
    private void createJobNodes() {
        Stat stat = zkc.exists(activeJobPath,null); // no watch
        if (stat == null) {
            System.out.println("Current primary job tracker creating active job root node on startup.");

            Code ret = zkc.create(activeJobPath,(byte[])null,CreateMode.PERSISTENT); 
            if (ret == Code.OK) {
                System.out.println("Active job root node now exists.");
            }
        }

        stat = zkc.exists(completedJobPath,null); // no watch
        if (stat == null) {
            System.out.println("Current primary job tracker creating completed job root node on startup.");

            Code ret = zkc.create(completedJobPath,(byte[])null,CreateMode.PERSISTENT); 
            if (ret == Code.OK) {
                System.out.println("Completed job root node now exists.");
            }
        }
    }
}
