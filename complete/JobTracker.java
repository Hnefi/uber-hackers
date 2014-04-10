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

/* class to store the job id and how many active partitions are going */
class Job {
    Integer jobID;
    int remainingParts;

    public Job(Integer i, int j) {
        jobID = i;
        remainingParts = j;
    }

    @Override
    public boolean equals(Object o) {
        if(o == null) return false;
        if (o == this) return true;
        if (! (o instanceof Job)) return false;

        Job rhs = (Job)o;
        if (this.jobID.equals(rhs.jobID)) { // only matters is the same jobID
            return true;
        }
        return false;
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
    JobTracker parentTracker;

    public PartitionThread(ZkConnector z, ZkPacket p,Integer i,JobTracker jt) {
        zkc = z;
        packet = p;
        activeJobPath = ZkConnector.activeJobPath;
        completedPath = ZkConnector.completedJobPath;
        jobID = i;
        parentTracker = jt;

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
                    ZkPacket workerResults = zkc.getPacket(finishedNodePath,false,null); // TODO: check if this is the wrong stat???
                    String md5Key = workerResults.md5;

                    if (workerResults.password != null) { 
                        String newCompletedJobPath = completedPath + "/" + jobID.toString();
                        Code ret = zkc.create(newCompletedJobPath,workerResults,CreateMode.PERSISTENT);
                        if (ret == Code.OK) {
                            System.out.println("New finished result created for job id: " + jobID.toString());
                        }

                        // now delete the ephemeral 'taken' node as well as the partition itself
                        String takenPath = thisNodePath + "/taken";
                        ret = zkc.delete(takenPath,-1); // -1 matches any version number
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
                        parentTracker.registerPartitionCompleted(md5Key,jobID);
                        Thread.currentThread().interrupt();
                        return;
                    }

                    // Call back to the jobTracker and register that one of the partitions for this md5 was completed, but no result
                    // created/required.
                    parentTracker.registerPartitionCompleted(md5Key,jobID);

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
    JobTracker parentTracker;

    public RequestHandler(Socket s,ZkConnector connector,Integer idx,JobTracker jt) {
        sock = s;
        zkc = connector;
        currentID = idx;
        parentTracker = jt;
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
            // Make a bunch of new partition watcher threads that do the rest of the work for us.
            Stat stat = zkc.exists(ZkConnector.workerPoolPath,null); // no watch
            if (stat == null) { // then only create one partition
                ZkPacket toPartition = new ZkPacket(incomingMD5,null,1,1,null,null,null);
                Thread partThread = new Thread(new PartitionThread(zkc,toPartition,currentID,parentTracker),"PartitionThread1");
                partThread.start();

                // Callback to parent signalling that job successfully created.
                parentTracker.addActiveJobToMap(incomingMD5,currentID,1);
            } else { // get number of workers currently connected, make that many partitions
                int numPartitions = zkc.getNumChildren(ZkConnector.workerPoolPath);
                if (numPartitions > 20) { numPartitions = 20; } // impose limit on # threads to make

                // spawn ALL DEM THREADS
                for (int i = 1;i<=numPartitions;i++) {
                    ZkPacket toPartition = new ZkPacket(incomingMD5,null,i,numPartitions,null,null,null);
                    Thread iterThread = new Thread(new PartitionThread(zkc,toPartition,currentID,parentTracker),"PartitionThread"+i);
                    iterThread.start();
                }

                // Callback to parent signalling that job successfully created.
                parentTracker.addActiveJobToMap(incomingMD5,currentID,numPartitions);
            }

            TrackerResponse toClient = new TrackerResponse();
            toClient.responseType = TrackerResponse.JOB_ID;
            toClient.jobID = currentID;

            try {
                oos.writeObject(toClient);
            } catch (IOException x) {
                System.err.println("IOException w. error: " + x.getMessage() + " when responding to ClientDriver w. ID.");
            }
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
    ServerSocket listeningSock = null;
    ZkConnector zkc;
    Watcher watcher;
    boolean isPrimary = false;
    Integer serverPort = null;

    // Maps for ID's that are already in the system, and ID's that are already completed. Maintained by a daemon
    // thread which is spawned as soon as this JobTracker becomes primary. 
    ConcurrentHashMap<String,Job> activeIDMap = null;
    ConcurrentHashMap<String,Job> completedIDMap = null;

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
        activeIDMap = new ConcurrentHashMap<String,Job>();
        completedIDMap = new ConcurrentHashMap<String,Job>();
        updateIDMaps();

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
            // 1. First need to parse incoming request md5 and assign this job a unique id.
            //      - handle duplicate id's by looking up md5->id in a concurrenthashmap, which
            //      is created and updated by a sub-daemon thread (which always runs as long as this primary
            //      machine is active).
            Integer currentID = new Integer(idGen.nextInt());
            while ( activeIDMap.containsValue(currentID) || completedIDMap.containsValue(currentID) ) {
                // this id is already in the system somewhere. Assign a new one and try again.
                currentID = new Integer(currentID.intValue()+1);
            }

            Thread t = new Thread(
                    new RequestHandler(recvSocket,zkc,currentID,this),"RequestHandlerThread");
            t.start();
        }
    }

    public synchronized void addActiveJobToMap(String incomingMD5,Integer currentID,int numParts) {
        Job toMap = new Job(currentID,numParts);
        Job sanityCheck = activeIDMap.put(incomingMD5,toMap);
        if (sanityCheck != null) {
            System.out.println("Sanity check failed in addActiveJobToMap()! This md5 was already mapped to a job id.");
        }
    }

    public synchronized void registerPartitionCompleted(String key,Integer jobID) {
        Job fromMap = activeIDMap.get(key);
        fromMap.remainingParts -= 1;
        if (fromMap.remainingParts == 0) {
            // remove from active map and put in completed map
            activeIDMap.remove(key);
            completedIDMap.put(key,fromMap);
        } else {
            activeIDMap.put(key,fromMap);
        }
    }

    // Only called once upon becoming the primary jobTracker... traverse all active children partitions
    // and existing completed jobs.
    private void updateIDMaps() {
        List<String> activeChildren = zkc.getChildren(ZkConnector.activeJobPath,false);

        if (activeChildren != null) {
            for (String elem : activeChildren) {
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
                Job toMap = new Job(jobID,0); // currently 0 jobs outstanding, will need to update this

                ZkPacket nodeData = zkc.getPacket(elem,false,null); //TODO: Confirm what stat to pass???
                String md5Key = nodeData.md5;

                if (!activeIDMap.containsValue(toMap)) { // this is a new job ID, need to add it in the map
                    // grab data from this node
                    Job oldValue = activeIDMap.put(md5Key,toMap);
                    if (oldValue != null) {
                        System.err.println("Sanity check failed!!!!! MD5 " + md5Key + " previously mapped to jobID in ACTIVE!");
                    }
                } else { // this job is already here, we now need to increment the number of outstanding partitions
                    Job fromMap = activeIDMap.get(md5Key);
                    Job newJobToMap = new Job(fromMap.jobID,fromMap.remainingParts++);  
                    activeIDMap.put(md5Key,newJobToMap);
                }
            }
        }

        List<String> completedChildren = zkc.getChildren(ZkConnector.completedJobPath,false);

        if(completedChildren != null) {
            for (String elem : completedChildren) {
                // node's name is the job id, no processing needed
                Integer jobID = new Integer(elem);
                Job toMap = new Job(jobID,0);

                if (!completedIDMap.containsValue(toMap)) { // this is a new job ID, need to add it in the map
                    // grab data from this node
                    ZkPacket nodeData = zkc.getPacket(elem,false,null); //TODO: Confirm what stat to pass???
                    String md5Key = nodeData.md5;
                    Job oldValue = completedIDMap.put(md5Key,toMap);
                    if (oldValue != null) {
                        System.err.println("Sanity check failed!!!!! MD5 " + md5Key + " previously mapped to jobID in CMP!");
                    }
                }
            }
        }
    }

    private void becomePrimary() {
        Stat stat = zkc.exists(ZkConnector.primaryJobTrackerPath,watcher);
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

            Code ret = zkc.create(ZkConnector.primaryJobTrackerPath,primaryPack,CreateMode.EPHEMERAL); // need ephemeral so backup wakes up.
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
        if (path.equalsIgnoreCase(ZkConnector.primaryJobTrackerPath)) {
            if (type == EventType.NodeDeleted) { // old primary dies
                System.out.println("Old primary deleted, now try to become primary!");
                becomePrimary();
            }
            if (type == EventType.NodeCreated) {
                System.out.println(ZkConnector.primaryJobTrackerPath + "created, this msg printed from handleEvent()");
                try {
                    Thread.sleep(2000);
                } catch (Exception consumed) {}
                becomePrimary(); // re-enable watch if spurious trigger
            }
        }
    }

    // Happens once on becoming primary. If these nodes exist, don't bother doing anything.
    private void createJobNodes() {
        Stat stat = zkc.exists(ZkConnector.activeJobPath,null); // no watch
        if (stat == null) {
            System.out.println("Current primary job tracker creating active job root node on startup.");

            Code ret = zkc.create(ZkConnector.activeJobPath,(byte[])null,CreateMode.PERSISTENT); 
            if (ret == Code.OK) {
                System.out.println("Active job root node now exists.");
            }
        }

        stat = zkc.exists(ZkConnector.completedJobPath,null); // no watch
        if (stat == null) {
            System.out.println("Current primary job tracker creating completed job root node on startup.");

            Code ret = zkc.create(ZkConnector.completedJobPath,(byte[])null,CreateMode.PERSISTENT); 
            if (ret == Code.OK) {
                System.out.println("Completed job root node now exists.");
            }
        }
    }
}
