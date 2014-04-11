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
import java.util.ArrayList;
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
    String finishedNodePath;
    Integer jobID;
    Watcher partitionWatcher;
    JobTracker parentTracker;

    public PartitionThread(String zkLocation,ZkPacket p,Integer i,JobTracker jt) {
        zkc = new ZkConnector();
        try {
            zkc.connect(zkLocation);
        } catch(InterruptedException consumed) {

        } catch (IOException x) {
            System.out.println("IOException: " + x.getMessage() + " in PartitionThread constructor.");
        }

        packet = p;
        activeJobPath = ZkConnector.activeJobPath;
        completedPath = ZkConnector.completedJobPath;
        jobID = i;
        parentTracker = jt;

        thisNodePath = activeJobPath + "/" + jobID.toString() + ":" + Integer.toString(packet.partId);
        System.out.println("Creating new partition thread with active path: " + thisNodePath);
        finishedNodePath = thisNodePath + ZkConnector.jobFinishedTag;

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
        } 

        checkForFinished(); // useful when recovering from previously killed jtracker.

        while(!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(10000); // don't overload this system with parallelism
            } catch (InterruptedException x) { 
                System.out.println("Thread ID #: " + Thread.currentThread().getId() + " exiting.");
                Thread.currentThread().interrupt(); // propagate to top level, thread killed.
            }
        }
    }

    private void checkForFinished() {
        Stat stat = zkc.exists(finishedNodePath,partitionWatcher);
        if (stat != null) {
            finishedNodeExists();
        } else {
            System.out.println("Watch set");
        }
    }

    private void finishedNodeExists() {
        System.out.println("Detected that the finished node exists.");
        // this job is finished! get the result packet and create a persistent results node inside ZK
        ZkPacket workerResults = zkc.getPacket(finishedNodePath,false,null); // TODO: check if this is the wrong stat???
        String md5Key = workerResults.md5;

        System.out.println("The password was successfully found as: " + workerResults.password);

        // now delete the ephemeral 'taken' node as well as the partition itself
        String takenPath = thisNodePath + ZkConnector.jobTakenTag;
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
        parentTracker.registerPartitionCompleted(md5Key,jobID,workerResults);
        System.out.println("Thread ID #: " + Thread.currentThread().getId() + " exiting.");
        Thread.currentThread().interrupt();
    }

    private void handleEvent(WatchedEvent event) {
        // This watch was set on a getChildren() call so it should wake up when children are created or removed.
        System.out.println("Partition thread with name: " + Thread.currentThread().getName() + " woke up on handleEvent().");
        String wokenPath = event.getPath();
        EventType wokenType = event.getType();
        if (wokenPath.equalsIgnoreCase(finishedNodePath)) { // watch triggered on right node.
            /*if(wokenType == EventType.NodeCreated) {
                System.out.println("Watcher detected finished node is completed.");
                finishedNodeExists();
            } else {
                checkForFinished();
            }*/
            checkForFinished();
        } else {
            System.out.println("Waking up on other path: " + wokenPath);
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
    String zkLoc;
    JobTracker parentTracker;
    ConcurrentHashMap<String,Job> activeIDMap = null;
    ConcurrentHashMap<String,Job> completedIDMap = null;

    public RequestHandler(Socket s,ZkConnector connector,String x,Integer idx,JobTracker jt,
            ConcurrentHashMap<String,Job>aid,
            ConcurrentHashMap<String,Job>cid) {
        sock = s;
        zkc = connector;
        zkLoc = x;
        currentID = idx;
        parentTracker = jt;
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

            if (activeIDMap.containsKey(incomingRequest.md5)) { // this md5 is already mapped
                TrackerResponse toClient = new TrackerResponse();
                toClient.responseType = TrackerResponse.DUPLICATE_MD5;
                try {
                    oos.writeObject(toClient);
                } catch (IOException x) {
                    System.err.println("IOException w. error: " + x.getMessage() + " when responding to ClientDriver w. ID.");
                }
                return;
            } else if (completedIDMap.containsKey(incomingRequest.md5)) { // this md5 already computed
                // Go to the completed node and look up.
                Job fromCompletedMap = completedIDMap.get(incomingRequest.md5);
                Integer jobID = fromCompletedMap.jobID;

                String pathToCmp = zkc.completedJobPath + "/" + jobID;
                ZkPacket nodeData = zkc.getPacket(pathToCmp,false,null); //TODO: Confirm what stat to pass???

                TrackerResponse toClient = new TrackerResponse();
                toClient.responseType = TrackerResponse.RESULT_FOUND;
                toClient.password = nodeData.password;

                try {
                    oos.writeObject(toClient);
                } catch (IOException x) {
                    System.err.println("IOException w. error: " + x.getMessage() + " when responding to ClientDriver w. ID.");
                }
                return;
            }

            String incomingMD5 = incomingRequest.md5;
            // Make a bunch of new partition watcher threads that do the rest of the work for us.
            Stat stat = zkc.exists(ZkConnector.workerPoolPath,null); // no watch
            if (stat == null) { // then only create one partition
                // Callback to parent signalling that job successfully created.
                parentTracker.addActiveJobToMap(incomingMD5,currentID,1);

                ZkPacket toPartition = new ZkPacket(incomingMD5,null,1,1,null,null,null);
                Thread partThread = new Thread(new PartitionThread(zkLoc,toPartition,currentID,parentTracker),"PartitionThread1");
                partThread.start();

            } else { // get number of workers currently connected, make that many partitions
                int numPartitions = stat.getNumChildren();
                if (numPartitions > 20) { numPartitions = 20; } // impose limit on # threads to make

                // Callback to parent signalling that job successfully created.
                parentTracker.addActiveJobToMap(incomingMD5,currentID,numPartitions);
                // spawn ALL DEM THREADS

                for (int i = 1;i<=numPartitions;i++) {
                    ZkPacket toPartition = new ZkPacket(incomingMD5,null,i,numPartitions,null,null,null);
                    Thread iterThread = new Thread(new PartitionThread(zkLoc,toPartition,currentID,parentTracker),"PartitionThread"+i);
                    iterThread.start();
                }

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
            // TODO: Handle different case of invalid job or no result.
            Integer incomingJobID = incomingRequest.jobID;
            Job toCompare = new Job(incomingJobID,0); // second arg doesn't matter for equality operation
            boolean validId = false;
            String result = null;
            if (completedIDMap.containsValue(toCompare)) { // lookup the result in zk
                System.out.println("Job ID: " + incomingJobID.intValue() + " is completed, looking up result.");
                String pathToCmp = zkc.completedJobPath + "/" + incomingJobID;
                ZkPacket nodeData = zkc.getPacket(pathToCmp,false,null); //TODO: Confirm what stat to pass???
                result = nodeData.password;
                validId = true;
            } else if (activeIDMap.containsValue(toCompare)) {
                validId = true;
            }

            TrackerResponse toClient = new TrackerResponse();
            if (result != null) {
                toClient.responseType = TrackerResponse.RESULT_FOUND;
                toClient.password = result;
            } else {
                if (validId)
                    toClient.responseType = TrackerResponse.NO_RESULT;
                else
                    toClient.responseType = TrackerResponse.INVALID_ID;
            }

            try {
                oos.writeObject(toClient);
            } catch (IOException x) {
                System.err.println("IOException w. error: " + x.getMessage() + " when responding to ClientDriver w. ID.");
            }
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
    String zkLoc;
    Watcher watcher;
    boolean isPrimary = false;
    Integer serverPort = null;

    // Maps for ID's that are already in the system, and ID's that are already completed. Maintained by a daemon
    // thread which is spawned as soon as this JobTracker becomes primary. 
    ConcurrentHashMap<String,Job> activeIDMap = null;
    ConcurrentHashMap<String,Job> completedIDMap = null;

    int newJobID = 0;

    public static void main(String[] args) {
        JobTracker jt = new JobTracker();
        jt.runMe(args);
    }

    public void runMe(String[] args) {

        String hosts = null;
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort serverPort");
            return;
        } else {
            hosts = args[0];
            zkLoc = hosts;
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
            Integer currentID = new Integer(newJobID);
            Job lookup = new Job(currentID,0);
            while ( activeIDMap.containsValue(lookup) || completedIDMap.containsValue(lookup) ) {
                // this id is already in the system somewhere. Assign a new one and try again.
                currentID = new Integer(currentID.intValue()+1);
                lookup.jobID = currentID;
            }

            Thread t = new Thread(
                    new RequestHandler(recvSocket,zkc,zkLoc,currentID,this,activeIDMap,completedIDMap),"RequestHandlerThread");
            t.start();
        }
    }

    public synchronized void addActiveJobToMap(String incomingMD5,Integer currentID,int numParts) {
        System.out.println("Callback to main JTracker, with MD5: " + incomingMD5 + " job id: " + currentID + " numParts: " + numParts);
        Job toMap = new Job(currentID,numParts);
        Job sanityCheck = activeIDMap.put(incomingMD5,toMap);

        if (sanityCheck != null) {
            System.out.println("Sanity check failed in addActiveJobToMap()! This md5 was already mapped to a job id.");
        }
    }

    public synchronized void registerPartitionCompleted(String key,Integer jobID,ZkPacket toCmpNode) {
        System.out.println("Registering one of the partitions completed for MD5: " + key + " job ID: " + jobID);
        Job fromMap = activeIDMap.get(key);
        fromMap.remainingParts -= 1;
        if (toCmpNode.password != null) {
            String newCompletedJobPath = ZkConnector.completedJobPath + "/" + jobID.toString();
            Code ret = zkc.create(newCompletedJobPath,toCmpNode,CreateMode.PERSISTENT);
            if (ret == Code.OK) {
                System.out.println("New finished result created for job id: " + jobID.toString());
            }
        }
        if (fromMap.remainingParts == 0) {
            // remove from active map and put in completed map
            System.out.println("Deregistering this id in the active map since all of the partitions are reg'd completed.");
            activeIDMap.remove(key);
            completedIDMap.put(key,fromMap);
            String newCompletedJobPath = ZkConnector.completedJobPath + "/" + jobID.toString();
            Code ret = zkc.create(newCompletedJobPath,toCmpNode,CreateMode.PERSISTENT);
            if (ret == Code.OK) {
                System.out.println("New finished result created for job id: " + jobID.toString());
            }
        } else {
            activeIDMap.put(key,fromMap);
        }
    }

    public boolean activeMapContains(String key) {
        return activeIDMap.containsKey(key);
    }

    // Only called once upon becoming the primary jobTracker... traverse all active children partitions
    // and existing completed jobs.
    private void updateIDMaps() {
        List<String> activeChildren = zkc.getChildren(ZkConnector.activeJobPath,false);
        List<Thread> threadsToStart = new ArrayList<Thread>();
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
                Job toMap = new Job(jobID,1); // currently 1 jobs outstanding, will need to update this

                String getPacketFrom = ZkConnector.activeJobPath + "/" + elem;
                //System.out.println(getPacketFrom);
                ZkPacket nodeData = zkc.getPacket(getPacketFrom,false,null); 
                String md5Key = nodeData.md5;

                if (!activeIDMap.containsValue(toMap) ) { // this is a new job ID, need to add it in the map
                    System.out.println("Adding new key: " + md5Key + " to activeIDMap.");
                    Job oldValue = activeIDMap.put(md5Key,toMap);
                    if (oldValue != null) {
                        System.err.println("Sanity check failed!!!!! MD5 " + md5Key + " previously mapped to jobID in ACTIVE!");
                    }
                } else { // increment number of outstanding partitions since it's not a new node
                    Job fromMap = activeIDMap.get(md5Key);
                    Job newJobToMap = new Job(fromMap.jobID,fromMap.remainingParts++);  
                    System.out.println("Incrementing outstanding count for key: " + md5Key + " to " + fromMap.remainingParts);
                    activeIDMap.put(md5Key,newJobToMap);
                }
                /* Need to create a new PartitionThread for this node regardless.*/
                Thread partThread = new Thread(new PartitionThread(zkLoc,nodeData,jobID,this),"PartitionThread");
                threadsToStart.add(partThread);
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
                    String getPacketFrom = ZkConnector.completedJobPath + "/" + elem;
                    ZkPacket nodeData = zkc.getPacket(getPacketFrom,false,null); //TODO: Confirm what stat to pass???
                    String md5Key = nodeData.md5;
                    Job oldValue = completedIDMap.put(md5Key,toMap);
                    if (oldValue != null) {
                        System.err.println("Sanity check failed!!!!! MD5 " + md5Key + " previously mapped to jobID in CMP!");
                    }
                }
            }
        }
        for (Thread idx : threadsToStart) {
            System.out.println("Starting Thread ID: " + idx.getId());
            idx.start();
        }

        System.out.println("Updated maps for ID's already in system.");
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
