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

import java.lang.Integer;

/* Thread which is responsible for updating the id->md5 maps that store the current id's and 
 * all outstanding id's. Spawned as soon as we become primary, and runs forever...
 * - note: this daemon thread does not ever change any nodes, but it keeps a watch on the active
 *   jobs node (since when we update the DATA there, we want to map that id in the current map).
 * - periodically wakes up and checks the results node for new results finished to add to maps
 */
class IDDaemonThread implements Runnable
{
    ZkConnector zkc;
    ConcurrentHashMap<Integer,String>activeIDMap = null;
    ConcurrentHashMap<Integer,String>completedIDMap = null;

    public IDDaemonThread(ConcurrentHashMap<Integer,String>activeM,
                          ConcurrentHashMap<Integer,String>completedM,
                          ZkConnector z) { // called from root main() method
        activeIDMap = activeM;
        completedIDMap = completedM;
        zkc = z;
    }

    public void run() {
        //TODO: All of this.
    }
}

/* Dumb threads which are created upon a new job getting mapped into the ZK system. */
class PartitionThread implements Runnable 
{

    public void run() {
        // TODO: All of this.
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

    // Maps which are used to check the ID validity.
    ConcurrentHashMap<Integer,String> activeIDMap = null;
    ConcurrentHashMap<Integer,String> completedIDMap = null;

    public RequestHandler(Socket s,ZkConnector connector,Integer idx,
                          ConcurrentHashMap<Integer,String>aid,
                          ConcurrentHashMap<Integer,String>cid) {
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
            // TODO: Handle new jobs (create threads, have them assign zk nodes, etc etc.
            // 1. First need to parse incoming request md5 and assign this job a unique id.
            //      - handle duplicate id's by looking up id->md5 in a concurrenthashmap, which
            //      is created and updated by a sub-daemon thread (which always runs as long as this primary
            //      machine is active).
             
            
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
    ServerSocket listeningSock = null;
    ZkConnector zkc;
    Watcher watcher;
    boolean isPrimary = false;
    Integer serverPort = null;

    // Maps for ID's that are already in the system, and ID's that are already completed. Maintained by a daemon
    // thread which is spawned as soon as this JobTracker becomes primary. 
    ConcurrentHashMap<Integer,String> activeIDMap = null;
    ConcurrentHashMap<Integer,String> completedIDMap = null;

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

        // Getting here means we are primary. Initialize hashmaps and get ready to begin accepting connections.
        activeIDMap = new ConcurrentHashMap<Integer,String>();
        completedIDMap = new ConcurrentHashMap<Integer,String>();
        // Now start ID mapper daemon thread.
        Thread idDaemonThread = new Thread(new IDDaemonThread(activeIDMap,completedIDMap,zkc),"IDDaemonThread");
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
                primaryPack = new ZkPacket(null,0,0,serverPort,InetAddress.getLocalHost());
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
}