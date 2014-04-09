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

import java.util.concurrent.ConcurrentHashMap;

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
    ZkConnector zkc;
    String activeJobPath = "/activeJobs";
    String completedJobPath = "/completedJobs";
    String primaryPath = "/primary";

    public RequestHandler(Socket s,ZkConnector connector) {
        sock = s;
        zkc = connector;
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

    /* Root zookeeper path for new client jobs. */
    String activeJobPath = "/activeJobs";
    String completedJobPath = "/completedJobs";
    String primaryPath = "/primary";
    ServerSocket listeningSock = null;
    ZkConnector zkc;
    Watcher watcher;

    public void main(String[] args) {

        String hosts = null;
        String serverPort = null;
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort serverPort");
            return;
        } else {
            hosts = args[0];
            serverPort = args[1];
        }

        /* Setup and overall workflow for a job tracker:
         *  1. Check connectivity to ZK, if not up then return an error and gracefully exit.
         *  2. Start up and try to become the primary Jobtracker - but if we can't do that then chill and become the backup.
         *  3. Once we are the primary, accept incoming requests from ClientDrivers and work with the znodes that store active
         *     and currently processing jobs.
         *  4.  
         */
        zkc = new ZkConnector();
        try {
            // parse cmd args and create a new connection to zk
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        // open up a server socket to block on for client connections
        listeningSock = new ServerSocket(Integer.parseInt(serverPort));

        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            } 
        };

        /* Block on server socket for new connection from ClientDriver */
        while(true) {
            Socket recvSocket = listeningSock.accept();
            // create a handler thread that does all of the handling operations
            Thread t = new Thread(new RequestHandler(recvSocket,zkc),"RequestHandlerThread");
            t.start();
        }
    }

    /*
       private void checkpath() {
       Stat stat = zkc.exists(myPath, watcher);
       if (stat == null) {              // znode doesn't exist; let's try creating it
       System.out.println("Creating " + myPath);
       Code ret = zkc.create(
       myPath,         // Path of znode
       null,           // Data not needed.
       CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
       );
       if (ret == Code.OK) System.out.println("the boss!");
       } 
       }

       private void handleEvent(WatchedEvent event) {
       String path = event.getPath();
       EventType type = event.getType();
       if(path.equalsIgnoreCase(myPath)) {
       if (type == EventType.NodeDeleted) {
       System.out.println(myPath + " deleted! Let's go!");       
       checkpath(); // try to become the boss
       }
       if (type == EventType.NodeCreated) {
       System.out.println(myPath + " created!");       
       try{ Thread.sleep(5000); } catch (Exception e) {}
       checkpath(); // re-enable the watch
       }
       }
       }
       */
    private void handleEvent(WatchedEvent event) { }
}
