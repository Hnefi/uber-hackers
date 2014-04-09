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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.util.List;

class WorkerHandler implements Runnable {
    Socket sock = null;
    DictionaryDB dict = null;

    public WorkerHandler( Socket s, DictionaryDB d ){
        sock = s;
        dict = d;
    }    
   
    public void run(){ 
        // Read first packet from the Worker and process.

        //Protocol:
        //  Worker sends the FileServer a DictionaryRequestPacket featuring two numbers representing a numerator/denominator pair
        //  FileServer sends the Worker a DictionaryRequestPacket featuring a list of strings corresponding to that section of the dictionary
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
            oos = new ObjectOutputStream(sock.getOutputStream());
            oos.flush();
            ois = new ObjectInputStream(sock.getInputStream());
        } catch (IOException x) {
            System.err.println("IOException when WorkerHandler trying to open streams to Worker.");
        }

        DictionaryRequestPacket incomingRequest = null;
        try {
            incomingRequest = (DictionaryRequestPacket) ois.readObject(); // blocking
        } catch (Exception e) {
            System.err.println("Error in Client Request Handler attempting to get first communication from client: "
                    + e.getMessage() );
        }

        List<String> dictionarySection = null;
        if (incomingRequest.partId != null && incomingRequest.totalParts != null){
            dictionarySection = dict.getSection(incomingRequest.partId, incomingRequest.totalParts);  
        }

        try {
            oos.writeObject(new DictionaryRequestPacket(null, null, dictionarySection));
        
            //Now that we've sent the section, close the socket and die.
            //oos.close();
            //ois.close();
            //sock.close();
        } catch (IOException e) {
            System.err.println("IOException when WorkerHandler trying to send dictionary section to Worker.");
        }
    }

}

public class FileServer {

    //This class instantiates a dictionary DB and handles the socket/Zookeeper communication
    String primaryPath = "/primaryFS";

    ZkConnector zkc;
    Watcher watcher;

    int serverPort;
    boolean isPrimary = false;

    ServerSocket listeningSock = null;

    public void main(String[] args){
        if (args.length != 3){
            System.out.println("Usage: FileServer <DICT> <ZKPORT> <MYPORT>");
            System.exit(-1);
        }

        //Process arguments
        DictionaryDB dict = new DictionaryDB(args[0]);
        String zkLoc = args[1];
        serverPort = Integer.parseInt(args[2]);

        //Connect to Zookeeper
        zkc = new ZkConnector();
        try {
            zkc.connect(zkLoc);
        } catch (Exception e){
            System.out.println("Zookeeper connect "+ e.getMessage());
        } 
    
        watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event){
                handleEvent(event);   
            }
        };

        //Wait until you are the primary FileServer
        while(!isPrimary){
            becomePrimary();
            if (!isPrimary){
                try{
                    Thread.sleep(5000); //don't overload Zookeeper
                } catch (Exception consumed) {}
            }
        }

        //If we're here, we're the primary FS so bring up the server sockets
        try {
            listeningSock = new ServerSocket(serverPort);
        } catch (IOException x){
            System.err.println("IOException when FileServer trying to open server port: " + x.getMessage() );
        }

        /* Now block on this socket for requests from the workers */
        while(true){
            Socket recvSocket = null;
            try {
                recvSocket = listeningSock.accept();
            } catch (IOException x){
                System.err.println("IOException when FileServer trying to accept new connection"
                        + x.getMessage()); 
            }
            //create a handler thread to communicate to the worker
            Thread t = new Thread ( new WorkerHandler(recvSocket, dict) );

            t.start(); 
        } 
    }

    //Primary election code, similar to JobTracker
    private void becomePrimary() {
        Stat stat = zkc.exists(primaryPath,watcher);
        if (stat == null) {
            System.out.println("FileServer with thread ID# " + Thread.currentThread().getId()
                    + " trying to become the primary FileServer");
            // make zkpacket that holds this primary ip+port to connect on
            ZkPacket primaryPack = null;
            try {
                primaryPack = new ZkPacket(null,0,0,serverPort,InetAddress.getLocalHost(), null);
            } catch(UnknownHostException x) {
                System.err.println("FileServer encountered UnknownHostException: " + x.getMessage()
                        + " when trying to getLocalHost() in becomePrimary().");
            }

            Code ret = zkc.create(primaryPath,primaryPack,CreateMode.EPHEMERAL); // need ephemeral so backup wakes up.
            if (ret == Code.OK) {
                System.out.println("FileServer with thread ID# " + Thread.currentThread().getId()
                        + " is now primary FileServer!");
                isPrimary = true; // now we can begin to accept incoming connections from ClientDrivers
            }
        }
    }

    // This happens when we see an event on the watched node (only use this watcher to determine primary and
    // secondary fileservers).
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

/*
    WORKER CODE!!
    public static String getHash(String word) {

        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }
*/
}
