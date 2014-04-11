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
import java.io.Console;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.Scanner;

public class ClientDriver {
    Watcher watcher; // watches for current primary jobtracker to connect to
    Socket sock = null;
    ObjectOutputStream oos = null;
    ObjectInputStream ois = null;
    ZkConnector zkc = null;
    InetAddress jtAddr = null;
    Integer jtPort = null;
    

    private class ClientInput {
        private Scanner scanner = new Scanner(System.in); // only one of these can exist
        private Console console = System.console();

        public String readInput() {
            System.out.println("Enter command for md5 system:");

            return console.readLine();
        }
    }

    public static void main(String[] args) {
        ClientDriver cd = new ClientDriver();
        cd.localMain(args);
    }

    public void localMain(String[] args) {
        String hosts = null;
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver zkServer:clientPort");
            return;
        } else {
            hosts = args[0];
        }

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

        setupJobTracker();
        ClientInput scanner = new ClientInput();

        while(true) { // can't explain that
            String userInput = scanner.readInput(); // blocking?
            // parse
            String[] parts = userInput.split(" ");
            if(parts[0].equals("newJob")) {
                makeJob(parts[1]);    
            } else if (parts[0].equals("checkJob")) {
                checkJob(parts[1]);
            } else {
                System.out.println("Invalid input command, please enter either 'newJob <md5>' or 'checkJob <jobID>'.");
                continue;
            }
        }

    }

    private void makeJob(String md5) {
        boolean opened = makeSocket();
        if(opened) {
            ClientNewJobRequest toJT = new ClientNewJobRequest();
            toJT.md5 = md5;

            try {
                oos.writeObject(toJT);
            } catch (IOException x) {
                System.out.println("IOException w. message: " + x.getMessage() + " in sending object for makeJob()");
            }

            /* Read back the job id */
            TrackerResponse tr = null;
            try {
                tr = (TrackerResponse) ois.readObject(); // blocking
            } catch (ClassNotFoundException x) {
                System.out.println("ClassnotfoundException when reading back object from jobtracker.");
            } catch (IOException x) {
                // this will happen if the jobtracker gets killed.
                closeSocket();
                System.out.println("IOException w. message: " + x.getMessage() + " when waiting for TrackerResponse after job request. A job ID was not returned for job request with md5: " + md5);
                setupJobTracker();
            }
            // Add what type of packet I got back....
            if (tr.responseType == TrackerResponse.JOB_ID) {
                System.out.println("Your job id is: " + tr.jobID.intValue() + " for md5: " + md5);
            } else if (tr.responseType == TrackerResponse.RESULT_FOUND) {
                if (tr.password == null) {
                    System.out.println("No password found...");
                } else {
                    System.out.println("We have hacked it, the password IS: " + tr.password);
                }
            }
            closeSocket();
        } else {
            System.out.println("No jobTracker connected..... Your job was not submitted.");
        }
    }

    private void checkJob(String jid) {
        boolean opened = makeSocket();
        if (opened) {
            ClientJobQueryRequest toJT = new ClientJobQueryRequest();
            toJT.jobID = new Integer(jid);

            try {
                oos.writeObject(toJT);
            } catch (IOException x) {
                System.out.println("IOException w. message: " + x.getMessage() + " in sending object for checkjob()");
            }

            /* Read back the result */
            TrackerResponse tr = null;
            try {
                tr = (TrackerResponse) ois.readObject(); // blocking
            } catch (ClassNotFoundException x) {
                System.out.println("ClassnotfoundException when reading back object from jobtracker.");
            } catch (IOException x) {
                // this will happen if the jobtracker gets killed.
                closeSocket();
                System.out.println("IOException w. message: " + x.getMessage() + " when waiting for TrackerResponse after job status request.");
                setupJobTracker();
            }
            if (tr.password == null) {
                System.out.println("No password found...");
            } else {
                System.out.println("We have hacked it, the password IS: " + tr.password);
            }
            closeSocket();
        } else {
            System.out.println("No jobTracker connected..... Your query was not submitted.");
        }
    }

    private void setupJobTracker() {
        Stat stat = zkc.exists(zkc.primaryJobTrackerPath,watcher);
        if (stat != null) {
            /* Get primary jobtracker from socket */
            String jobTrackerPath = zkc.primaryJobTrackerPath;
            ZkPacket retPacket = zkc.getPacket(jobTrackerPath,false,null);
            jtAddr = retPacket.jobTrackerIP;
            jtPort = retPacket.jobTrackerPort;
        } else {
            jtAddr = null;
            jtPort = null;
        }
    }

    private void closeSocket() {
        try {
            ois.close();
            oos.close();
            sock.close();
        } catch (IOException x) {
            System.out.println("Error: " + x.getMessage() + " in trying to close sockets.");
        }
        sock = null;
        oos = null;
        ois = null;
    }

    private boolean makeSocket() {
        // connect
        if (jtAddr != null && jtPort != null) {
            try {
                sock = new Socket(jtAddr,jtPort.intValue());
                oos = new ObjectOutputStream(sock.getOutputStream());
                oos.flush();
                ois = new ObjectInputStream(sock.getInputStream());
                return true;
            } catch (IOException x) {
                System.out.println("ioException w. message: " + x.getMessage() + " in makeSocket() of clientDriver");
            }
        }
        return false;
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if (path.equalsIgnoreCase(ZkConnector.primaryJobTrackerPath)) {
            if (type == EventType.NodeDeleted) { // old primary dies
                System.out.println("Old job tracker offline, reset parameters for new one.");
                setupJobTracker();
            }
            if (type == EventType.NodeCreated) {
                System.out.println(ZkConnector.primaryJobTrackerPath + "created, this msg printed from handleEvent()");
                try {
                    Thread.sleep(2000);
                } catch (Exception consumed) {}
                setupJobTracker(); // re-enable watch if spurious trigger
            }
        }
    }
}
