import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.CreateMode;

import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

class WorkerJob {
    public String md5 = null;
    public Integer partitionId = null;
    public Integer totalPartitions = null;
    public String zkPath = null;
    
    public WorkerJob (Integer p, Integer t, String m, String z){
        partitionId = p;
        totalPartitions = t;
        md5 = m;
        zkPath = z;
    }
}

class Worker {

    ZkConnector zkc;
    //Watcher newJobsWatcher;

    int myPort;
    
    Socket toFileServerSocket;

    WorkerJob curJob = null;
    
    public static void main (String[] args){
        Worker w = new Worker();
        w.run(args);
    }

    public void run(String[] args){
        if (args.length != 2){
            System.out.println("Usage: Worker <ZKLOC> <MYPORT>");
            System.exit(-1);
        }
    
        String zkLoc = args[0];
        myPort = Integer.parseInt(args[1]);

        zkc = new ZkConnector();
        try{
            zkc.connect(zkLoc);
        } catch (Exception e) {
            System.out.println("Zookeeper connect "+e.getMessage());
        }
        System.out.println("Connected to Zookeeper!");
        
        while (!Thread.currentThread().isInterrupted()){
            while(curJob == null){
                try{
                    Thread.sleep(5000); //don't overload Zookeeper
                } catch (Exception consumed) {}
                getJob();
            }
            processJob();
        }

    }

    //private void handleEvent(WatchedEvent event){

    //}

    private void getJob() {
        List<String> curPartitions = zkc.getChildren(ZkConnector.activeJobPath, false);
        if (curPartitions != null){
            for (String partitionPath : curPartitions){
                String jobPath = ZkConnector.activeJobPath + "/" + partitionPath; 
                Stat takenStat = zkc.exists(jobPath + ZkConnector.jobTakenTag, null);
                if (takenStat == null){
                    //Awesome! Try to create the taken tag - be quick! We're racing other workers!
                    Code createCode = zkc.create(partitionPath + ZkConnector.jobTakenTag, (String)null, CreateMode.EPHEMERAL);
                    if (createCode == Code.OK){
                        //SWEET! WOOORRRRKKKKK!
                        ZkPacket partitionData = zkc.getPacket(jobPath, false, null);
                        if (partitionData == null){
                            System.out.println("Found a null Packet in a job! AHHHHHHHHH!");
                            System.exit(-1);
                        }
                        curJob = new WorkerJob(partitionData.partId, partitionData.totalNum, partitionData.md5, jobPath);
                        break;
                    }
                } 
            } 
        }
    }

    private ArrayList<String> getDictionarySection(Integer pid, Integer numParts){
        //Three steps to getting a section of the dictionary to work on:
        //  1. Ask ZooKeeper where the primary FS is
        //  2. Open a new connection to the FS
        //  3. Ask the FS for a dictionary section and return it when it's done

        System.out.println("Retreiving dictionary section...");
        ArrayList<String> ret = null;
        while (ret == null){
            Socket toFileServerSocket = null;
            ObjectOutputStream toFS = null;
            ObjectInputStream fromFS = null;
            System.out.println("Establishing connection to FileServer...");
            while (toFileServerSocket == null){
                if (zkc.exists(ZkConnector.primaryFileServerPath, null) == null){
                    System.out.println("I see you killed a File Server.... I will go so sleep for a while and try again.");
                    try{
                        Thread.sleep(5000);
                    }catch(InterruptedException e){
                    }
                    System.out.println("*yawn*... What'd I miss?");
                    continue;
                }
                ZkPacket fileServerPacket = zkc.getPacket(ZkConnector.primaryFileServerPath, false, null); 
                if (fileServerPacket != null){
                    Integer fsPort = fileServerPacket.jobTrackerPort;
                    InetAddress fsAddr = fileServerPacket.jobTrackerIP;
    
                    try {
                        toFileServerSocket = new Socket(fsAddr, fsPort);
    
                        toFS = new ObjectOutputStream(toFileServerSocket.getOutputStream());
                        toFS.flush();
                        fromFS = new ObjectInputStream(toFileServerSocket.getInputStream());
                    } catch (UnknownHostException e) {
                        System.err.println("Unknown FileServer host at address "+fsAddr+":"+fsPort);
                    } catch (IOException e){
                        //do nothing; this is what happens if the FS is down, and ZooKeeper should fix this soon!
                    }
                }
            }

            DictionaryRequestPacket reqDict = new DictionaryRequestPacket(curJob.partitionId, curJob.totalPartitions, null);
            while (ret == null){
                try {
                    toFS.writeObject(reqDict); 
                    DictionaryRequestPacket fromFSPacket = (DictionaryRequestPacket)fromFS.readObject();
                    ret = fromFSPacket.dictSection;
                } catch (IOException e){
                    System.err.println("IOException waiting for dictionary list!");
                } catch (ClassNotFoundException e){
                    System.err.println("Unexpected packet type found from the FileServer!");
                }
            }

            try{
                toFS.close();
                fromFS.close();
                toFileServerSocket.close();
            } catch (IOException e){
            }
        }
        return ret;
    }

    private void processJob() {
        if (curJob == null){
            System.err.println("Null job made it into processJob!");
            System.exit(-1);
        }

        System.out.println("Processing job "+curJob.zkPath+":: "+curJob.partitionId+"/"+curJob.totalPartitions+" for md5: "+curJob.md5);

        //Get the list of words from the file server
        String jobMD5 = curJob.md5;
        String result = null;
        ArrayList<String> dictionarySection = getDictionarySection(curJob.partitionId, curJob.totalPartitions);

        //Do the work of hashing and finding a match
        for(String dictWord : dictionarySection){
            String md5 = getHash(dictWord);
            if (md5.equals(jobMD5)){
                result = dictWord;
                break;
            }
        }
      
        if (result != null){
            System.out.println("Your password was \""+result+"\" !!\n");
        } else {
            System.out.println("Could not find your password in the section!");
        } 
        //Create a completed node in Zookeeper and store the MD5 and result there
        ZkPacket donePacket = new ZkPacket(curJob.md5, result, curJob.partitionId, curJob.totalPartitions, null, null, null);

        int i = 5;
        while (i < 5){
            Code ret = zkc.create(curJob.zkPath + ZkConnector.jobFinishedTag,donePacket,CreateMode.PERSISTENT); // need ephemeral so backup wakes up.
            if (ret == Code.OK) {
                System.out.println("Successfully registered job finished!");
                break;
            }
            ++i;
        }
        if (i == 5){
            System.err.println("Cannot register taken job as finished!! Something must be horribly wrong. ... Bye!");
            System.exit(-1);
        }
        //Clear the curJob
        curJob = null; 
    }

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

}
