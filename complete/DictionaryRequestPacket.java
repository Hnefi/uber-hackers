import java.util.ArrayList;
import java.io.Serializable;
import java.lang.Integer;

class DictionaryRequestPacket implements Serializable {
    //This class is the main communication between workers and the file server. Since Zookeeper tracks a primary fileserver,
    //each communication between workers and the fileserver involves opening a *new socket* and sending *exactly two of these
    //packets* between worker and server. The protocol is as follows:

    //The worker initiates a connection and request to the file server which is comprised of two numbers. The first, totalParts,
    //indicates how many sections the dictionary should be split into and the second, partId, corresponds to which of these sections
    //the worker is requesting. For example, { totalParts = 10, partId = 2 } corresponds to a worker requesting the second 10% of
    //the dictionary.
    public Integer totalParts = null;
    public Integer partId = null;

    //In the second request, the FileServer populates the packet with a list of Strings corresponding to the section of the dictionary
    //requested by the Worker.
    public ArrayList<String> dictSection = null;
   
    public DictionaryRequestPacket (Integer p, Integer t, ArrayList<String> d){
        partId = p;
        totalParts = t;
        dictSection = d;
    } 
}
