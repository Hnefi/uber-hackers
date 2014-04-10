import java.io.Serializable;

/* Class which the ClientDrivers send over a socket to inform the JobTrackers what they want to do.
 * This is currently here just for compilation but will need to actually write this file at some point.
 */

public class ClientRequest implements Serializable {
    public String md5 = null;
    public Integer jobID = null;
}


class ClientNewJobRequest extends ClientRequest {
}

class ClientJobQueryRequest extends ClientRequest {

}

