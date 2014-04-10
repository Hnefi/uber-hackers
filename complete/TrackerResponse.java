import java.io.Serializable;

public class TrackerResponse implements Serializable {
    public int responseType = -1;
    public Integer jobID = null;
    public String password = null;

    public static final int RESULT_FOUND = 1;
    public static final int NO_RESULT = 2;
    public static final int JOB_ID = 3;
}