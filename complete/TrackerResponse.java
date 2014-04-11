import java.io.Serializable;

public class TrackerResponse implements Serializable {
    public int responseType = -1;
    public Integer jobID = null;
    public String password = null;

    public static final int RESULT_FOUND = 1;
    public static final int NO_RESULT = 2;
    public static final int JOB_ID = 3;
    public static final int DUPLICATE_MD5 = 4;
    public static final int INVALID_ID = 9001; // >9000, what do
}
