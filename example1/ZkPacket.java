import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class ZkPacket implements Serializable {
    public String md5;
    public int partId;
    public int totalNum;

    public ZkPacket(String m, int i, int n){
        md5 = m;
        partId = i;
        totalNum = n;
    }

    public byte[] asBytes(){
        return ZkPacket.asBytes(this);
    }

    public static byte[] asBytes(ZkPacket p){
        byte[] ret = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(p);
            ret = out.toByteArray();
        } catch (java.io.IOException e) {
            System.out.println(e.getMessage());
            return null;
        }
        return ret;
    }

    public static ZkPacket asPacket(byte[] b){
        ZkPacket ret = null;
        if (b != null){
            try {
                ByteArrayInputStream in = new ByteArrayInputStream(b);
                ObjectInputStream is = new ObjectInputStream(in);
                Object o = is.readObject();
                if (o instanceof ZkPacket){
                    ret = (ZkPacket) o;
                }
            } catch (java.io.IOException e) {
                System.out.println(e.getMessage());
                return null;
            } catch (ClassNotFoundException e) {
                System.out.println(e.getMessage());
                return null;
            }
        }
        return ret; 
    }
}
