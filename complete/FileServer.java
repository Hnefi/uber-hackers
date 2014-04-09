import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.util.List;

public class FileServer {

    //This class instantiates a dictionary DB and handles the socket/Zookeeper communication

    public static void main(String[] args){
        if (args.length != 5){
            System.out.println("Usage: MD5Test <DICT> <PARTID> <TOTALPARTS> <ZKPORT> <MYPORT>");
            System.exit(-1);
        }
        DictionaryDB dict = new DictionaryDB(args[0]);

        List<String> wholeDict = dict.getSection(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        if (wholeDict != null){
            String word = wholeDict.get(0);
            System.out.println("There are "+String.valueOf(wholeDict.size())+" words in the dictionary!");
            String hash = getHash(word);
            System.out.println(hash); 
        } else {
            System.out.println("Invalid input!");
        }
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
