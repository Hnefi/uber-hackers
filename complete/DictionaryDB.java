import java.nio.charset.Charset;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class DictionaryDB {
    //Read in a file of strings to use as the Dictionary and provide APIs to get individual words
    //as well as sections defined by (ID, Total).

    //For example, calling getSection(2, 10) returns the second of ten parts of the dictionary.

    private final String dictFile;
    private final ArrayList<String> dictById;

    public DictionaryDB(String fileLoc){
        dictFile = fileLoc;
        dictById = new ArrayList<String>();

        Charset charset = Charset.forName("US-ASCII");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(dictFile));
            String line = null;
            while ((line = reader.readLine()) != null) {
                dictById.add(line);
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }     
    }

    public String getWord(int id){
        return dictById.get(id);
    }

    public ArrayList<String> getSection(int partitionId, int totalPartitions){
        if (partitionId > totalPartitions || partitionId <= 0 || totalPartitions <= 0){
            return null;
        }

        int numWords = dictById.size();
        
        //The last section has an extra couple of words which are the remainder after 
        //dividing the list evenly. Technically we should balance this remainder across 
        //the pieces in some manner to make it more fair, but that's too fancy.
        //The max size of this appending is (totalPartitions-1) words, which means there are 2 cases:
        //  - totalPartitions small == small difference in the last section size
        //  - totalPartitions large == small partition sizes anyway, so the last one being large matters less
        int lastSectionSize = numWords % totalPartitions;
        int numWordsPerPartition = (numWords - lastSectionSize) / totalPartitions;

        ArrayList<String> retList = new ArrayList<String>();

        int endingId = (partitionId == totalPartitions)? numWords : partitionId * numWordsPerPartition;
        for (int i = (partitionId - 1)*numWordsPerPartition; i < endingId; ++i){
            retList.add(dictById.get(i));
        }
        return retList;
    }
}
