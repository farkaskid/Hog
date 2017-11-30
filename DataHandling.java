package fileFork;
import java.io.*;

/**
 * Created by farkaskid on 24/3/16.
 */
public class DataHandling {

    // FINDING THE FILE

    private String findFile(int fileno, String location){
        String file = Integer.toString(fileno);
        char[] filearray = file.toCharArray();
        StringBuffer location1 = new StringBuffer(location + "/Data");
        for (int i = 0 ; i < filearray.length - 1 ; i++) {
            location1.append("/" + Character.toString(filearray[i])) ;
        }

        return location1.append("/File" + file + ".txt").toString()  ;
    }

    // INSERTING THE FILE

    private String insertFile(int fileno, String location){
        String file = Integer.toString(fileno);
        char[] filearray = file.toCharArray();
        StringBuffer location1 = new StringBuffer(location + "/Data");
        for (int i = 0 ; i < filearray.length - 1 ; i++) {
            location1.append("/" + Character.toString(filearray[i])) ;
        }
        if(!new File(location1.toString()).exists()){
            (new File(location1.toString())).mkdirs();
        }
        return location1.append("/File" + file + ".txt").toString()  ;
    }

    // GET A NEW READER

    protected BufferedReader getReader(int fileno, String location){
        BufferedReader br = null;
        try {
             br = new BufferedReader(new FileReader(findFile(fileno, location)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return br;
    }

    // GET A NEW WRITER

    protected BufferedWriter getWriter(int fileno, String location){
        BufferedWriter br = null;
        try {
            br = new BufferedWriter(new FileWriter(insertFile(fileno, location)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return br;
    }

    // GET A FILE DESCRIPTOR

    protected File getFile(int fileno, String location){
        return (new File(findFile(fileno, location)));
    }
}
