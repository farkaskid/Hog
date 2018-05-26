package hog;

import java.io.IOException;

/**
 * Created by farkaskid on 13/3/16.
 */
public class Insert {
    public static void main(String a[]) throws IOException{
        Entry e;
        Engine E = new Engine();
        E.createSystem("/home/farkaskid/fileFork_DBs", "Test2");
        E.addIndex("Name");

        double start = System.nanoTime();
        for(int i = 0 ; i < 10 ; i++){
            for(char c = 'a' ; c <= 'z' ; c++){
                e = E.request();
                e.addAttribute("Name", Character.toString(c) + "Adam" + Integer.toString(i));
                e.addAttribute("Number", Integer.toString(i));
                E.insert(e);
            }

            for(char c = 'A' ; c <= 'Z' ; c++){
                e = E.request();
                e.addAttribute("Name", Character.toString(c) + "Adam" + Integer.toString(i));
                e.addAttribute("Number", Integer.toString(i));
                E.insert(e);
            }
        }

        E.shutdown();
        double end = System.nanoTime();
        System.out.println("The time is: " + Double.toString((end - start)/1000000000));
    }
}
