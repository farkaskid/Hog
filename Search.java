package fileFork;

import java.io.IOException;

/**
 * Created by farkaskid on 14/3/16.
 */
public class Search {
    public static void main(String a[]) throws IOException, InterruptedException {
        Engine E = new Engine();
        E.importSystem("/home/farkaskid/fileFork_DBs/", "Test2");
        double start = System.nanoTime();
        Resultset r1 = E.search("Name", "=", "AAdam9");
//        Resultset r2 = E.search("Name", "<", "mAdam2300");
//        r1.unite(r2);
        r1.getdata();
        r1.show();
        double end = System.nanoTime();
        System.out.println("The time is: " + Double.toString((end - start)/1000000000) + " Members: " + Integer.toString(r1.size()));
        E.shutdown();
    }
}
