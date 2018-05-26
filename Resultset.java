package hog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class Resultset{

    private LinkedList<Entry> records;
    private Set<Integer> files;
    protected final LinkedList<String> fileset;
    private final ExecutorService threadPool;
    private int noe;
    private final String location;
    protected DataHandling dh;
    // WORKER CLASSES

    // SPLITTER CLASS

    private class Splitter implements Callable<LinkedHashSet<Integer>> {

        final String temp;
        final LinkedHashSet<Integer> h;
        Splitter(String a){
            temp = a;
            h = new LinkedHashSet<Integer>();
        }

        @Override
        public LinkedHashSet<Integer> call() throws Exception {
            StringTokenizer st = new StringTokenizer(temp, ";");
            while(st.hasMoreTokens()){
                h.add(Integer.parseInt(st.nextToken()));
            }
            return h;
        }
    }

//    READER CLASS

    private class Reader implements Callable<Entry>{

        final int fileno;
        Entry E;
        Reader(int a){
            fileno = a;
            E = null;
        }

        @Override
        public Entry call() throws Exception {
            try {
                E = new Entry(fileno, location);
            } catch (IOException e) {}
            return E;
        }
    }

    // COMMIT CLASS

    private class Commit implements Callable<Boolean>{
        final Entry e;
        final boolean b;
        boolean result;
        Commit(Entry e){
            this.e = e;
            this.b = true;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                e.commit(b);
                result = true;
            } catch (IOException e1) {
                result = false;
            }
            return result;
        }
    }

//    CONSTRUCTOR

    protected Resultset(String s, ExecutorService es){
        records = new LinkedList<Entry>();
        files = new LinkedHashSet<Integer>();
        noe = 0;
        location = s;
        fileset = new LinkedList<String>();
        threadPool = es;
        dh = new DataHandling();
    }

    public final int size() {
        return noe;
    }

    // PREPARE FILES

    protected void prepare() throws InterruptedException {

        HashSet<Integer> h;
        int i;
        CompletionService<LinkedHashSet<Integer>> ecs ;
        if(files.isEmpty() && !fileset.isEmpty()){

            ecs = new ExecutorCompletionService<LinkedHashSet<Integer>>(threadPool);
            for (String aFileset : fileset) {
                ecs.submit(new Splitter(aFileset));
            }
            i = fileset.size();
            while(i != 0){
                try {
                    h = ecs.take().get();
                    files.addAll(h);
                    i--;
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            fileset.clear();
            noe = files.size();
        }
    }

    // SET OPERATIONS

    public final void unite(Resultset AnotherSet) {

        files.addAll(AnotherSet.files);
        noe = files.size();
    }

    public final void intersect(Resultset AnotherSet) {

        if(files.size() >= AnotherSet.files.size() )
            files.retainAll(AnotherSet.files);

        else{
            AnotherSet.files.retainAll(files);
            files = AnotherSet.files;
        }
        noe = files.size();
    }

    // GET DATA

   public void getdata() throws InterruptedException {

       CompletionService<fileFork.Entry> ecs;
       int i;
       fileFork.Entry e;

       ecs = new ExecutorCompletionService<fileFork.Entry>(threadPool);
       i = files.size();
       for (Integer file : files) {
           ecs.submit(new Reader(file));
       }

       while(i != 0){
           try {
               e = ecs.take().get();
               records.add(e);
               i--;
           } catch (ExecutionException e1) {
               e1.printStackTrace();
           }
       }
   }

//    void getdata1(){
//
//        int i;
//        if(!files.isEmpty()){
//            Iterator<Integer> it = files.iterator();
//            while(it.hasNext()){
//                i = it.next();
//                try {
//                    records.put(i, new fileFork.Entry(i, location));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }

//   REMOVE

   public final void remove() {
       Iterator<Integer> it = files.iterator();
       int i, f = 0;
       CompletionService ecs = new ExecutorCompletionService(threadPool);
       while(it.hasNext()){
           i = it.next();
           final int finalI = i;
           ecs.submit(new Callable() {
                          @Override
                          public Object call() throws Exception {
                              try {
                                  dh.getFile(finalI, location).delete();
                              } catch (Exception e) {}
                              return finalI;
                          }
                      }
                     );
       }
       i = files.size();
       while(i != 0){
           try {
               f = (Integer)ecs.take().get();
               i--;
           } catch (InterruptedException e) {
               e.printStackTrace();
           } catch (ExecutionException e) {
               e.printStackTrace();
           }
           files.remove(f);
       }
       records.clear();
       noe = 0;
   }

    // UPDATE

    private void update(boolean append, String key, String s[]) throws IOException, InterruptedException {

        getdata();
        Iterator it = records.iterator();
        Entry e;
        Attribute p;
        String[] arr, arvalues;
        int i, mark;
        boolean updated;
        BufferedReader br = new BufferedReader(new FileReader(location + "/Indexes/ListOfIndexes.txt"));
        ArrayList<String> indexes = new ArrayList<String>();
        String temp;
        arvalues = s;
        StringTokenizer st;
        while((temp = br.readLine())!= null && temp.length()!= 0){
            st = new StringTokenizer(temp, ":");
            indexes.add(st.nextToken());
        }
        br.close();

        if(indexes.contains(key)){

        }

        else{
            LinkedList<Entry> map = new LinkedList<Entry>();
            while(it.hasNext()){
                e = (Entry)it.next();
                updated = false;
                for (Attribute aData : e.data) {
                    p = aData;

                    if (p.key.equals(key) && append) {
                        arr = new String[p.values.length + arvalues.length];
                        for (i = 0; i < p.values.length; i++) arr[i] = p.values[i];
                        mark = i;
                        for (; i < p.values.length + arvalues.length; i++) arr[i] = arvalues[i - mark];
                        p.values = arr;
                        updated = true;
                    }

                    if (p.key.equals(key) && !append) {
                        arr = new String[arvalues.length];
                        for (i = 0; i < arr.length; i++) arr[i] = arvalues[i];
                        p.values = arr;
                        updated = true;
                    }

                    if (updated)
                        break;
                }

                if(!updated){
                    Attribute attribute1 = new Attribute(key, arvalues);
                    e.data.add(attribute1);
                }
                map.add(e);
            }
            records = map;
        }
    }

    public final void updateSet(int nov, boolean append, String...s) throws IOException, InterruptedException {
        String[] arvalues = new String[nov];
        String key;
        int k, j;

        for(int i = 0 ; i < s.length/(nov + 1) ; i++){
            k = i*(nov + 1);
            key = s[k];
            j = 1;
            for(;j <= nov ; j++) arvalues[j - 1] = s[k + j];
            update(append, key, arvalues);
        }
    }

    public final void commitChanges(boolean Choice) {
        Entry e;
        Iterator it = records.iterator();
        CompletionService ecs;

        if(Choice){
            ecs = new ExecutorCompletionService<Boolean>(threadPool);
            int i = 0;
            while(it.hasNext()){
                e = (Entry) it.next();
                i++;
                ecs.submit(new Commit(e));
            }
            while(i != 0){
                try {
                    ecs.take().get();
                    i--;
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (ExecutionException e1) {
                    e1.printStackTrace();
                }
            }
        }
        else{

            ecs = new ExecutorCompletionService<Entry>(threadPool);
            Iterator<Integer> it1 = files.iterator();
            int i = files.size();
            while(it1.hasNext()){
                ecs.submit(new Reader(it1.next()));
            }
            while(i != 0){
                try {
                    try {
                        e = (Entry) ecs.take().get();
                        records.add(e);
                        i--;
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                } catch (ExecutionException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public final void show() {
        Entry e;
        CompletionService<Entry> ecs;
        if(records.size() != files.size() || !fileset.isEmpty()) {
            if(files.isEmpty()){
                try {
                    prepare();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }

            ecs = new ExecutorCompletionService<Entry>(threadPool);

            for (Integer file : files) {
                ecs.submit(new Reader(file));
            }
            int i = files.size();
            while(i != 0){
                try {
                    i--;
                    e = ecs.take().get();
                    e.show();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (ExecutionException e1) {
                    e1.printStackTrace();
                } catch (Exception e1){}
            }

        }
        else{
            Iterator it = records.iterator();
            if(records.size() == 0){
                System.out.println("No Result(s)");
            }
            else if(records.size() == 1){
                System.out.println("1 Result");
                while (it.hasNext()){
                    e = (Entry)it.next();
                    e.show();
                }
            }
            else{
                System.out.println(Integer.toString(records.size()) + " Results");
                while (it.hasNext()){
                    e = (Entry)it.next();
                    e.show();
                }
            }
        }
    }
}
