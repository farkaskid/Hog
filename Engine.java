package hog;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.*;

@SuppressWarnings("WhileLoopReplaceableByForEach")
public class Engine {
    private int noe;
    private final int[] noet;
    private final ArrayList<String> keys;
    private final HashMap<String, Integer> keymap;
    private final int nop;
    private String location;
    private final ExecutorService threadPool;
    private final JobScheduler js;
    protected DataHandling dh;

    // CONSTRUCTOR

    public Engine() {
        keys = new ArrayList<String>();
        noe = 0;
        nop = Runtime.getRuntime().availableProcessors();
        threadPool = new ThreadPoolExecutor(nop, nop, 1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());

        js = new JobScheduler();
        noet = new int[nop];
        keymap = new HashMap<String, Integer>();
        dh = new DataHandling();
    }

    // CREATE METHOD

    final public void createSystem(String Location, String Name) throws IOException {
        location = Location + "/" + Name;
        new File(location).mkdirs();
        new File(location + "/Indexes").mkdirs();
        new File(location + "/Data").mkdirs();
        BufferedWriter bw;

        // NOE FILES

        bw = new BufferedWriter(new FileWriter(location + "/Noe.txt"));
        bw.write("0");
        bw.close();

        // LISTOFINDEXES FILE

        bw = new BufferedWriter(new FileWriter(location + "/" + "Indexes" + "/" + "ListOfIndexes.txt"));
        bw.close();
    }

    // IMPORT SYSTEM METHOD

    final public void importSystem(String Location, String Name) throws IOException {
        String temp, temp1;
        StringTokenizer st;
        location = Location + "/" + Name;
        BufferedReader br = null;
        File f = new File(location + "/Indexes");
        f.setExecutable(true);
        f.setReadable(true);
        f.setWritable(true);
        f = new File(location + "/Noe.txt");
        f.setExecutable(true);
        f.setReadable(true);
        f.setWritable(true);
        try {
            br = new BufferedReader(new FileReader(location + "/Noe.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        noe = Integer.parseInt(br.readLine());
        br.close();
        br = new BufferedReader(new FileReader(location + "/Indexes/ListOfIndexes.txt"));
        while ((temp = br.readLine()) != null && temp.length() != 0) {
            st = new StringTokenizer(temp, ":");
            temp = st.nextToken();
            temp1 = st.nextToken();
            if (temp1.equals("ALPHA"))
                keys.add(temp);

            else
                keymap.put(temp, Integer.parseInt(temp1));
        }
        br.close();
    }

    //  JOB-SCHEDULER

    private class JobScheduler {
        final ExecutorService[] es;
        final ExecutorCompletionService<Resultset>[] ecs = new ExecutorCompletionService[nop];
        int turn;

        public JobScheduler() {
            es = new ExecutorService[nop];
            for (int i = 0; i < nop; i++) {
                es[i] = Executors.newSingleThreadExecutor();
                ecs[i] = new ExecutorCompletionService<Resultset>(es[i]);
            }
            turn = 0;
        }

        void addJob(Entry e) {
            es[turn].submit(new Worker(turn, e));
            turn++;
            if (turn == nop)
                turn = 0;
        }

        Resultset search(String index, String query, String value) throws ExecutionException, InterruptedException {
            Resultset rs[] = new Resultset[nop];
            Resultset result = new Resultset(location, threadPool);
            String newValue = value;

            if (keymap.containsKey(index))
                newValue = String.format("%" + Integer.toString(keymap.get(index)) + "s", value).replace(" ", "0");

            for (int i = 0; i < nop; i++) {
                ecs[i].submit(new Finder(index, query, newValue, i));
            }

            for (int i = 0; i < nop; i++) {
                rs[i] = ecs[i].take().get();
            }

            for (int i = 0; i < nop; i++) {
                result.fileset.addAll(rs[i].fileset);
            }
            result.prepare();
            return result;
        }

        void makeIndex(String key, String value, int fno) {
            es[turn].submit(new Indexer(key, value, turn, fno));
            turn++;
            if (turn == nop)
                turn = 0;
        }

        void makeIndex(String key, String value, int fno, int degree) {
            es[turn].submit(new NumericIndexer(key, value, turn, fno, degree));
            turn++;
            if (turn == nop)
                turn = 0;
        }
    }

    //  WORKER THREAD

    private class Worker implements Callable<Integer> {

        final int tno;
        final Entry e;

        Worker(int a, Entry E) {
            tno = a;
            e = E;
        }

        @Override
        public Integer call() throws Exception {
            try {
                add(e, tno);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return tno;
        }
    }

    // INDEXER THREAD

    private class Indexer implements Callable<Integer> {

        final int tno;
        final int fno;
        final String key;
        final String value;

        Indexer(String key, String value, int a, int fno) {
            tno = a;
            this.fno = fno;
            this.key = key;
            this.value = value;
        }

        @Override
        public Integer call() throws Exception {
            try {
                indexEntry(key, value, fno, tno);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return tno;
        }
    }

    // FINDER THREAD

    private class Finder implements Callable<Resultset> {
        final String index;
        final String query;
        final String value;
        final int no;

        public Finder(String index, String query, String value, int a) {
            this.query = query;
            this.index = index;
            this.value = value;
            no = a;
        }

        @Override
        public Resultset call() throws Exception {
            return find(index, query, value, no);
        }
    }

    // SEARCH METHOD

    public final Resultset search(String Attribute, String Query, String Value) {
        Resultset result = null;
        try {
            result = js.search(Attribute, Query, Value);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    //  REQUEST VACANCY

    public final Entry request() {
        noe++;
        return new Entry(location, noe);
    }

    // INSERTION

    public final void insert(Entry Entry) {
        js.addJob(Entry);
    }

    // CLOSING THE POOL

    public final void shutdown() throws IOException {

        if (threadPool.isTerminated())
            threadPool.shutdown();

        else {
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(500, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < nop; i++) {
            if (js.es[i].isTerminated())
                js.es[i].shutdown();

            else {
                js.es[i].shutdown();
                try {
                    js.es[i].awaitTermination(500, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        BufferedWriter bw = new BufferedWriter(new FileWriter(location + "/Noe.txt"));
        bw.write(Integer.toString(noe));
        bw.close();

        // LOCKING FILES

    }

    // INDEX ENTRY

    private void indexEntry(String key, String value, int fileno, int threadno) throws IOException {

        StringBuffer indexpath;
        BufferedReader br;
        int n;
        char[] valuebase;
        String temp;
        char first;
        BufferedWriter bw = null;
        indexpath = new StringBuffer(location + "/Indexes/" + key + Integer.toString(threadno));
        valuebase = value.toCharArray();
        char ch = value.charAt(0);
        int j = 1;
        if (Character.isUpperCase(ch)) {
            if (!(new File(indexpath + "/$" + Character.toString(ch))).exists()) {
                indexpath.append("/$").append(Character.toString(ch));
                new File(indexpath.toString()).mkdirs();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/Index.txt"));
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/files.txt"));
                bw.write(Integer.toString(fileno) + ";");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/noe.txt"));
                bw.write("1");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/status.txt"));
                bw.write("nf");
                bw.close();
            } else j = 0;
        } else {
            if (!(new File(indexpath + "/" + Character.toString(ch))).exists()) {
                indexpath.append("/").append(Character.toString(ch));
                new File(indexpath.toString()).mkdirs();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/Index.txt"));
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/files.txt"));
                bw.write(Integer.toString(fileno) + ";");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/noe.txt"));
                bw.write("1");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/status.txt"));
                bw.write("nf");
                bw.close();
            } else j = 0;
        }
        first = ch;
        for (; j < valuebase.length; j++) {

            first = valuebase[j];

            // IF FIRST CHARACTER IS CAPITAL

            if (Character.isUpperCase(first) && (new File(indexpath + "/$" + Character.toString(first)).exists())) {

                // UPDATING THE INDEXPATH

                indexpath.append("/$").append(Character.toString(first));
            } else if (new File(indexpath + "/" + Character.toString(first)).exists()) {

                // UPDATING THE INDEXPATH

                indexpath.append("/").append(Character.toString(first));
            } else break;

            // UPDATING THE FILES STORE

            bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/files.txt", true));
            bw.write(Integer.toString(fileno) + ";");
            bw.close();

            // UPDATING THE NUMBER OF ELEMENTS

            br = new BufferedReader(new FileReader(indexpath.toString() + "/noe.txt"));
            n = Integer.parseInt(br.readLine());
            br.close();
            n++;
            bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/noe.txt"));
            bw.write(Integer.toString(n));
            bw.close();
        }

        br = new BufferedReader(new FileReader(indexpath.toString() + "/status.txt"));
        temp = br.readLine();
        br.close();

        if (temp.equals("nf")) {

            // CHECKING FOR REPEATED VALUES

            br = new BufferedReader(new FileReader(indexpath.toString() + "/Index.txt"));
            int c, i;
            ArrayList<String> temp1 = new ArrayList<String>();
            Attribute p;
            c = 0;
            StringBuilder sb;
            boolean mark = true;
            while ((temp = br.readLine()) != null && temp.length() != 0) {
                temp1.add(temp);
                p = new Attribute(temp);
                if (p.key.equals(value)) {
                    temp1.remove(temp);
                    temp1.add(temp + Integer.toString(fileno) + ";");
                    mark = false;
                }
            }
            br.close();

            if (!mark) {
                File f = new File(indexpath.toString() + "/Index.txt");
                f.delete();
                File f1 = new File(indexpath.toString() + "/newfile.txt");
                bw = new BufferedWriter(new FileWriter(f1));
                Iterator<String> it = temp1.iterator();
                while (it.hasNext()) {
                    bw.write(it.next());
                    bw.newLine();
                }
                bw.close();
                File f2 = new File(indexpath.toString() + "/Index.txt");
                f1.renameTo(f2);
                c = temp1.size();
            }

            // UPDATING THE INDEX

            if (mark) {
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/Index.txt", true));
                bw.write(value + ":" + Integer.toString(fileno) + ";");
                bw.newLine();
                bw.close();
                c = temp1.size() + 1;
            }


            if (c > 62) {
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/status.txt"));
                bw.write("f");
                bw.close();
                addLevel(indexpath.toString());
                (new File(indexpath.toString() + "/Index.txt")).delete();
            }

        } else {
            if (Character.isUpperCase(first)) {
                new File(indexpath.toString() + "/$" + Character.toString(first)).mkdirs();

                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/$" + Character.toString(first) + "/Index.txt"));
                bw.write(value + ":" + Integer.toString(fileno) + ";");
                bw.newLine();
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/$" + Character.toString(first) + "/files.txt"));
                bw.write(Integer.toString(fileno) + ";");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/$" + Character.toString(first) + "/noe.txt"));
                bw.write("1");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/$" + Character.toString(first) + "/status.txt"));
                bw.write("nf");
                bw.close();
            } else {
                new File(indexpath.toString() + "/" + Character.toString(first)).mkdirs();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/" + Character.toString(first) + "/Index.txt"));
                bw.write(value + ":" + Integer.toString(fileno) + ";");
                bw.newLine();
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/" + Character.toString(first) + "/files.txt"));
                bw.write(Integer.toString(fileno) + ";");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/" + Character.toString(first) + "/noe.txt"));
                bw.write("1");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/" + Character.toString(first) + "/status.txt"));
                bw.write("nf");
                bw.close();
            }
        }
    }

    // FILLING A VACANCY

    private void add(Entry e, int no) throws IOException {
        Attribute p;
        Iterator<Attribute> it = e.data.iterator();
        BufferedWriter bw;

        // UPDATING NOE

        noet[no]++;

        bw = dh.getWriter(e.fileno, location);
        while (it.hasNext()) {
            p = it.next();
            bw.write(p.key + ":");
            for (String s : p.values) bw.write(s + ";");
            bw.newLine();
            if (keys.contains(p.key)) {
                for (String s : p.values) {
                    indexEntry(p.key, s, e.fileno, no);
                }
            }

            if (keymap.containsKey(p.key)) {
                for (String s : p.values) {
                    indexEntry(p.key, s, e.fileno, no, keymap.get(p.key));
                }
            }
        }
        bw.close();
    }

    // ADDING NEW LEVEL TO THE INDEX

    private void addLevel(String s) throws IOException {

        BufferedWriter bw;
        BufferedReader br;

        // CREATING INDEX FILES

        // NUMERICAL INDEX FILES

        // SHIFTING THE ENTRIES

        if ((new File(s + "/Index.txt")).exists()) {
            char first;
            int c;
            String temp, temp1;
            StringTokenizer st1, st2;
            st1 = new StringTokenizer(s, "/");
            st2 = new StringTokenizer(location, "/");
            c = st1.countTokens() - st2.countTokens() - 2;
            BufferedReader br1;
            br = new BufferedReader(new FileReader(s + "/Index.txt"));

            while ((temp = br.readLine()) != null && temp.length() != 0) {
                st1 = new StringTokenizer(temp, ":");
                temp1 = st1.nextToken();
                first = temp1.charAt(c);

                if (Character.isUpperCase(first)) {
                    if (!(new File(s + "/$" + Character.toString(first)).exists())) {
                        (new File(s + "/$" + Character.toString(first))).mkdirs();
                        bw = new BufferedWriter(new FileWriter(s + "/$" + Character.toString(first) + "/Index.txt"));
                        bw.close();
                        bw = new BufferedWriter(new FileWriter(s + "/$" + Character.toString(first) + "/files.txt"));
                        bw.close();
                        bw = new BufferedWriter(new FileWriter(s + "/$" + Character.toString(first) + "/noe.txt"));
                        bw.write("0");
                        bw.close();
                        bw = new BufferedWriter(new FileWriter(s + "/$" + Character.toString(first) + "/status.txt"));
                        bw.write("nf");
                        bw.close();
                    }
                } else {
                    if (!(new File(s + "/" + Character.toString(first)).exists())) {
                        (new File(s + "/" + Character.toString(first))).mkdirs();
                        bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/Index.txt"));
                        bw.close();
                        bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/files.txt"));
                        bw.close();
                        bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/noe.txt"));
                        bw.write("0");
                        bw.close();
                        bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/status.txt"));
                        bw.write("nf");
                        bw.close();
                    }
                }

                if (temp1.length() == c + 1) {
                    bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/blank.txt"));
                    bw.write(temp);
                    bw.newLine();
                    bw.close();
                    bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/files.txt", true));
                    bw.write(st1.nextToken());
                    bw.close();
                } else {
                    if (Character.isUpperCase(first))
                        bw = new BufferedWriter(new FileWriter(s + "/$" + Character.toString(first) + "/Index.txt", true));
                    else
                        bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/Index.txt", true));
                    bw.write(temp);
                    bw.newLine();
                    bw.close();

                    // UPDATING THE FILES STORE

                    if (Character.isUpperCase(first))
                        bw = new BufferedWriter(new FileWriter(s + "/$" + Character.toString(first) + "/files.txt", true));
                    else
                        bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/files.txt", true));
                    bw.write(st1.nextToken());
                    bw.close();

                    // UPDATING THE NUMBER OF ELEMENTS

                    int n;
                    if (Character.isUpperCase(first)) {
                        br1 = new BufferedReader(new FileReader(s + "/$" + Character.toString(first) + "/noe.txt"));
                        n = Integer.parseInt(br1.readLine());
                        br1.close();
                        n++;
                        bw = new BufferedWriter(new FileWriter(s + "/$" + Character.toString(first) + "/noe.txt"));
                    } else {
                        br1 = new BufferedReader(new FileReader(s + "/" + Character.toString(first) + "/noe.txt"));
                        n = Integer.parseInt(br1.readLine());
                        br1.close();
                        n++;
                        bw = new BufferedWriter(new FileWriter(s + "/" + Character.toString(first) + "/noe.txt"));
                    }

                    bw.write(Integer.toString(n));
                    bw.newLine();
                    bw.close();
                }
            }
            br.close();
        }
        (new File(s + "/Index.txt")).delete();
    }

    // FIND ENTRY

    private Resultset find(String key, String op, String value, int a) throws IOException {

        BufferedReader br;
        StringTokenizer st;
        Resultset rs = new Resultset(location, threadPool);
        StringBuilder indexpath = new StringBuilder(location + "/Indexes/" + key + Integer.toString(a));
        char[] valuebase;
        boolean found = false;

        //  EQUALS

        if (op.equals("=")) {

            if (keys.contains(key) || keymap.containsKey(key)) {

                valuebase = value.toCharArray();

                for (char first : valuebase) {

                    // IF FIRST CHARACTER IS CAPITAL

                    if (Character.isUpperCase(first) && (new File(indexpath + "/$" + Character.toString(first)).exists())) {
                        // UPDATING THE INDEXPATH

                        indexpath.append("/$").append(Character.toString(first));
                    } else if (new File(indexpath + "/" + Character.toString(first)).exists()) {
                        // UPDATING THE INDEXPATH

                        indexpath.append("/").append(Character.toString(first));
                    } else {

                        try {
                            br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                            String temp;
                            StringTokenizer st1;
                            while ((temp = br.readLine()) != null && temp.length() != 0) {
                                st1 = new StringTokenizer(temp, ":");
                                if (st1.nextToken().equals(value)) {
                                    rs.fileset.add(st1.nextToken());
                                    found = true;
                                }
                            }
                            br.close();
                            break;
                        } catch (FileNotFoundException e) {
                            break;
                        }
                    }
                }

                if (!found && (new File(indexpath + "/blank.txt")).exists()) {
                    br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                    String temp = br.readLine();
                    StringTokenizer st1 = new StringTokenizer(temp, ":");
                    if (st1.nextToken().equals(value)) {
                        rs.fileset.add(st1.nextToken());
                    }
                    br.close();
                }
            }


//            else{
//                fileFork.Entry e;
//                fileFork.Pair p;
//                boolean marker = false;
//                for(int i = 1 ; i <= noe ; i++){
//                    e = new fileFork.Entry(i, location);
//                    Iterator<fileFork.Pair> it = e.data.listIterator();
//                    while (it.hasNext()){
//                        p = it.next();
//                        if(p.key.equals(key)){
//                            for(int k = 0 ; k < p.values.length ; k++){
//                                if(p.values[k].equals(value)){
//                                    rs.addEntry(e);
//                                    marker = true;
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                }
//            }
        }

        // LESS THAN

        else if (op.equals("<")) {
            if (keys.contains(key) || keymap.containsKey(key)) {

                char[] valueset = value.toCharArray();

                for (char first : valueset) {

                    if (Character.isUpperCase(first)) {
                        for (char c = 'A'; c < first; c++) {

                            if ((new File(indexpath + "/$" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/$" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);

                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/$" + Character.toString(first))).exists()) {
                            indexpath.append("/$").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    } else if (Character.isLowerCase(first)) {
                        for (char c = 'a'; c < first; c++) {

                            if ((new File(indexpath + "/" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/" + Character.toString(first))).exists()) {
                            indexpath.append("/").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    } else if (Character.isDigit(first)) {
                        for (char c = '0'; c < first; c++) {

                            if ((new File(indexpath + "/" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/" + Character.toString(first))).exists()) {
                            indexpath.append("/").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    }
                }
            }
        }

//            else{
//                fileFork.Entry e;
//                fileFork.Pair p;
//                boolean marker = false;
//                for(int i = 1 ; i <= noe ; i++){
//                    e = new fileFork.Entry(i, location);
//                    Iterator<fileFork.Pair> it = e.data.listIterator();
//                    while (it.hasNext()){
//                        p = it.next();
//                        if(p.key.equals(key)){
//                            for(int k = 0 ; k < p.values.length ; k++){
//                                int val = p.values[k].compareTo(value);
//                                if(val < 0){
//                                    rs.addEntry(e);
//                                    marker = true;
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                }
//            }


        // GREATER THAN

        else if (op.equals(">")) {
            if (keys.contains(key) || keymap.containsKey(key)) {

                char[] valueset = value.toCharArray();

                for (char first : valueset) {

                    if (Character.isUpperCase(first)) {
                        for (char c = 'Z'; c > first; c--) {

                            if ((new File(indexpath + "/$" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/$" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/$" + Character.toString(first))).exists()) {
                            indexpath.append("/$").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    } else if (Character.isLowerCase(first)) {
                        for (char c = 'z'; c > first; c--) {

                            if ((new File(indexpath + "/" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/" + Character.toString(first))).exists()) {
                            indexpath.append("/").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    } else if (Character.isDigit(first)) {
                        for (char c = '9'; c > first; c--) {

                            if ((new File(indexpath + "/" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/" + Character.toString(first))).exists()) {
                            indexpath.append("/").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    }
                }
            }
        }

//            else{
//                fileFork.Entry e;
//                fileFork.Pair p;
//                boolean marker = false;
//                for(int i = 1 ; i <= noe ; i++){
//                    e = new fileFork.Entry(location + "/Data/File" + Integer.toString(i) + ".txt", location);
//                    Iterator<fileFork.Pair> it = e.data.listIterator();
//                    while (it.hasNext()){
//                        p = it.next();
//                        if(p.key.equals(key)){
//                            for(int k = 0 ; k < p.values.length ; k++){
//                                int val = p.values[k].compareTo(value);
//                                if(val > 0){
//                                    rs.addEntry(e);
//                                    marker = true;
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        GREATER THAN OR EQUAL TO

        else if (op.equals(">=")) {
            if (keys.contains(key) || keymap.containsKey(key)) {

                char[] valueset = value.toCharArray();

                for (char first : valueset) {

                    if (Character.isUpperCase(first)) {
                        for (char c = 'Z'; c > first; c--) {

                            if ((new File(indexpath + "/$" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/$" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/$" + Character.toString(first))).exists()) {
                            indexpath.append("/$").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    } else if (Character.isLowerCase(first)) {
                        for (char c = 'z'; c > first; c--) {

                            if ((new File(indexpath + "/" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/" + Character.toString(first))).exists()) {
                            indexpath.append("/").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    } else if (Character.isDigit(first)) {
                        for (char c = '9'; c > first; c--) {

                            if ((new File(indexpath + "/" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/" + Character.toString(first))).exists()) {
                            indexpath.append("/").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    }
                }
            }
        }

//            else{
//                fileFork.Entry e;
//                fileFork.Pair p;
//                boolean marker = false;
//                for(int i = 1 ; i <= noe ; i++){
//                    e = new fileFork.Entry(location + "/Data/File" + Integer.toString(i) + ".txt", location);
//                    Iterator<fileFork.Pair> it = e.data.listIterator();
//                    while (it.hasNext()){
//                        p = it.next();
//                        if(p.key.equals(key)){
//                            for(int k = 0 ; k < p.values.length ; k++){
//                                int val = p.values[k].compareTo(value);
//                                if(val >= 0){
//                                    rs.addEntry(e);
//                                    marker = true;
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//        LESS THAN OR EQUAL TO

        else if (op.equals("<=")) {
            if (keys.contains(key) || keymap.containsKey(key)) {

                char[] valueset = value.toCharArray();

                for (char first : valueset) {

                    if (Character.isUpperCase(first)) {
                        for (char c = 'A'; c < first; c++) {

                            if ((new File(indexpath + "/$" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/$" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/$" + Character.toString(first))).exists()) {
                            indexpath.append("/$").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    } else if (Character.isLowerCase(first)) {
                        for (char c = 'a'; c < first; c++) {

                            if ((new File(indexpath + "/" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/" + Character.toString(first))).exists()) {
                            indexpath.append("/").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    } else if (Character.isDigit(first)) {
                        for (char c = '0'; c < first; c++) {

                            if ((new File(indexpath + "/" + Character.toString(c))).exists()) {
                                br = new BufferedReader(new FileReader(indexpath + "/" + Character.toString(c) + "/files.txt"));
                                String s = br.readLine();
                                if (s != null)
                                    rs.fileset.add(s);
                                br.close();
                            }
                        }

                        if ((new File(indexpath + "/" + Character.toString(first))).exists()) {
                            indexpath.append("/").append(Character.toString(first));
                            if ((new File(indexpath + "/blank.txt").exists())) {
                                br = new BufferedReader(new FileReader(indexpath + "/blank.txt"));
                                st = new StringTokenizer(br.readLine(), ":");
                                st.nextToken();
                                rs.fileset.add(st.nextToken());
                                br.close();
                            }
                        } else {
                            try {
                                StringBuilder sb = new StringBuilder("");
                                String temp;
                                br = new BufferedReader(new FileReader(indexpath + "/Index.txt"));
                                while ((temp = br.readLine()) != null && temp.length() != 0) {
                                    st = new StringTokenizer(temp, ":");
                                    temp = st.nextToken();
                                    if (temp.compareTo(value) < 0) {
                                        sb.append(st.nextToken());
                                    }
                                }
                                if (sb.length() != 0)
                                    rs.fileset.add(sb.toString());

                                br.close();
                            } catch (FileNotFoundException e) {
                                break;
                            }
                        }
                    }
                }
            }
        }

//            else{
//                fileFork.Entry e;
//                fileFork.Pair p;
//                boolean marker = false;
//                for(int i = 1 ; i <= noe ; i++){
//                    e = new fileFork.Entry(location + "/Data/File" + Integer.toString(i) + ".txt", location);
//                    Iterator<fileFork.Pair> it = e.data.listIterator();
//                    while (it.hasNext()){
//                        p = it.next();
//                        if(p.key.equals(key)){
//                            for(int k = 0 ; k < p.values.length ; k++){
//                                int val = p.values[k].compareTo(value);
//                                if(val <= 0){
//                                    rs.addEntry(e);
//                                    marker = true;
//                                    break;
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
        return rs;
    }

//    ADDING A NEW INDEX

    public final void addIndex(String Attribute) throws IOException {

        BufferedWriter bw;

        // LISTOFINDEXES FILE

        bw = new BufferedWriter(new FileWriter(location + "/" + "Indexes" + "/" + "ListOfIndexes.txt", true));
        bw.write(Attribute + ":" + "ALPHA");
        bw.newLine();
        bw.close();
        keys.add(Attribute);

//        ARRANGING FILES ACCORDING TO THE INDEX

        Entry e;
        Attribute p;
        for (int i = 1; i <= noe; i++) {
            e = new Entry(i, location);
            Iterator<Attribute> it = e.data.listIterator();
            while (it.hasNext()) {
                p = it.next();
                if (p.key.equals(Attribute)) {

                    for (String s : p.values) {
                        js.makeIndex(Attribute, s, i);
                    }
                }
            }
        }
    }

    // ADD NEW NUMERIC INDEX

    public final void addIndex(String NumericAttribute, int MaxDigits) throws IOException {

        BufferedWriter bw;

        // LISTOFINDEXES FILE

        bw = new BufferedWriter(new FileWriter(location + "/" + "Indexes" + "/" + "ListOfIndexes.txt", true));
        bw.write(NumericAttribute + ":" + Integer.toString(MaxDigits));
        bw.newLine();
        bw.close();
        keymap.put(NumericAttribute, MaxDigits);

//        ARRANGING FILES ACCORDING TO THE INDEX

        Entry e;
        Attribute p;
        for (int i = 1; i <= noe; i++) {
            e = new Entry(i, location);
            Iterator<Attribute> it = e.data.listIterator();
            while (it.hasNext()) {
                p = it.next();
                if (p.key.equals(NumericAttribute)) {

                    for (String s : p.values) {
                        js.makeIndex(NumericAttribute, s, i, MaxDigits);
                    }
                }
            }
        }
    }

    private class NumericIndexer implements Callable<Integer> {

        String key;
        String value;
        int fno;
        int tno;
        int degree;

        @Override
        public Integer call() throws Exception {
            try {
                indexEntry(key, value, fno, tno, degree);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return tno;
        }


        public NumericIndexer(String key, String value, int turn, int fno, int degree) {
            this.key = key;
            this.value = value;
            this.tno = turn;
            this.fno = fno;
            this.degree = degree;
        }
    }

    private void indexEntry(String key, String value, int fno, int tno, int degree) throws IOException {
        StringBuffer indexpath;
        BufferedReader br;
        int n;
        char[] valuebase;
        String temp, newValue;
        char first;
        BufferedWriter bw = null;
        newValue = String.format("%" + Integer.toString(degree) + "s", value).replace(" ", "0");
        indexpath = new StringBuffer(location + "/Indexes/" + key + Integer.toString(tno));
        valuebase = newValue.toCharArray();
        char ch = newValue.charAt(0);
        int j = 1;
        if (Character.isUpperCase(ch)) {
            if (!(new File(indexpath + "/$" + Character.toString(ch))).exists()) {
                indexpath.append("/$").append(Character.toString(ch));
                new File(indexpath.toString()).mkdirs();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/Index.txt"));
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/files.txt"));
                bw.write(Integer.toString(fno) + ";");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/noe.txt"));
                bw.write("1");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/status.txt"));
                bw.write("nf");
                bw.close();
            } else j = 0;
        } else {
            if (!(new File(indexpath + "/" + Character.toString(ch))).exists()) {
                indexpath.append("/").append(Character.toString(ch));
                new File(indexpath.toString()).mkdirs();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/Index.txt"));
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/files.txt"));
                bw.write(Integer.toString(fno) + ";");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/noe.txt"));
                bw.write("1");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/status.txt"));
                bw.write("nf");
                bw.close();
            } else j = 0;
        }
        first = ch;
        for (; j < valuebase.length; j++) {

            first = valuebase[j];

            // IF FIRST CHARACTER IS CAPITAL

            if (Character.isUpperCase(first) && (new File(indexpath + "/$" + Character.toString(first)).exists())) {

                // UPDATING THE INDEXPATH

                indexpath.append("/$").append(Character.toString(first));
            } else if (new File(indexpath + "/" + Character.toString(first)).exists()) {

                // UPDATING THE INDEXPATH

                indexpath.append("/").append(Character.toString(first));
            } else break;

            // UPDATING THE FILES STORE

            bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/files.txt", true));
            bw.write(Integer.toString(fno) + ";");
            bw.close();

            // UPDATING THE NUMBER OF ELEMENTS

            br = new BufferedReader(new FileReader(indexpath.toString() + "/noe.txt"));
            n = Integer.parseInt(br.readLine());
            br.close();
            n++;
            bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/noe.txt"));
            bw.write(Integer.toString(n));
            bw.close();
        }

        br = new BufferedReader(new FileReader(indexpath.toString() + "/status.txt"));
        temp = br.readLine();
        br.close();

        if (temp.equals("nf")) {

            // CHECKING FOR REPEATED VALUES

            br = new BufferedReader(new FileReader(indexpath.toString() + "/Index.txt"));
            int c;
            ArrayList<String> temp1 = new ArrayList<String>();
            Attribute p;
            c = 0;
            StringBuilder sb;
            boolean mark = true;
            while ((temp = br.readLine()) != null && temp.length() != 0) {
                temp1.add(temp);
                p = new Attribute(temp);
                if (p.key.equals(newValue)) {
                    temp1.remove(temp);
                    temp1.add(temp + Integer.toString(fno) + ";");
                    mark = false;
                }
            }
            br.close();

            if (!mark) {
                File f = new File(indexpath.toString() + "/Index.txt");
                f.delete();
                File f1 = new File(indexpath.toString() + "/newfile.txt");
                bw = new BufferedWriter(new FileWriter(f1));
                Iterator<String> it = temp1.iterator();
                while (it.hasNext()) {
                    bw.write(it.next());
                    bw.newLine();
                }
                bw.close();
                File f2 = new File(indexpath.toString() + "/Index.txt");
                f1.renameTo(f2);
                c = temp1.size();
            }

            // UPDATING THE INDEX

            if (mark) {
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/Index.txt", true));
                bw.write(newValue + ":" + Integer.toString(fno) + ";");
                bw.newLine();
                bw.close();
                c = temp1.size() + 1;
            }


            if (c > 62) {
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/status.txt"));
                bw.write("f");
                bw.close();
                addLevel(indexpath.toString());
                (new File(indexpath.toString() + "/Index.txt")).delete();
            }

        } else {
            if (Character.isUpperCase(first)) {
                new File(indexpath.toString() + "/$" + Character.toString(first)).mkdirs();

                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/$" + Character.toString(first) + "/Index.txt"));
                bw.write(newValue + ":" + Integer.toString(fno) + ";");
                bw.newLine();
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/$" + Character.toString(first) + "/files.txt"));
                bw.write(Integer.toString(fno) + ";");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/$" + Character.toString(first) + "/noe.txt"));
                bw.write("1");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/$" + Character.toString(first) + "/status.txt"));
                bw.write("nf");
                bw.close();
            } else {
                new File(indexpath.toString() + "/" + Character.toString(first)).mkdirs();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/" + Character.toString(first) + "/Index.txt"));
                bw.write(newValue + ":" + Integer.toString(fno) + ";");
                bw.newLine();
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/" + Character.toString(first) + "/files.txt"));
                bw.write(Integer.toString(fno) + ";");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/" + Character.toString(first) + "/noe.txt"));
                bw.write("1");
                bw.close();
                bw = new BufferedWriter(new FileWriter(indexpath.toString() + "/" + Character.toString(first) + "/status.txt"));
                bw.write("nf");
                bw.close();
            }
        }
    }

    // REMOVING AN EXISTING INDEX

//    void removeIndex(String index) throws IOException{
//
//        String filename;
//        File f;
//
//        REMOVING FROM LISTOFINDEXES
//
//        keys.remove(index);
//        BufferedWriter bw = new BufferedWriter(new FileWriter(location + "/Indexes/ListOfIndexes.txt"));
//        Iterator<String> it = keys.listIterator();
//        while(it.hasNext()){
//            bw.write(it.next());
//            bw.newLine();
//        }
//        bw.close();
//
//        DELETING INDEX FILES
//
//        for(int i = 0 ; i <= 9 ; i++){
//            f = new File(location + "/Indexes/" + index + "/" + Integer.toString(i) + "Index.txt");
//            f.delete();
//        }
//
//        for(char i = 'a' ; i <= 'z' ; i++){
//            f = new File(location + "/Indexes/" + index + "/" + Character.toString(i) + "Index.txt");
//            f.delete();
//        }

//        for(char i = 'a' ; i <= 'z' ; i++){
//            f = new File(location + "/Indexes/" + index + "/" + "cap" + Character.toString(i) + "Index.txt");
//            f.delete();
//        }
//
    // DELETING THE DIRECTORY

//        new File(location + "/Indexes/" + index).delete();
//    }
}
