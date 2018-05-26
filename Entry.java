package hog;

import java.io.*;
import java.util.ArrayList;

public class Entry{
    protected ArrayList<Attribute> data;
    protected int fileno;
    private String location;
    private DataHandling dh;

    // CONSTRUCTOR

    protected Entry(int file, String s) throws IOException {
        data = new ArrayList<Attribute>();
        dh = new DataHandling();
        location = s;
        BufferedReader br = dh.getReader(file, location);
        fileno = file;
        String temp;
        Attribute p;
        while((temp = br.readLine())!= null && temp.length()!= 0){
            p = new Attribute(temp);
            data.add(p);
        }
        br.close();
    }

    // CONSTRUCTOR

    protected Entry(Entry e){
        data = new ArrayList<Attribute>();
        data = e.data;
        location = e.location;
        fileno = e.fileno;
    }

    // CONSTRUCTOR

    protected Entry(String location, int file){
        fileno = file;
        this.location = location;
        data = new ArrayList<Attribute>();
    }

    public final void addAttribute(String Name, String... Values){
        data.add(new Attribute(Name, Values));
    }

    protected void commit(boolean b) throws IOException{

        ArrayList<Attribute> data1;
        if(b){
            BufferedWriter bw = dh.getWriter(fileno, location);
            Attribute p;
            StringBuffer line;
            for (Attribute aData : data) {
                p = aData;
                line = new StringBuffer("");
                line.append(p.key).append(":");
                for (int i = 0; i < p.values.length; i++) {
                    line.append(p.values[i]);
                    if (i == p.values.length)
                        break;
                    line.append(";");
                }
                bw.write(line.toString());
                bw.newLine();
            }
            bw.close();
        }

        else{
            data1 = new ArrayList<Attribute>();
            BufferedReader br = dh.getReader(fileno, location);
            String temp;
            Attribute p;
            while((temp = br.readLine())!= null && temp.length()!= 0){
                p = new Attribute(temp);
                data1.add(p);
            }
            data = data1;
            br.close();
        }
    }

    protected void show(){
        Attribute p;
        System.out.println("---------------------------------------");
        System.out.println("KEY\t:\tVALUE(S)");
        for (Attribute aData : data) {
            p = aData;
            p.show();
        }
        System.out.println();
        System.out.println("---------------------------------------");
    }

}
