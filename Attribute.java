package hog;

import java.util.StringTokenizer;

/**
 * Created by Siddharth on 1/4/2016.
 */

// PAIR CLASS

public class Attribute {
    final protected String key;
    protected String[] values;

    // CONSTRUCTOR

    protected Attribute(String s){
        int count;
        StringTokenizer st = new StringTokenizer(s, ":");
        key = st.nextToken();
        StringTokenizer st1 = new StringTokenizer(st.nextToken(), ";");
        count = st1.countTokens();
        values = new String[count];
        for(int i = 0 ; i < count ; i++){
            values[i] = st1.nextToken();
        }
    }

    // CONSTRUCTOR

    protected Attribute(String arkey, String[] arvalues){
        key = arkey;
        values = new String[arvalues.length];
        System.arraycopy(arvalues, 0, values, 0, arvalues.length);
    }

    protected void show(){
        System.out.print(key + "\t:\t");
        for(int i = 0 ; i < values.length ;){
            System.out.print(values[i]);
            i++;
            if(i == values.length)
                break;
            System.out.print(";");
        }
        System.out.println();
    }
}
