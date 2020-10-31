/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SourceCode;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import static java.lang.Double.sum;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author bcktrack
 */
public class DbSelector {

    int mySqlSelected = 100;
    int mongoSelected = 200;
    int hbaseSelected = 300;

    static JSONObject jsonObj1, jsonObj2;
    static JSONParser parser;
    static Object obj1, obj2;
    static JSONArray jarr;
    static Iterator iterator;
    static JSONArray list;
    static FileWriter file1;
    //static FileWriter file2;
    static double mongoRetrieval = 1;
    static double hbaseMean = 1;
    static double mongoMean = 1;
    static String query = "";
    static Double SUM = 0.0; // previous sum
    static Double NUM = 1.0; // no. of times query executed 

    //Constructor
    public DbSelector() {
        System.out.println("in dbselector");
        //JSON handling
        parser = new JSONParser();
        try {
            obj1 = parser.parse(new FileReader(".mongoSelectorData.json"));
            obj2 = parser.parse(new FileReader(".hbaseSelectorData.json"));

            //file1 = new FileWriter(".mongoSelectorData.json");
            //file2 = new FileWriter(".hbaseSelectorData.json");
            
        } catch (IOException ex) {
            Logger.getLogger(DbSelector.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(DbSelector.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public boolean decide(int QueryType) throws IOException {

        long decisionStart = System.currentTimeMillis();
        switch (QueryType) {
            case 10:
                query = "starTable";
                break;
            case 11:
                query = "colTable";
                break;
            case 1021:
                query = "star=";
                break;
            case 1121:
                query = "col=";
                break;
            case 103:
                query = "star>";
                break;
            case 113:
                query = "col>";
                break;
            case 106:
                query = "star>=";
                break;
            case 116:
                query = "col>=";
                break;
            case 104:
                query = "star<";
                break;
            case 114:
                query = "col<";
                break;
            case 107:
                query = "star<=";
                break;
            case 117:
                query = "col<=";
                break;
            case 109:
                query = "starAnd";
                break;
            case 119:
                query = "colAnd";
                break;
            case 108:
                query = "starOr";
                break;
            case 118:
                query = "colOr";
                break;
            case 121:
                query = "avg";
                break;
            case 122:
                query = "avgGrpby";
                break;
            case 123:
                query = "max";
                break;
            case 124:
                query = "maxGrpby";
                break;
            case 125:
                query = "min";
                break;
            case 126:
                query = "minGrpby";
                break;
            case 127:
                query = "count";
                break;
            case 128:
                query = "countGrpby";
                break;
        }
        System.out.println("after case in decision");
        mongoMean = 0.0;
        hbaseMean = 0.0;
        SUM = 0.0;

        jsonObj1 = (JSONObject) obj1;
        jarr = (JSONArray) jsonObj1.get(query);
        iterator = jarr.iterator();
        System.out.println("before SUM iterator");
        
        SUM = Double.parseDouble(iterator.next().toString());
        System.out.println("before SUM iterator");
        NUM = Double.parseDouble(iterator.next().toString());
        mongoMean = SUM / NUM;

        //jarr.clear();

        jsonObj2 = (JSONObject) obj2;
        jarr = (JSONArray) jsonObj2.get(query);
        iterator = jarr.iterator();

        SUM = Double.parseDouble(iterator.next().toString());
        NUM = Double.parseDouble(iterator.next().toString());
        hbaseMean = SUM / NUM;

        long decisionEnd = System.currentTimeMillis();

        System.out.println("Decision made in " + (decisionEnd - decisionStart) + " ms");
        if (mongoMean <= hbaseMean) {
             System.out.println("mongo selected");
            return true;
           
            //Thread mongoThread = new Thread(new MongoExecuteQuery());
        } else {
             System.out.println("hbase selected");
            return false;
            
            //Thread hbaseThread = new Thread(new HbaseExecuteQuery());
        }
        
    }

    public void selectorFileWrite() throws IOException {
        System.out.println("in selector write");
        SUM += SUM + mongoRetrieval;
        ++NUM;
        list = new JSONArray();
        list.add(SUM.toString());
        list.add(NUM.toString());
        jsonObj1.put(query, list);
        //file1.write(jsonObj1.toJSONString());
        //file1.flush();
        //file1.close();
        //file2.write(jsonObj2.toJSONString());
        //file2.flush();
        //file2.close();
    }

    public boolean checkJoin(String tables) {
        System.out.println("in check join");
        StringTokenizer numTables = new StringTokenizer(tables, ",");
        System.out.println("in check join tokenizet");
        if (numTables.countTokens() > 1) {
            return true; // means run on MySQL
        } else {
            return false; // run on mongo or hbase
        }
    }

    public void runOnMySQL() {

    }

}

class MongoExecuteQuery implements Runnable {

    @Override
    public void run() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}

class HbaseExecuteQuery implements Runnable {

    @Override
    public void run() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
