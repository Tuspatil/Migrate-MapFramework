/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SourceCode;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author hacktivist
 */
public class Hbasemig {
  

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args){
        // TODO code application logic here
          int count = 0;
        try {

    
    
           
           
                            Configuration config = HBaseConfiguration.create();
                            HBaseAdmin admin = new HBaseAdmin(config);
                            String var = "testdata";
                            //System.out.println(""+var);
                            HTableDescriptor desc = new HTableDescriptor(var);
                            HColumnDescriptor name = new HColumnDescriptor("testtable");
                            desc.addFamily(name);
                            admin.createTable(desc);
                            HTable hTable = new HTable(config, var);
                            System.out.println("table created");

                            ////////////////////////////////////////////
                            System.out.println("file line");
                            String tabn="bigdata";
                            String strFile = "/home/ankitpagar/Documents/dataSet1.csv";
                            ArrayList<String> keys = new ArrayList<>();
                            ArrayList<String> values = new ArrayList<>();

                            BufferedReader br = new BufferedReader(new FileReader(strFile));
                            String strLine = "";
                            StringTokenizer st = null;
                            int lineNumber = 0, tokenNumber = 0;
                            int flag = 0;
                            while ((strLine = br.readLine()) != null) {

                                lineNumber++;
                                st = new StringTokenizer(strLine, ",");
                                while (st.hasMoreTokens()) {
                                    if (lineNumber == 1) {
                                        keys.add(st.nextToken());
                                    } else {
                                        flag = 1;
                                        values.add(st.nextToken());
                                    }//if else
                                }//while

                                if (flag == 1) {
                                    Iterator valIt = values.iterator();
                                    Iterator keyIt = keys.iterator();
                                    Put p = new Put(Bytes.toBytes(valIt.next().toString()));
                                    keyIt.next();
                                    while (keyIt.hasNext()) {
                                        p.add(Bytes.toBytes("testtable"), Bytes.toBytes(keyIt.next().toString()), Bytes.toBytes(valIt.next().toString()));
                                    }//while
                                    hTable.put(p);
                                }//if
                                values.clear();
                            }//while

                            hTable.close();

                            //////////////////////////////////////////
                            count = count + 1;
                            System.out.println("count:"+count);
              //          }// for loop ends cut it
                        

                    /////}

//        }
            //    }//if ends cut it
            //}//while ends cut it
            }catch (Exception e) {

            e.printStackTrace();
        }

            //hTable.close();
        }

    }