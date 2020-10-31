package SourceCode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//package SourceCode;

/**
 *
 * @author ankitpagar
 */
public class Javasql {
     static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost/";

   static final String USER = "root";
   static final String PASS = "ubuntu1404";




    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
  
        // TODO code application logic here
   
 Connection conn = null;
   Statement stmt = null;
   try{
      Class.forName("com.mysql.jdbc.Driver");

      System.out.println("Connecting to database...");
      conn = DriverManager.getConnection(DB_URL, USER, PASS);

      stmt = conn.createStatement();
      
      
      
      
      
      String database="num";
      String sel="use "+database;
      stmt.execute(sel);
      ArrayList list=new ArrayList();
      ArrayList list1=new ArrayList();
       //String query="select tab1.id, tab2.name from tab1 inner join tab2 on tab1.id=tab2.id1;";
      //ResultSet rs=stmt.executeQuery(query);
      DatabaseMetaData metad=conn.getMetaData();
      ResultSet rs=metad.getCatalogs();
      while(rs.next()){
          //System.out.println(""+rs.getString(1));
          String dbase=rs.getString(1);
          list.add(dbase);
      
      }
      
      for(int i=0;i!=list.size();i++){
       System.out.println(""+list.get(i));

      }
      int count=0;
      for(int i=2;i!=4;i++){
          
          String q="use "+list.get(i);
          stmt.executeQuery(q);
          String q1="show tables";
          ResultSet res=stmt.executeQuery(q1);
          while(res.next()){
              
              String tables=res.getString(1);
              list1.add(tables);
          }
          
          System.out.println("*************");
          
          for(int j=0;j!=list1.size();j++){
              
              
              System.out.println(""+list1.get(j));
              //mysql dump and mongo import here............
              String query2="select * from "+list1.get(j)+" into outfile "
                      + "'/tmp/pgmdata"+count+".csv' fields terminated by ',' lines terminated by '\\r\\n';";
              stmt.executeQuery(query2);
              count=count+1;
          }   //mongo import stmt here 
          list1.clear();
      }
      
     
   }catch(Exception e){
      //Handle errors for JDBC
      e.printStackTrace();   
   }
    
    }
    
}
