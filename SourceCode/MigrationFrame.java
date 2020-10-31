/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SourceCode;
import java.sql.Connection;
import static SourceCode.MySqlAccess.stmt;
import java.sql.*;
import java.util.ArrayList;
/**
 *
 * @author ankitpagar
 */
public class MigrationFrame extends javax.swing.JFrame {
    
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost/";

   static final String USER = "root";
   static final String PASS = "ubuntu1404";
    public MigrationFrame() {
        Connection conn = null;
   Statement stmt = null;
       
        initComponents();
        
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        barMySqlToJava = new javax.swing.JProgressBar();
        barJavaToMongo = new javax.swing.JProgressBar();
        jButton1 = new javax.swing.JButton();
        jLabel1 = new javax.swing.JLabel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setMaximumSize(new java.awt.Dimension(1366, 658));
        setMinimumSize(new java.awt.Dimension(1300, 658));
        setResizable(false);
        getContentPane().setLayout(null);
        getContentPane().add(barMySqlToJava);
        barMySqlToJava.setBounds(290, 300, 270, 20);
        getContentPane().add(barJavaToMongo);
        barJavaToMongo.setBounds(730, 300, 260, 20);

        jButton1.setText("Start Transfer");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });
        getContentPane().add(jButton1);
        jButton1.setBounds(570, 440, 150, 29);

        jLabel1.setIcon(new javax.swing.ImageIcon("/home/ankitpagar/NetBeansProjects/BE_project_UI/src/UI/pics/migrationScreen.jpg")); // NOI18N
        getContentPane().add(jLabel1);
        jLabel1.setBounds(0, 0, 1530, 650);

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
         try{
      Class.forName("com.mysql.jdbc.Driver");

      System.out.println("Connecting to database...");
       Connection conn = (Connection) DriverManager.getConnection(DB_URL, USER, PASS);

      stmt = conn.createStatement();
      
      
      
      
      
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
      for(int i=0;i!=list.size();i++){
          
          String q="use "+list.get(i);
          stmt.executeQuery(q);
          String q1="show tables";
          ResultSet res=stmt.executeQuery(q1);
          while(res.next()){
              
              String tables=res.getString(1);
              list1.add(tables);
          }
          
          System.out.println("*************");
          barMySqlToJava.setValue(100);
          barMySqlToJava.setIndeterminate(true);
          for(int j=0;j!=list1.size();j++){
              
              
              //System.out.println(""+list1.get(j));
              //String s="echo \"select * from "+list1.get(j)+" \" | mysql "+list.get(i)+" -B -uroot -pubuntu1404 | sed s/\\\\t/,/g > /tmp/pgmdata"+count+".csv";
              //String[] command={"xterm","-e",s}; 
              //Runtime.getRuntime().exec(command);
              
              //mongo import
              String command1="mongoimport --host=127.0.0.1 -d "+list.get(i)+" -c "+list1.get(j)+" --type csv --file /tmp/pgmdata"+count+".csv --headerline";
              String[] com={"xterm","-e",command1};
              Runtime.getRuntime().exec(com);
              
              
              
              count=count+1;
          }   //mongo import stmt here 
          barJavaToMongo.setValue(100);
          barJavaToMongo.setIndeterminate(true);
          list1.clear();
      }
      
     
   }catch(Exception e){
      //Handle errors for JDBC
      e.printStackTrace();   
   }
           
           
    }//GEN-LAST:event_jButton1ActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
   
   
    
       
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new MigrationFrame().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JProgressBar barJavaToMongo;
    private javax.swing.JProgressBar barMySqlToJava;
    private javax.swing.JButton jButton1;
    private javax.swing.JLabel jLabel1;
    // End of variables declaration//GEN-END:variables
}