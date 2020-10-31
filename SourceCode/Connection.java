/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SourceCode;

import com.mongodb.Mongo;
import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import javax.swing.JOptionPane;
import java.sql.*;
//import com.mysql.jdbc.Statement;

/**
 *
 * @author bcktrack
 */
public class Connection {

    public Mongo getMongoConnection(String ip) {
        Mongo mongo = null;
        try {
            mongo = new Mongo(ip, 27017);

        } catch (UnknownHostException ex) {
            JOptionPane.showInternalMessageDialog(null, " Mongo Connectivity Error ...!! ");
        }
        return mongo;
    }

    public Statement getMySqlConnection(String ip,String userName,String password) {
        Connection conn = null;
        Statement stmt = null;
        try {
            String JDBC_DRIVER = "com.mysql.jdbc.Driver";
            String DB_URL = "jdbc:mysql://"+ ip +"/";
            conn = (Connection) DriverManager.getConnection(DB_URL, userName, password);
            //stmt = (Statement)conn.createStatement;
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "MySql Connectivity Error ...!!");
        }
        return stmt;
    }

    Statement createStatement() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    DatabaseMetaData getMetaData() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
