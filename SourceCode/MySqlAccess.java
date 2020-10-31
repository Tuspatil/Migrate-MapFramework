/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SourceCode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import javax.swing.JOptionPane;

/**
 *
 * @author bcktrack
 */
public class MySqlAccess extends MongoAccess {

    public static Statement stmt = null;
    public static String MySqlDb = null;

    public void MySqlConnect(String ip, String userName, String password, String DbName) {
        Connection connect = new Connection();
        stmt = connect.getMySqlConnection(ip, userName, password);
        MySqlDb = DbName;
    }

    public int MySqlQueryChecker(String select, String from, String where) {
        String sel = "use " + MySqlDb;
        try {
            stmt.execute(sel);
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "MySql Connectivity Error ...!!");
        }
        String s = "select " + select + " from " + from + " where " + where+" ;";
        try {
            stmt.executeQuery(s);
            return 1;
        } catch (SQLException ex) {
            return 0;
        }
    }

    public ArrayList MySqlMapper(String select, String from, String where) {
            ArrayList<String> arr = new ArrayList<>();
        try {
            String sel = "use " + MySqlDb;
            stmt.execute(sel);
            String query = "select " + select + " from " + from + " where " + where+" ;";
            ResultSet rs = stmt.executeQuery(query);
            
            System.out.println("Id\t Name");
            while (rs.next()) {
                
                int id = rs.getInt("id");
                String name = rs.getString("name");
                
                System.out.print(" " + id);
                System.out.print("\t");
                System.out.println(" " + name);
                
            }
        } catch (SQLException ex) {
            JOptionPane.showMessageDialog(null, "MySql Connectivity Error ...!!");
        }
        return arr;
    }

}
