/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SourceCode;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 *
 * @author bcktrack
 */
public class MongoAccess extends Connection {
    mapFrame mf = new mapFrame();
    String select;
    String table;
    String cond="";
    public static Mongo mongo = null;
    public static DB db = null;
    public static DBCollection collection = null;
    public static DBCursor cursorDoc = null;

    public void MongoConnect(String ip, String dbName, String collectionName) {
        Connection connect = new Connection();
        mongo = connect.getMongoConnection(ip);
        db = mongo.getDB(dbName);
        collection = db.getCollection(collectionName);
    }
        public static boolean isInteger(String s) {
    try { 
        Integer.parseInt(s); 
    } catch(NumberFormatException e) { 
        return false; 
    } catch(NullPointerException e) {
        return false;
    }
    // only got here if we didn't return false
    return true;
}
    public ArrayList MongoMapper(String select, String from, String where) {

        StringTokenizer tokens = null;
        ArrayList<String> arr = new ArrayList<>();
        ArrayList<String> result = new ArrayList<>();
        System.out.println("Im in MongoAccess");
        int checker = 0;
        if (where.toLowerCase().contains("=")) {
            System.out.println("Im in Equals");
            if (where.toLowerCase().contains("!=")) {

                tokens = new StringTokenizer(where, "!=");

                while (tokens.hasMoreElements()) {
                    arr.add(tokens.nextToken());
                }

                System.out.println(arr.get(0));
                System.out.flush();

                arr.set(1, arr.get(1).replace("\"", ""));
                System.out.println(arr.get(1));
                System.out.flush();

                checker = 5;
            } else {
                if (where.toLowerCase().contains("<=")) {
                    System.out.println("Im in LTE");
                    tokens = new StringTokenizer(where, "<=");

                    while (tokens.hasMoreElements()) {
                        arr.add(tokens.nextToken());
                    }
                    //arr.get(0)=arr.get(0).replace("!","");
                    System.out.println(arr.get(0));
                    System.out.flush();

                    arr.set(1, arr.get(1).replace("\"", ""));
                    System.out.println(arr.get(1));
                    System.out.flush();

                    checker = 7;
                } else {
                    if (where.toLowerCase().contains(">=")) {
                        System.out.println("Im in GTE");
                        tokens = new StringTokenizer(where, ">=");

                        while (tokens.hasMoreElements()) {
                            arr.add(tokens.nextToken());
                        }
                        //arr.get(0)=arr.get(0).replace("!","");
                        System.out.println(arr.get(0));
                        System.out.flush();

                        arr.set(1, arr.get(1).replace("\"", ""));
                        System.out.println(arr.get(1));
                        System.out.flush();

                        checker = 6;
                    } else {
                        if (where.toLowerCase().contains("=")) {
                            System.out.println("Im in equals below");
                            tokens = new StringTokenizer(where, "=");

                            while (tokens.hasMoreElements()) {
                                arr.add(tokens.nextToken());
                            }

                            System.out.println(arr.get(0));
                            System.out.flush();

                            arr.set(1, arr.get(1).replace("\"", ""));
                            System.out.println(arr.get(1));
                            System.out.flush();

                            checker = 2;
                        }

                    }
                }

            }
        } else {

            if (where.toLowerCase().contains(">")) {

                tokens = new StringTokenizer(where, ">");

                while (tokens.hasMoreElements()) {
                    arr.add(tokens.nextToken());
                }
                System.out.println(arr.get(0));
                System.out.flush();

                arr.set(1, arr.get(1).replace("\"", ""));
                System.out.println(arr.get(1));
                System.out.flush();

                checker = 3;
            }

            if (where.toLowerCase().contains("<")) {

                tokens = new StringTokenizer(where, "<");

                while (tokens.hasMoreElements()) {
                    arr.add(tokens.nextToken());
                }

                System.out.println(arr.get(0));
                System.out.flush();

                arr.set(1, arr.get(1).replace("\"", ""));
                System.out.println(arr.get(1));
                System.out.flush();

                checker = 4;
            }
        }

        switch (checker) {

            case 2: {
                System.out.println("im in case 2 integer");
                boolean isint = isInteger(arr.get(1));
                if (isint == false) {
                    BasicDBObject whereQuery = new BasicDBObject();

                    whereQuery.put(arr.get(0), arr.get(1));
                    cursorDoc = collection.find(whereQuery);
                } else {
                    Integer number = new Integer(arr.get(1));
                    BasicDBObject whereQuery = new BasicDBObject();
                    whereQuery.put(arr.get(0), number);
                    cursorDoc = collection.find(whereQuery);
                }
            }
            break;

            case 3:
                System.out.println("Im in case 3");
                float gt = (float) Integer.parseInt(arr.get(1));
                System.out.println(gt);
                System.out.flush();
                BasicDBObject gtQuery = new BasicDBObject();
                gtQuery.put("rollno", new BasicDBObject("$gt", gt));
                cursorDoc = collection.find(gtQuery);
                break;

            case 4:
                System.out.println("Im in case 3");
                float lt = (float) Integer.parseInt(arr.get(1));
                System.out.println("TheHacktivist" + lt);
                System.out.flush();
                BasicDBObject ltQuery = new BasicDBObject();
                ltQuery.put("rollno", new BasicDBObject("$lt", lt));
                cursorDoc = collection.find(ltQuery);
                break;

            case 5:
                System.out.println("Im in case 3");
                float ne = (float) Integer.parseInt(arr.get(1));
                System.out.println("TheHacktivist" + ne);
                System.out.flush();
                BasicDBObject neQuery = new BasicDBObject();
                neQuery.put("rollno", new BasicDBObject("$ne", ne));
                cursorDoc = collection.find(neQuery);
                break;

            case 6:
                System.out.println("im in case 6 integer");
                float gte = (float) Integer.parseInt(arr.get(1));
                System.out.println("TheHacktivist" + gte);
                System.out.flush();
                BasicDBObject gteQuery = new BasicDBObject();
                gteQuery.put("rollno", new BasicDBObject("$gte", gte));
                cursorDoc = collection.find(gteQuery);
                break;

            case 7:
                System.out.println("im in case 6 integer");
                float lte = (float) Integer.parseInt(arr.get(1));
                System.out.println("TheHacktivist" + lte);
                System.out.flush();
                BasicDBObject lteQuery = new BasicDBObject();
                lteQuery.put("rollno", new BasicDBObject("$lte", lte));
                cursorDoc = collection.find(lteQuery);
                break;

            default:
                cursorDoc = collection.find();
                break;
        }

        while (cursorDoc.hasNext()) {
            System.out.println(cursorDoc.next());
            //displayBox.setText(cursorDoc.next().toString());
            System.out.println(" ");
        }

        return result;
    }
}
