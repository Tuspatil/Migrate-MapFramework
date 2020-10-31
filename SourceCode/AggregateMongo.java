/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package SourceCode;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;

public class AggregateMongo {
 public static void main(String args[]) throws UnknownHostException {
 MongoClient mongo = new MongoClient();
 DB db = mongo.getDB("mongoagg");

 DBCollection coll = db.getCollection("mycol1");

 /*
 MONGO SHELL : db.aggregationExample.aggregate(
 {$match : {type : "local"}} ,
 {$project : { department : 1 , amount : 1 }} ,
 {$group : {_id : "$department" , average : {$avg : "$amount"} } } ,
 {$sort : {"amount" : 1}}
 );
 */ 

 

 DBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$title")
 .append("average", new BasicDBObject("$avg", "$salamt")));



 AggregationOutput output = coll.aggregate(group);

for (DBObject result : output.results()) {
 System.out.println(result);
 }
 }
}
