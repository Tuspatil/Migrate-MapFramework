
package SourceCode;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import com.mongodb.AggregationOutput;
import com.mongodb.MongoClient;

import com.mongodb.DB;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;


import static javax.management.Query.match;
/**
 *
 * @author tushar
 */
public class Mongo {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            
            // Now connect to your databases
            DB db = mongoClient.getDB( "tushar" );
            DBCollection collection = db.getCollection("col2");
            //DBObject match = new BasicDBObject("$match", new BasicDBObject("type", "student") );
            // DBObject match = new BasicDBObject("$match", new BasicDBObject() );

// build the $projection operation
DBObject fields = new BasicDBObject("dept", 1);
fields.put("amount", 1);
fields.put("_id", 0);
DBObject project = new BasicDBObject("$project", fields );

// Now the $group operation
DBObject groupFields = new BasicDBObject( "_id", "$dept");
groupFields.put("average", new BasicDBObject( "$avg", "$amount"));
DBObject group = new BasicDBObject("$group", groupFields);

// run aggregation
AggregationOutput output = collection.aggregate(project, group );
System.out.println(output.getCommandResult());;
        } catch (UnknownHostException ex) {
            Logger.getLogger(Mongo.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}