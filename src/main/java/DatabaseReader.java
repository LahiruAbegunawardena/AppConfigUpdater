import com.mongodb.*;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by Lahiru Abegunawardena on 12/12/2018.
 */

public class DatabaseReader {
    public static void main(String args[]){

        String tenant_id = args[0], token = args[1];

        System.out.println("tenant_id : " + tenant_id);
        System.out.println("token     : " + token);

//        String file_path = "/home/dev/app-config/";
        String file_path = "D:/nCingaApps/AppConfigurations/source/app-config/";


        HashMap dataSet = new HashMap<String, String>();

//        System.out.println("\ndataset to pass : " + dataSet + "\n");

//        if(tenant_id.equals("sgt")){
//            file_path+= "sublime/";
//        } else if (tenant_id.equals("ncs")|| tenant_id.equals("ncsant")) {
//
//            file_path+= "ananta/";
//        }

        file_path  = file_path + token + "/";

        dataSet.put("tenant_id", tenant_id);
        dataSet.put("token", token);
        dataSet.put("path", file_path);

//        MongoClientURI uri = new MongoClientURI("mongodb://nspuser:nCinga123@localhost:27017/NSP_Users");
        MongoClientURI uri = new MongoClientURI("mongodb://dev_reg_test:123@localhost:27017/dev_reg_qa");

        MongoClient mongoClient = null;
        try {
            mongoClient = new MongoClient(uri);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        DB db = mongoClient.getDB("dev_reg_qa");

        DBCollection col1 = db.getCollection("ProvisioningProfile");

        Iterable<DBObject> opt = col1.aggregate(Arrays.asList(
                (DBObject) new BasicDBObject("$match", new BasicDBObject("tenant", tenant_id).append("token", token)),
                (DBObject) new BasicDBObject("$project", new BasicDBObject("_id", 0).append("app_group_id",1))
        )).results();

        for (DBObject dbObject : opt) {
            String appgrp_id = dbObject.get("app_group_id").toString();
            System.out.println(appgrp_id);

            DatabaseReader rfd = new DatabaseReader();

            rfd.checkAppGroup(db, appgrp_id, dataSet);
        }
    }

    public void checkAppGroup(DB db, String appgro_id, HashMap data){

        DBCollection coll = db.getCollection("AppGroup");
        System.out.println();
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("_id", new ObjectId(appgro_id));
        DBCursor dc;
        dc = coll.find(searchQuery);
        Map j = (Map) dc.next().toMap().get("products_map");
        Collection<Object> a = j.values();

        int i =1;

        DatabaseReader rfd = new DatabaseReader();

        for (Object b  : a) {

            System.out.println("\n\nProduction_release no : " + i + "\n\n");

            String latest_release = ((Map) b).get("latest").toString();
            System.out.println("latest release :" + latest_release);
            i++;

            rfd.checkRelease(db, latest_release, data);
        }

    }

    public void checkRelease(DB db, String release_id, HashMap data){
        DBCollection coll = db.getCollection("Release");

        Iterable<DBObject> opt = coll.aggregate(Arrays.asList(
                (DBObject) new BasicDBObject("$match", new BasicDBObject("_id", new ObjectId(release_id))),
                (DBObject) new BasicDBObject("$project", new BasicDBObject("_id", 0).append("product_id",1).append("version", 1))
        )).results();

        for (DBObject dbObject : opt) {
            String prod_id = dbObject.get("product_id").toString();
            String version_no = dbObject.get("version").toString();

            if(data.get("tenant_id").equals("sgt")){

                //create object according to tenant in Sublime
                SublimeSpecificRun ssr = new SublimeSpecificRun(db, prod_id, data);


            } else if ( (data.get("tenant_id").equals("ncs")) || (data.get("tenant_id").equals("ncsant")) ){

                //create object according to tenant in Ananta
                AnantaSpecificRun asr = new AnantaSpecificRun(db, prod_id, data);
            }
        }
    }
}