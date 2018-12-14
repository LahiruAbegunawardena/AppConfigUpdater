import com.mongodb.*;
import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Lahiru Abegunawardena on 12/12/2018.
 */

public class SublimeSpecificRun {
    String Prod_Id;
    HashMap Dta;

    SublimeSpecificRun(DB db, String prod_id, HashMap data){
        this.Prod_Id = prod_id;
        this.Dta = data;

        this.getProductEntryData(db);
    }

    public void getProductEntryData(DB db){

        DBCollection coll = db.getCollection("ProductEntry");

        Iterable<DBObject> opt = coll.aggregate(Arrays.asList(
                (DBObject) new BasicDBObject("$match", new BasicDBObject("_id", new ObjectId(this.Prod_Id))),
                (DBObject) new BasicDBObject("$project", new BasicDBObject("_id", 0).append("className",0))
        )).results();

        for (DBObject dbObject : opt) {
            String product_type = dbObject.get("product_type").toString();
            String tenant_id = this.Dta.get("tenant_id").toString();
            String token = this.Dta.get("token").toString();
            String path = this.Dta.get("path").toString();

//            path =  path + "sublime-" + product_type + ".json";
            path =  path + token  + "-" + product_type + ".json";

            if (product_type.equals("endline_qc")) {
                this.readFile1(dbObject, path, coll);
            }else{
                this.readFile2(dbObject, path, coll);
            }
        }

        System.out.println("\n...............................................................................................");
        System.out.println("...............................................................................................\n");
//        System.out.println("ProductEntry collection connected..");

    }

    public  void readFile1(DBObject dbo, String path, DBCollection coll){
        System.out.println("\npath of the file to read : "+ path + "\n");

        JSONParser passer = new JSONParser();


        Object obj = null;
        try {
            obj = passer.parse(new FileReader(path));

            JSONObject jso = (JSONObject) obj;
            jso.remove("version");
            jso.remove("app_id");

            Object jso_o = (Object) jso;

            System.out.println("File read output as Object : " + jso_o);

            Object DBo = (Object) dbo;
            System.out.println("\nDatabase output as Object  : " + DBo);

//            Dbo_m.g

            if (jso_o.toString().equals(DBo.toString())){
                System.out.println("\n.................................. Matches ..................................");
            }else{
                System.out.println("\n............................... Doesn't Match ...............................");

                Map DB_toset = (Map)jso_o;

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("className", "com.ncinga.db.data.ProductEntry");
                newDocument.putAll(DB_toset);

                BasicDBObject searchQuery = new BasicDBObject().append("_id", new ObjectId(this.Prod_Id));

                coll.update(searchQuery, newDocument);

            }

        } catch (IOException e) {
            System.out.println("Cannot find this file...");
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public  void readFile2(DBObject dbo, String path, DBCollection coll){
        System.out.println("\npath of the file to read : "+ path + "\n");

        JSONParser passer = new JSONParser();


        Object obj = null;
        try {

            //file reader configuration
            obj = passer.parse(new FileReader(path));

            JSONObject jso = (JSONObject) obj;
            JSONArray obj2 = (JSONArray)jso.get("apps");
            JSONObject obj3 = (JSONObject)(obj2.get(0));

            obj3.remove("version");
            obj3.remove("app_id");

            Object jso_o2 = (Object) obj3;
            System.out.println("File read output as Object : " + jso_o2);
            //end of file reader configuration


            //begin dboutput formatting


            Object DBo = (Object) dbo;
            System.out.println("\nDatabase output as Object  : " + DBo);
            //end of db output formatting

            if (jso_o2.toString().equals(DBo.toString())){
                System.out.println("\n.................................. Matches ..................................");
            }else{
                System.out.println("\n............................... Doesn't Match ...............................");

                Map DB_toset = (Map)jso_o2;

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("className", "com.ncinga.db.data.ProductEntry");
                newDocument.putAll(DB_toset);
                BasicDBObject searchQuery = new BasicDBObject().append("_id", new ObjectId(this.Prod_Id));
                coll.update(searchQuery, newDocument);


            }

        } catch (IOException e) {
            System.out.println("file not found");
        } catch (ParseException e) {
            System.out.println("json parse error found");
        }
    }
}
