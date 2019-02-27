import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Lahiru Abegunawardena on 1/14/2019.
 */
public class JSONFileReader {
    String Prod_Id;
    HashMap Dta;

    JSONFileReader(DB db, String prod_id, HashMap data){
        this.Prod_Id = prod_id;
        this.Dta = data;
        System.out.println("Data print in JSONFileReader : " + this.Dta);

        this.getProductEntryData(db);
    }

    public void getProductEntryData(DB db){

        DBCollection coll = db.getCollection("ProductEntry");

        Iterable<DBObject> opt = coll.aggregate(Arrays.asList(
//                (DBObject) new BasicDBObject("$unwind", "$ProductEntry"),
                (DBObject) new BasicDBObject("$match", new BasicDBObject("_id", new ObjectId(this.Prod_Id))),
                (DBObject) new BasicDBObject("$project", new BasicDBObject("_id", 0).append("className",0))
        )).results();

        for (DBObject dbObject : opt) {
            String product_type = dbObject.get("product_type").toString();
            String tenant_id = this.Dta.get("tenant_id").toString();
            String token = this.Dta.get("token").toString();
            String path = this.Dta.get("path").toString();



            this.readFile(dbObject, path, coll, token, product_type);

        }

        System.out.println("\n...............................................................................................");
        System.out.println("...............................................................................................\n");
//        System.out.println("ProductEntry collection connected..");

    }

    public  void readFile(DBObject dbo, String path, DBCollection coll, String token, String product_type){

        String path1 =  path + token  + "-" + product_type + ".json";
        System.out.println("\npath of the file to read : "+ path1 + "\n");

        JSONParser passer = new JSONParser();

        try {

            //file reader configuration
            Object obj = passer.parse(new FileReader(path1));
            this.FileReaderCon(obj, dbo, coll);

        } catch (IOException e) {
            System.out.println("file not found...");
            String path2 =  path + product_type + ".json";
            System.out.println("edited file path: " + path2);

            try {
                Object obj2 = passer.parse(new FileReader(path2));
                this.FileReaderCon(obj2, dbo, coll);

            } catch (IOException e1) {
                System.out.println("There is no file for this db query");
            } catch (ParseException e1) {
                System.out.println("Parse error found this time...");
//                e1.printStackTrace();
            }
        } catch (ParseException e) {
            System.out.println("Parse error found... " + e);
        }

    }

    public void FileReaderCon(Object obj, DBObject dbo, DBCollection coll){
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
            System.out.println("\n.......................... Successfully updated db ..........................");

        }
    }
}
