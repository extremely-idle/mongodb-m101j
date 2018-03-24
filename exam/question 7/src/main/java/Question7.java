import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;

/**
 * Created by Ross on 07/05/2017.
 */
public class Question7 {
    public static void main(String[] args) {
        MongoClient c = new MongoClient();
        MongoDatabase albumDB = c.getDatabase("albums");
        MongoDatabase imagesDB = c.getDatabase("images");
        MongoCollection<Document> albums = albumDB.getCollection("albums");
        MongoCollection<Document> images = imagesDB.getCollection("images");

        List<Document> imageList = new ArrayList<Document>();
        images.find().projection(include("_id")).sort(ascending("_id")).into(imageList);
        List<Integer> imageIdList = new ArrayList<Integer>();
        for (Document image : imageList) {
            imageIdList.add(image.getInteger("_id"));
        }

        MongoCursor<Document> cursor = albums.find().projection(include("images")).sort(ascending("_id")).iterator();

        System.out.println("size => " + imageIdList.size());

        try {
            while (cursor.hasNext()) {
                Document d = cursor.next();
                List<Integer> imageDocumentList = (ArrayList<Integer>) d.get("images");
//                System.out.println("id => " + d.get("_id"));
                for (Integer image : imageDocumentList) {
                    if (imageIdList.contains(image)) {
                        imageIdList.remove(image);
                    }
                }
            }
        } finally {
            cursor.close();
        }

        System.out.println("size => " + imageIdList.size());

        for (Integer id : imageIdList) {
            images.deleteOne(Filters.eq("_id", id));
        }
    }
}
