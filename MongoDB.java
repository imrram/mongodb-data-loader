
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.Sorts;

public class MongoDB {

    public static final String DATABASE_NAME = "mydb";
    public MongoClient mongoClient;
    public MongoDatabase db;

    public static void main(String[] args) throws Exception {
        MongoDB qmongo = new MongoDB();
        qmongo.connect();

        // qmongo.load();
        // qmongo.loadNest();

        // System.out.println(qmongo.query1(1000));
        // System.out.println(qmongo.query2(32));
        // System.out.println(qmongo.query2Nest(32));
        // System.out.println(qmongo.query3());
        // System.out.println(qmongo.query3Nest());
        // System.out.println(MongoDB.toString(qmongo.query4()));
        System.out.println(MongoDB.toString(qmongo.query4Nest()));
    }

    public MongoDatabase connect() {
        try {
            String url = "mongodb+srv://g24ai1056:ovE0B7h7EndHwWYi@bigdata.hdsrzgl.mongodb.net/";
            mongoClient = MongoClients.create(url);
            System.out.println("MongoClient created.");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        db = mongoClient.getDatabase(DATABASE_NAME);
        System.out.println("Connected to database: " + DATABASE_NAME);
        return db;
    }

    public void load() throws Exception {
        MongoCollection<Document> customers = db.getCollection("customer");
        MongoCollection<Document> orders = db.getCollection("orders");

        customers.drop();
        orders.drop();
        System.out.println("Dropped existing collections.");

        try (BufferedReader brCust = new BufferedReader(new FileReader("data/customer.tbl"))) {
            String line;
            while ((line = brCust.readLine()) != null) {
                line = line.endsWith("|") ? line.substring(0, line.length() - 1) : line;
                String[] parts = line.split("\\|");
                Document doc = new Document("custkey", Integer.parseInt(parts[0]))
                        .append("name", parts[1])
                        .append("address", parts[2])
                        .append("nationkey", Integer.parseInt(parts[3]))
                        .append("phone", parts[4])
                        .append("acctbal", Double.parseDouble(parts[5]))
                        .append("mktsegment", parts[6])
                        .append("comment", parts[7]);
                customers.insertOne(doc);
            }
            System.out.println("Customer data loaded.");
        } catch (Exception e) {
            System.out.println("Error while loading Customers data");
            e.printStackTrace();
        }

        try (BufferedReader brOrder = new BufferedReader(new FileReader("data/order.tbl"))) {
            String line;
            while ((line = brOrder.readLine()) != null) {
                line = line.endsWith("|") ? line.substring(0, line.length() - 1) : line;
                String[] parts = line.split("\\|");
                Document doc = new Document("orderkey", Integer.parseInt(parts[0]))
                        .append("custkey", Integer.parseInt(parts[1]))
                        .append("orderstatus", parts[2])
                        .append("totalprice", Double.parseDouble(parts[3]))
                        .append("orderdate", parts[4])
                        .append("orderpriority", parts[5])
                        .append("clerk", parts[6])
                        .append("shippriority", Integer.parseInt(parts[7]))
                        .append("comment", parts[8]);
                orders.insertOne(doc);
            }
            System.out.println("Orders data loaded.");
        } catch (Exception e) {
            System.out.println("Error while loading Orders data");
            e.printStackTrace();
        }
    }

    public void loadNest() throws Exception {
        MongoCollection<Document> nested = db.getCollection("custorders");
        nested.drop();

        MongoCollection<Document> customers = db.getCollection("customer");
        MongoCollection<Document> orders = db.getCollection("orders");

        System.out.println("Loading nested documents...");
        int count = 0;
        for (Document customer : customers.find()) {
            int custkey = customer.getInteger("custkey");
            List<Document> customerOrders = orders.find(eq("custkey", custkey)).into(new ArrayList<>());
            Document doc = new Document(customer);
            doc.append("orders", customerOrders);
            nested.insertOne(doc);
            count++;
        }
        System.out.println("Nested documents loaded: " + count);
    }

    public String query1(int custkey) {
        System.out.println("Running query1 for custkey: " + custkey);
        MongoCollection<Document> col = db.getCollection("customer");
        Document doc = col.find(eq("custkey", custkey)).first();
        return doc != null ? doc.getString("name") : "Customer not found";
    }

    public String query2(int orderId) {
        System.out.println("Running query2 for orderId: " + orderId);
        MongoCollection<Document> col = db.getCollection("orders");
        Document doc = col.find(eq("orderkey", orderId)).first();
        return doc != null ? doc.getString("orderdate") : "Order not found";
    }

    public String query2Nest(int orderId) {
        System.out.println("Running query2Nest for orderId: " + orderId);
        MongoCollection<Document> col = db.getCollection("custorders");
        Document match = col.find(new Document("orders.orderkey", orderId)).first();
        if (match != null) {
            List<Document> orders = (List<Document>) match.get("orders");
            for (Document order : orders) {
                if (order.getInteger("orderkey") == orderId) {
                    return order.getString("orderdate");
                }
            }
        }
        return "Order not found";
    }

    public long query3() {
        System.out.println("Running query3 (count all orders)");
        MongoCollection<Document> col = db.getCollection("orders");
        return col.countDocuments();
    }

    public long query3Nest() {
        System.out.println("Running query3Nest (count nested orders)");
        MongoCollection<Document> col = db.getCollection("custorders");
        long total = 0;
        for (Document doc : col.find()) {
            List<Document> orders = (List<Document>) doc.get("orders");
            total += orders.size();
        }
        return total;
    }

    public MongoCursor<Document> query4() {
        System.out.println("Running query4 (Top 5 customers by totalprice)");
        MongoCollection<Document> orders = db.getCollection("orders");

        AggregateIterable<Document> result = orders.aggregate(Arrays.asList(
                Aggregates.group("$custkey", Accumulators.sum("total", "$totalprice")),
                Aggregates.sort(Sorts.descending("total")),
                Aggregates.limit(5)
        ));

        return result.iterator();
    }

    public Iterator<Document> query4Nest() {
        System.out.println("Running query4Nest (Top 5 from nested custorders)");
        MongoCollection<Document> col = db.getCollection("custorders");

        List<Document> results = new ArrayList<>();
        for (Document cust : col.find()) {
            int custkey = cust.getInteger("custkey");
            String name = cust.getString("name");
            List<Document> orders = (List<Document>) cust.get("orders");
            double sum = 0;
            for (Document order : orders) {
                sum += order.getDouble("totalprice");
            }
            results.add(new Document("custkey", custkey).append("name", name).append("total", sum));
        }

        results.sort((a, b) -> Double.compare(b.getDouble("total"), a.getDouble("total")));

        return results.subList(0, Math.min(5, results.size())).iterator();
    }

    public static String toString(MongoCursor<Document> cursor) {
        StringBuilder buf = new StringBuilder();
        int count = 0;
        buf.append("Rows:\n");
        if (cursor != null) {
            while (cursor.hasNext()) {
                Document obj = cursor.next();
                buf.append(obj.toJson()).append("\n");
                count++;
            }
            cursor.close();
        }
        buf.append("Number of rows: ").append(count);
        return buf.toString();
    }

    public MongoDatabase getDb() {
        return db;
    }

    public static String toString(Iterator<Document> iterator) {
        StringBuilder buf = new StringBuilder();
        int count = 0;
        buf.append("Rows:\n");
        while (iterator.hasNext()) {
            Document obj = iterator.next();
            buf.append(obj.toJson()).append("\n");
            count++;
        }
        buf.append("Number of rows: ").append(count);
        return buf.toString();
    }
}
