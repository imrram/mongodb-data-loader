# MongoDB Data Loader & Query Runner

This repo connects to a MongoDB instance (Atlas or local), loads data from `.tbl` files (`customer.tbl`, `order.tbl`), and supports querying via multiple methods using the MongoDB Java Driver.

---

## Directory Structure

```
.
├── MongoDB.java
├── data/
│   ├── customer.tbl
│   ├── orders.tbl
│   └── tpch_ddl.sql
├── run.sh
└── README.md
```

---

## Requirements

- Java 8+
- MongoDB (Atlas or Local)
- MongoDB Java Driver 4.x
- `.tbl` files inside `data/` folder

---

## Dependencies

Driver which are required to execute the code are under jar_files folder:

```
bson
gson
mongodb-driver-core
mongodb-driver-sync
```

If compiling manually, download the `.jar` and run with:

```bash
javac -cp ".:jar_files/*" MongoDB.java
java -cp ".:jar_files/*" MongoDB
```

---

## How to Use

### 1. Set Up MongoDB Connection

Update the `MongoDB.java` with your MongoDB URI (local or Atlas):

```java
String url = "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/";
```

### 2. Load Data

In `main()` of `MongoDB.java`, uncomment these lines to load and nest data:

```java
qmongo.load();
qmongo.loadNest();
```

Then run:

```bash
./run.sh
```

### 3. Run Queries

Uncomment any of these to test:

```java
System.out.println(qmongo.query1(1000));
System.out.println(qmongo.query2(32));
System.out.println(qmongo.query2Nest(32));
System.out.println(qmongo.query3());
System.out.println(qmongo.query3Nest());
System.out.println(MongoDB.toString(qmongo.query4()));
System.out.println(MongoDB.toString(qmongo.query4Nest()));
```

---

## Queries Explained

| Method             | Description                                      |
|--------------------|--------------------------------------------------|
| `query1(custkey)`  | Get customer name by custkey                     |
| `query2(orderId)`  | Get order date from `orders`                     |
| `query2Nest(orderId)` | Get order date from nested `custorders`       |
| `query3()`         | Count total orders in `orders` collection        |
| `query3Nest()`     | Count total nested orders in `custorders`        |
| `query4()`         | Top 5 customers by total price using aggregation |
| `query4Nest()`     | Same as above using nested documents             |

---

## Notes

- Files must be `|` delimited, as expected in `.tbl` format.
- Trailing newlines in `.tbl` files may cause parse errors — ensure clean files.
- `custorders` collection contains nested documents from `customer` and `orders`.

---

## Troubleshooting

- **SLF4J Warning**: Logging is disabled — can be ignored for assignments.
- **ClassNotFoundError**: Ensure all MongoDB driver `.jar` files are added in `lib/` folder.
- **No output from query4Nest**: Ensure `.toString()` is used, not `System.out.println(iterator)`.

---

## License

For academic and learning purposes only. Not intended for production use.

---
