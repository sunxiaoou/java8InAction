package lambdasinaction.tmp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBase {
    Connection conn;
    Admin admin;

    public HBase(String host, int port, String znode) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        conf.set("hbase.zookeeper.property.clientPort", "" + port);
        conf.set("zookeeper.znode.parent", znode);
//        conf.addResource("target/classes/lambdasinaction/tmp/hbase-site.xml");
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public void close() throws IOException {
        admin.close();
        conn.close();
    }

    public List<String> getNameSpaces() throws IOException {
        List<String> spaces = new ArrayList<>();
        NamespaceDescriptor[] descriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor descriptor : descriptors) {
            spaces.add(descriptor.getName());
        }
        return spaces;
    }

    public List<String> getTables(String space) throws IOException {
        List<String> tables = new ArrayList<>();
        List<TableDescriptor> descriptors = admin.listTableDescriptorsByNamespace(Bytes.toBytes(space));
        for (TableDescriptor descriptor : descriptors) {
            tables.add(descriptor.getTableName().getNameAsString());
        }
        return tables;
    }

    public Map<String, Map<String, Map<String, String>>> scanTable(String name) throws IOException {
        TableName tableName = TableName.valueOf(name);
        if (! admin.tableExists(tableName)) {
            return null;
        }
        Map<String, Map<String, Map<String, String>>> tableData = new HashMap<>();
        try (Table table = conn.getTable(TableName.valueOf(name))) {
            try (ResultScanner scanner = table.getScanner(new Scan())) {
                for (Result result: scanner) {
                    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyMap = result.getNoVersionMap();
                    Map<String, Map<String, String>> row = new HashMap<>();
                    for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> familyEntry: familyMap.entrySet()) {
                        NavigableMap<byte[], byte[]> qualifierMap = familyEntry.getValue();
                        Map<String, String> family = new TreeMap<>();
                        for (Map.Entry<byte[], byte[]> qualifierEntry: qualifierMap.entrySet()) {
                            family.put(Bytes.toString(qualifierEntry.getKey()),
                                    Bytes.toString(qualifierEntry.getValue()));
                        }
                        row.put(Bytes.toString(familyEntry.getKey()), family);
                    }
                    tableData.put(Bytes.toString(result.getRow()), row);
                }
            }
        }
        return tableData;
    }

    class HTable {
        Table table;
        String cf;

        public HTable(String name, String cf) throws IOException {
            TableName tableName = TableName.valueOf(name);
            if (! admin.tableExists(tableName)) {
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(
                                ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf))
                                        .setBloomFilterType(BloomType.ROWCOL)
                                        .setCompressionType(Compression.Algorithm.GZ)
                                        .build())
                        .build();
                admin.createTable(tableDescriptor);
            }
            table = conn.getTable(TableName.valueOf(name));
            this.cf = cf;
        }

        public void close() throws IOException {
            table.close();
        }

        public void truncate(String name) throws IOException {
            TableName tableName = TableName.valueOf(name);
            if (admin.tableExists(tableName)) {
                if (admin.isTableEnabled(tableName)) {
                    admin.disableTable(tableName);
                }
                admin.truncateTable(tableName, false);
            }
        }

        public void drop(String name) throws IOException {
            TableName tableName = TableName.valueOf(name);
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        }

        public void put(List<Map<String, String>> rows) throws IOException {
            for (Map<String, String> row : rows) {
                Put p = new Put(Bytes.toBytes(row.get("ROW")));
                for (Map.Entry<String, String> entry : row.entrySet()) {
                    if (!entry.getKey().equals("ROW")) {
                        p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                    }
                }
                table.put(p);
            }
        }

        public String get(String pk, String col) throws IOException {
            Get g = new Get(Bytes.toBytes(pk));
            Result r = table.get(g);
            byte[] value = r.getValue(Bytes.toBytes(cf), Bytes.toBytes(col));
            return Bytes.toString(value);
        }

        public List<String> scan(String col) throws IOException {
            Scan s = new Scan();
            s.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col));
            ResultScanner scanner = table.getScanner(s);
            List<String> rows = new ArrayList<>();
            for (Result rr : scanner) {
                rows.add(rr.toString());
            }
            scanner.close();
            return rows;
        }
    }

    public void test() throws IOException {
        List<Map<String, String>> list = new ArrayList<>();
        Map<String, String> map1 = new HashMap<>();
        map1.put("ROW", "101");
        map1.put("name", "香瓜");
        map1.put("price", "800.0");
        list.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("ROW", "102");
        map2.put("name", "草莓");
        map2.put("price", "150.0");
        list.add(map2);

        Map<String, String> map3 = new HashMap<>();
        map3.put("ROW", "103");
        map3.put("name", "苹果");
        map3.put("price", "120.0");
        list.add(map3);

        HTable table = new HTable("fruit", "cf");
        table.put(list);
        System.out.println(table.get("103", "name"));
        System.out.println(table.scan("price"));
        table.close();
    }

    // https://hbase.apache.org/apidocs/index.html
    public void test(String name) throws IOException {
        Table table = conn.getTable(TableName.valueOf(name));
        try {

            // To add to a row, use Put.  A Put constructor takes the name of the row
            // you want to insert into as a byte array.  In HBase, the Bytes class has
            // utility for converting all kinds of java types to byte arrays.  In the
            // below, we are converting the String "myLittleRow" into a byte array to
            // use as a row key for our update. Once you have a Put instance, you can
            // adorn it by setting the names of columns you want to update on the row,
            // the timestamp to use in your update, etc. If no timestamp, the server
            // applies current time to the edits.
            Put p = new Put(Bytes.toBytes("myLittleRow"));

            // To set the value you'd like to update in the row 'myLittleRow', specify
            // the column family, column qualifier, and value of the table cell you'd
            // like to update.  The column family must already exist in your table
            // schema.  The qualifier can be anything.  All must be specified as byte
            // arrays as hbase is all about byte arrays.  Lets pretend the table
            // 'myLittleHBaseTable' was created with a family 'myLittleFamily'.
            p.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"),
                    Bytes.toBytes("Some Value"));

            // Once you've adorned your Put instance with all the updates you want to
            // make, to commit it do the following (The HTable#put method takes the
            // Put instance you've been building and pushes the changes you made into
            // hbase)
            table.put(p);

            // Now, to retrieve the data we just wrote. The values that come back are
            // Result instances. Generally, a Result is an object that will package up
            // the hbase return into the form you find most palatable.
            Get g = new Get(Bytes.toBytes("myLittleRow"));
            Result r = table.get(g);
            byte[] value = r.getValue(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));

            // If we convert the value bytes, we should get back 'Some Value', the
            // value we inserted at this location.
            String valueStr = Bytes.toString(value);
            System.out.println("GET: " + valueStr);

            // Sometimes, you won't know the row you're looking for. In this case, you
            // use a Scanner. This will give you cursor-like interface to the contents
            // of the table.  To set up a Scanner, do like you did above making a Put
            // and a Get, create a Scan.  Adorn it with column names, etc.
            Scan s = new Scan();
            s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
            ResultScanner scanner = table.getScanner(s);
            try {
                // Scanners return Result instances.
                // Now, for the actual iteration. One way is to use a while loop like so:
                for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                    // print out the row we found and the columns we were looking for
                    System.out.println("Found row: " + rr);
                }

                // The other approach is to use a foreach loop. Scanners are iterable!
                // for (Result rr : scanner) {
                //   System.out.println("Found row: " + rr);
                // }
            } finally {
                // Make sure you close your scanners when you are done!
                // Thats why we have it inside a try/finally clause
                scanner.close();
            }

            // Close your table and cluster connection.
        } finally {
            if (table != null) table.close();
        }
    }

    public static void main(String[] args) throws IOException {
//        HBase db = new HBase("localhost", 2181);
        HBase db = new HBase("192.168.55.250", 2181, "/hbase");
        System.out.println(db.getNameSpaces());
        System.out.println(db.getTables("manga"));
        System.out.println(db.scanTable("manga:fruit"));
//        db.dropTable("myLittleHBaseTable");
//        db.createTable("myLittleHBaseTable", "myLittleFamily");
//        db.test("myLittleHBaseTable");
//        db.test();
        db.close();
    }
}