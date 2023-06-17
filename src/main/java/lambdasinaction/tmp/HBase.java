package lambdasinaction.tmp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

// refer to org.apache.hadoop.hbase.client sample in https://hbase.apache.org/apidocs/index.html
public class HBase {
    private static final Logger LOG = LoggerFactory.getLogger(HBase.class);

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

    public void createTable(String space, String name, String family) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        if (! admin.tableExists(tableName)) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family))
                            .setBloomFilterType(BloomType.ROWCOL)
                            .setCompressionType(Compression.Algorithm.GZ).build())
                    .build();
            admin.createTable(tableDescriptor);
        }
    }

    public void dropTable(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    public void truncateTable(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        if (admin.tableExists(tableName)) {
            if (admin.isTableEnabled(tableName)) {
                admin.disableTable(tableName);
            }
            admin.truncateTable(tableName, false);
        }
    }

    public void putTable(String space, String name, Map<String, Map<String, Map<String, String>>> rows)
            throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        try (Table table = conn.getTable(tableName)) {
            for (Map.Entry<String, Map<String, Map<String, String>>> rowEntry : rows.entrySet()) {
                String row = rowEntry.getKey();
                Put put = new Put(Bytes.toBytes(row));
                Map<String, Map<String, String>> families = rowEntry.getValue();
                for (Map.Entry<String, Map<String, String>> familyEntry : families.entrySet()) {
                    String columnFamily = familyEntry.getKey();
                    Map<String, String> qualifiers = familyEntry.getValue();
                    for (Map.Entry<String, String> qualifierEntry : qualifiers.entrySet()) {
                        String qualifier = qualifierEntry.getKey();
                        String value = qualifierEntry.getValue();
//                        System.out.println("Row: " + row + ", Column Family: " + columnFamily +
//                                ", Qualifier: " + qualifier + ", Value: " + value);
                        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                    }
                }
                table.put(put);
            }
        }
    }

    public Map<String, Map<String, Map<String, String>>> scanTable(String space, String name) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        if (! admin.tableExists(tableName)) {
            return null;
        }
        Map<String, Map<String, Map<String, String>>> tableData = new HashMap<>();
        try (Table table = conn.getTable(tableName)) {
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

    public String getCell(String space, String name, String pk, String cf, String col) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        try (Table table = conn.getTable(tableName)) {
            Get g = new Get(Bytes.toBytes(pk));
            Result r = table.get(g);
            byte[] value = r.getValue(Bytes.toBytes(cf), Bytes.toBytes(col));
            return Bytes.toString(value);
        }
    }

    public static Map<String, Map<String, Map<String, String>>> testData() {
        Map<String, Map<String, Map<String, String>>> rows = new HashMap<>();

        Map<String, Map<String, String>> row101 = new HashMap<>();
        Map<String, String> cf101 = new TreeMap<>();
        cf101.put("name", "香瓜");
        cf101.put("price", "800.0");
        row101.put("cf", cf101);
        rows.put("101", row101);

        Map<String, Map<String, String>> row102 = new HashMap<>();
        Map<String, String> cf102 = new TreeMap<>();
        cf102.put("name", "草莓");
        cf102.put("price", "150.0");
        row102.put("cf", cf102);
        rows.put("102", row102);

        Map<String, Map<String, String>> row103 = new HashMap<>();
        Map<String, String> cf103 = new TreeMap<>();
        cf103.put("name", "苹果");
        cf103.put("price", "120.0");
        row103.put("cf", cf103);
        rows.put("103", row103);

        Map<String, Map<String, String>> row104 = new HashMap<>();
        Map<String, String> cf104 = new TreeMap<>();
        cf104.put("name", "柠檬");
        cf104.put("price", "200.0");
        row104.put("cf", cf104);
        rows.put("104", row104);

        Map<String, Map<String, String>> row105 = new HashMap<>();
        Map<String, String> cf105 = new TreeMap<>();
        cf105.put("name", "橙子");
        cf105.put("price", "115.0");
        row105.put("cf", cf105);
        rows.put("105", row105);

        Map<String, Map<String, String>> row106 = new HashMap<>();
        Map<String, String> cf106 = new TreeMap<>();
        cf106.put("name", "香蕉");
        cf106.put("price", "110.0");
        row106.put("cf", cf106);
        rows.put("106", row106);

        return rows;
    }

    public static void main(String[] args) throws IOException {
//        HBase db = new HBase("localhost", 2181);
        HBase db = new HBase("192.168.55.250", 2181, "/hbase");
        System.out.println(db.getNameSpaces());
        System.out.println(db.getTables("manga"));
        db.dropTable(null, "fruit");
        db.createTable(null, "fruit", "cf");
        Map<String, Map<String, Map<String, String>>> rows = testData();
        db.putTable(null, "fruit", rows);
        System.out.println(db.scanTable(null, "fruit"));
        System.out.println(db.getCell(null, "fruit", "105", "cf", "name"));
        db.close();
    }
}