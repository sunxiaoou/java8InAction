package lambdasinaction.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.TestReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;
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

    public List<String> listNameSpaces() throws IOException {
        List<String> spaces = new ArrayList<>();
        NamespaceDescriptor[] descriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor descriptor : descriptors) {
            spaces.add(descriptor.getName());
        }
        LOG.debug("spaces: {}", spaces);
        return spaces;
    }

    public List<String> listTables(String space) throws IOException {
        List<String> tables = new ArrayList<>();
        List<TableDescriptor> descriptors = admin.listTableDescriptorsByNamespace(Bytes.toBytes(space));
        for (TableDescriptor descriptor : descriptors) {
            tables.add(descriptor.getTableName().getNameAsString());
        }
        LOG.debug("tables: {}", tables);
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

    public void putRow(String space, String name, Pair<String, Map<String, Map<String, String>>> row)
            throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        try (Table table = conn.getTable(tableName)) {
                String key = row.getFirst();
                Put put = new Put(Bytes.toBytes(key));
                Map<String, Map<String, String>> families = row.getSecond();
            for (Map.Entry<String, Map<String, String>> familyEntry : families.entrySet()) {
                String columnFamily = familyEntry.getKey();
                Map<String, String> qualifiers = familyEntry.getValue();
                for (Map.Entry<String, String> qualifierEntry : qualifiers.entrySet()) {
                    String qualifier = qualifierEntry.getKey();
                    String value = qualifierEntry.getValue();
                    LOG.debug("Row: {}, column family: {}, qualifier: {}, value: {}",
                            key, columnFamily, qualifier, value);
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                }
            }
            table.put(put);
        }
    }

    public void putRows(String space, String name, Map<String, Map<String, Map<String, String>>> rows)
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

    public void deleteRow(String space, String name, String key) throws IOException {
        TableName tableName = TableName.valueOf(space == null ? name : space + ':' + name);    // qualified name
        try (Table table = conn.getTable(tableName)) {
            table.delete(new Delete(Bytes.toBytes(key)));
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

    public List<String> listPeers() throws IOException {
        List<String> peers = new ArrayList<>();
        List<ReplicationPeerDescription> descriptions = admin.listReplicationPeers();
        for (ReplicationPeerDescription descriptor : descriptions) {
            peers.add(descriptor.getPeerId());
        }
        LOG.debug("peers: {}", peers);
        return peers;
    }

    public void addPeer(String peerId, String clusterKey, String endpoint) throws IOException {
        ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
                .setClusterKey(clusterKey)
                .setReplicationEndpointImpl(endpoint)
                .build();
        admin.addReplicationPeer(peerId, peerConfig, false);
    }

    public void removePeer(String peerId) throws IOException {
        admin.removeReplicationPeer(peerId);
    }

    public void test(String peerId) throws IOException {
        ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
                .setClusterKey("localhost:2181:/hb2")
                .setReplicationEndpointImpl(TestReplicationEndpoint.ReplicationEndpointForTest.class.getName())
                .build();
        admin.addReplicationPeer(peerId, peerConfig, false);

        peerConfig = admin.getReplicationPeerConfig(peerId);
        LOG.debug("peer: {}", peerConfig);
    }

    public static Pair<String, Map<String, Map<String, String>>> fruit(Triple<Integer, String, Float> triple) {
        Map<String, Map<String, String>> families = new HashMap<>();
        Map<String, String> family = new TreeMap<>();
        family.put("name", triple.getSecond());
        family.put("price", String.valueOf(triple.getThird()));
        families.put("cf", family);
        return new Pair<>(String.valueOf(triple.getFirst()), families);
    }

    public static Map<String, Map<String, Map<String, String>>> fruits() {
        final Triple<Integer, String, Float>[] triples = new Triple[]{
                new Triple<>(101, "香瓜", (float) 800.0),
                new Triple<>(102, "草莓", (float) 150.0),
                new Triple<>(103, "苹果", (float) 120.0),
                new Triple<>(104, "柠檬", (float) 200.0),
                new Triple<>(105, "橙子", (float) 115.0),
                new Triple<>(106, "香蕉", (float) 110.0)
        };
        Map<String, Map<String, Map<String, String>>> rows = new HashMap<>();
        for (Triple<Integer, String, Float> triple : triples) {
            Pair<String, Map<String, Map<String, String>>> pair = fruit(triple);
            rows.put(pair.getFirst(), pair.getSecond());
        }
        LOG.debug("fruits: {}", rows);
        return rows;
    }

    public static void main(String[] args) throws IOException {
        HBase db = new HBase("localhost", 2181, "/hbase");
//        HBase db = new HBase("192.168.55.250", 2181, "/hbase");
        db.listNameSpaces();
        db.listTables("manga");

//        db.test("htest");
//        db.dropTable(null, "fruit");
//        db.createTable("manga", "fruit", "cf");
//        db.truncateTable("manga", "fruit");
//        db.putRows("manga", "fruit", fruits());
//        db.putRow("manga", "fruit", fruit(new Triple<>(107, "🍐", (float) 115)));
        db.deleteRow("manga", "fruit", "107");
        System.out.println(db.scanTable("manga", "fruit"));
//        System.out.println(db.getCell("manga", "fruit", "105", "cf", "name"));
        db.close();
    }
}