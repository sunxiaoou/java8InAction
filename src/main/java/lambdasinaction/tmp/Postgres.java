package lambdasinaction.tmp;

import java.sql.*;
import java.util.*;

public class Postgres {
    private final Connection conn;

    public Postgres(String hostName, int port, String dbName, String user, String password) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/%s", hostName, port, dbName);
        this.conn =  DriverManager.getConnection(url, user, password);
    }

    public void close() throws SQLException {
        conn.close();
    }

    public List<String> schemaList() throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SHOW DATABASES");
        List<String> databases = new ArrayList<>();
        while (rs.next()) {
            databases.add(rs.getString(1));
        }
        return databases;
    }

    public List<String> tabList(String schema) throws SQLException {
        String sql = String.format("select * from information_schema.tables where table_schema = '%s';", schema);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql);
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString(3));
        }
        return tables;
    }

    public Map<String, String> tabList2(String schema) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getTables(null, schema, null,
                new String[]{"TABLE", "PARTITIONED TABLE"});
        Map<String, String> tables = new HashMap<>();
        while (rs.next()) {
            tables.put(rs.getString(3),rs.getString(4));
        }
        return tables;
    }

    public List<String> inhTables(String schema, String partitionTable) throws SQLException {
        Statement statement = conn.createStatement();
        String sql = String.format("select relid from pg_partition_tree('%s.%s') where isleaf = true",
                schema, partitionTable);
        List<String> tables = new ArrayList<>();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            tables.add(rs.getString("relid"));
        }
        return tables;
    }

    public void partition(String schema, String partitionTable) throws SQLException {
        Statement statement = conn.createStatement();
        String sql = String.format("select inhrelid, inhparent, relname, relpartbound from pg_inherits " +
                "join pg_class on pg_inherits.inhrelid = pg_class.oid " +
                "where inhparent = '%s.%s'::regclass",
                schema, partitionTable);
        ResultSet rs = statement.executeQuery(sql);
        Map<String, String> inhTables = new HashMap<>() ;
        int inhParent = -1;
        while (rs.next()) {
            inhParent = rs.getInt("inhparent");
            inhTables.put(rs.getString("relname"), rs.getString("relpartbound"));
        }

        System.out.println(String.format("%s.%s(%d)", schema, partitionTable, inhParent));
        sql = String.format("select * from pg_catalog.pg_partitioned_table where partrelid=%d", inhParent);
        rs = statement.executeQuery(sql);
        Map<String, String> attrs = new HashMap<>();
        while (rs.next()) {
            attrs.put("partstrat", rs.getString("partstrat"));
            attrs.put("partnatts", rs.getString("partnatts"));
            attrs.put("partdefid", rs.getString("partdefid"));
            attrs.put("partattrs", rs.getString("partattrs"));
        }
        System.out.println(attrs);
        System.out.println(inhTables);
    }

    public void partition2(String schema, String partitionTable) throws SQLException {
        Statement statement = conn.createStatement();
        String sql = String.format(
                "select inhparent, relname, pg_get_expr(relpartbound, inhrelid) as bound from pg_inherits " +
                "join pg_class on pg_inherits.inhrelid = pg_class.oid " +
                "where inhparent = '%s.%s'::regclass", schema, partitionTable);
        ResultSet rs = statement.executeQuery(sql);
        Map<String, String> inhTables = new HashMap<>() ;
        int inhParent = -1;
        if (! rs.isBeforeFirst()) {
            System.out.println("No data");
            return;
        }
        while (rs.next()) {
            inhParent = rs.getInt("inhparent");
            inhTables.put(rs.getString("relname"), rs.getString("bound"));
        }

        sql = String.format("select pg_get_partkeydef(%d) as keydef", inhParent);
        rs = statement.executeQuery(sql);
        String keyDef = null;
        while (rs.next()) {
            keyDef = rs.getString("keydef");
        }
        System.out.println(String.format("%s.%s(%d)", schema, partitionTable, inhParent));
        System.out.println(keyDef);
        System.out.println(inhTables);
    }

    public Tree partitionTree(String schema, String partitionTable) throws SQLException {
        Statement statement = conn.createStatement();
        String sql = String.format("select * from pg_partition_tree('%s.%s')",
                schema, partitionTable);
        List<String> tables = new ArrayList<>();
        ResultSet rs = statement.executeQuery(sql);
        if (! rs.isBeforeFirst()) {
            System.out.println("No partition");
            return null;
        }
        Tree tree = null;
        while (rs.next()) {
            String name = rs.getString("relid");
            String parent = rs.getString("parentrelid");
            boolean leaf = rs.getBoolean("isleaf");
            int level = rs.getInt("level");
            if (level == 0) {
                tree = new Tree(name, new Pair<>(leaf, level));
            } else if (tree != null) {
                tree.addChild(parent, name, new Pair<>(leaf, level));
            }
        }
        return tree;
    }

    public static void main(String... argv) {
        Postgres pg;
        try {
            // pg = new Postgres("192.168.55.250", 5432, "hue_d", "hue_u", "huepassword");
            pg = new Postgres("localhost", 5432, "hue_d", "hue_u", "huepassword");
            System.out.println(pg.tabList2("manga"));
            System.out.println(pg.inhTables("manga", "customers"));
            Tree tree = pg.partitionTree("manga", "customers");
            tree.preOrder(null);
            pg.partition2("manga", "customers");
            pg.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
