package lambdasinaction.tmp;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Postgres {
    private final Connection conn;

    public Postgres(String hostName, int port, String dbName, String user, String password) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/%s", hostName, port, dbName);
        this.conn =  DriverManager.getConnection(url, user, password);
    }

    public void close() throws SQLException {
        conn.close();
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

    public List<String> tabList2(String schema, String pattern, String[] types) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getTables(null, schema, pattern, types);
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString(3));
        }

        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (String s : tables) {
            sb.append("'").append(s).append("', ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(")");

        Statement statement = conn.createStatement();
        String sql = "select relname, relispartition from pg_class where relname in " + sb.toString();
        rs = statement.executeQuery(sql);
        tables = new ArrayList<>();
        while (rs.next()) {
            if (! rs.getBoolean("relispartition")) {
                tables.add(rs.getString("relname"));
            }
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

    private void parseColumnMetaForGetTableColumns(ResultSet rs, List<ColumnMeta> columnMetaList) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            ColumnMeta columnMeta = new ColumnMeta(rs.getString(4));
            columnMeta.setType(rs.getInt(5));
            columnMeta.setTypeName(rs.getString(6));
            columnMeta.setLength(Long.valueOf(rs.getObject(7).toString()));
            columnMeta.setPrecision(Long.valueOf(rs.getObject(7).toString()));
            columnMeta.setScale((Integer) (rs.getObject(9)));
            columnMeta.setNullable(rs.getInt(11));
            columnMeta.setRemarks(rs.getString(12));
            columnMeta.setDefaultValues(rs.getString(13));
            if (metaData.getColumnCount() >= 23) {
                columnMeta.setAutoIncrement(rs.getString(23));
            }
            columnMetaList.add(columnMeta);
        }
    }

    public List<ColumnMeta> getTableColumns(String schemaName, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        List<ColumnMeta> columnMetaList = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(null, schemaName, tableName, null)) {
            parseColumnMetaForGetTableColumns(rs, columnMetaList);
        }
        return columnMetaList;
    }

    public Tree partitionTree(String schema, String partitionTable) throws SQLException {
        Statement statement = conn.createStatement();
        String sql = String.format(
                "select relid, parentrelid, level, " +
                        "pg_get_partkeydef(relid) as keydef, " +
                        "pg_get_expr(relpartbound, oid) as bound " +
                        "from pg_partition_tree('%s.%s') " +
                        "left join pg_class on pg_class.oid = relid", schema, partitionTable);
        ResultSet rs = statement.executeQuery(sql);
        if (! rs.isBeforeFirst()) {
            System.out.println("No partition");
            return null;
        }
        Tree tree = null;
        while (rs.next()) {
            String name = rs.getString("relid");
            String parent = rs.getString("parentrelid");
            String keyDef = rs.getString("keydef");
            String bound = rs.getString("bound");
            int level = rs.getInt("level");
            if (level == 0) {
                tree = new Tree(name, new Pair<>(keyDef, bound));
            } else if (tree != null) {
                tree.addChild(parent, name, new Pair<>(keyDef, bound));
            }
        }
        return tree;
    }

    public List<Map<String, Object>> partitionMap(String schema, String partitionTable) throws SQLException {
        Statement statement = conn.createStatement();
        String sql = String.format(
                "select relid, parentrelid, isleaf, level, " +
                        "pg_get_partkeydef(relid) as keydef, " +
                        "pg_get_expr(relpartbound, oid) as bound " +
                        "from pg_partition_tree('%s.%s') " +
                        "left join pg_class on pg_class.oid = relid", schema, partitionTable);
        ResultSet rs = statement.executeQuery(sql);
        if (! rs.isBeforeFirst()) {
            System.out.println("No partition");
            return null;
        }
        List<Map<String, Object>> list = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            map.put("name", rs.getString("relid"));
            map.put("parent", rs.getString("parentrelid"));
            map.put("isLeaf", rs.getBoolean("isleaf"));
            map.put("level", rs.getInt("level"));
            map.put("keyDef", rs.getString("keydef"));
            map.put("bound", rs.getString("bound"));
            list.add(map);
        }
        return list;
    }

    public Tree partitionTree2(List<Map<String, Object>> list) {
        Tree tree = null;
        for (Map<String, Object> map: list) {
            if ((int) map.get("level") == 0) {
                tree = new Tree((String) map.get("name"),
                        new Pair<>(map.get("keyDef"), map.get("bound")));
            } else if (tree != null) {
                tree.addChild((String) map.get("parent"), (String) map.get("name"),
                        new Pair<>(map.get("keyDef"), map.get("bound")));
            }
        }
        return tree;
    }


    public static void main(String... argv) {
        Postgres pg;
        try {
            // pg = new Postgres("localhost", 5432, "hue_d", "hue_u", "huepassword");
            pg = new Postgres("192.168.55.250", 5432, "hue_d", "hue_u", "huepassword");
            String[] types = new String[]{"TABLE", "PARTITIONED TABLE", "TYPE"};
            System.out.println(pg.tabList2("manga", null, types));
//            System.out.println(pg.inhTables("manga", "customers"));
//            List<Map<String, Object>> map = pg.partitionMap("manga", "customers");
//            System.out.println(pg.partitionTree2(map));
            System.out.println(pg.getTableColumns("manga", "students"));
            pg.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
