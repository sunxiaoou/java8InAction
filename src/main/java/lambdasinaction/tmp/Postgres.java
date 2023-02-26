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

        Statement stat = conn.createStatement();
        String sql = "select relname, relispartition from pg_class where relname in " + sb.toString();
        rs = stat.executeQuery(sql);
        tables = new ArrayList<>();
        while (rs.next()) {
            if (! rs.getBoolean("relispartition")) {
                tables.add(rs.getString("relname"));
            }
        }

        return tables;
    }

    public List<String> inhTables(String schema, String partitionTable) throws SQLException {
        Statement stat = conn.createStatement();
        String sql = String.format("select relid from pg_partition_tree('%s.%s') where isleaf = true",
                schema, partitionTable);
        List<String> tables = new ArrayList<>();
        ResultSet rs = stat.executeQuery(sql);
        while (rs.next()) {
            tables.add(rs.getString("relid"));
        }
        return tables;
    }

    private void parseColumnMetaForGetTableColumns(ResultSet rs, List<ColumnMeta> columnList) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            ColumnMeta column = new ColumnMeta(rs.getString(4));
            column.setType(rs.getInt(5));
            column.setTypeName(rs.getString(6));
            column.setLength(Long.valueOf(rs.getObject(7).toString()));
            column.setPrecision(Long.valueOf(rs.getObject(7).toString()));
            column.setScale((Integer) (rs.getObject(9)));
            column.setNullable(rs.getInt(11));
            column.setRemarks(rs.getString(12));
            column.setDefaultValues(rs.getString(13));
            if (metaData.getColumnCount() >= 23) {
                column.setAutoIncrement(rs.getString(23));
            }
            columnList.add(column);
        }
    }

    public List<ColumnMeta> getTableColumns(String schema, String tableName) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        List<ColumnMeta> columnList = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(null, schema, tableName, null)) {
            parseColumnMetaForGetTableColumns(rs, columnList);
        }
        return columnList;
    }

    public List<ColumnMeta> getUdtColumns(String schema, String name) throws SQLException {
        Statement stat = conn.createStatement();
        String sql = String.format(
                "select attname, atttypid, attlen, attnotnull,atthasdef, attmissingval, typname " +
                        "from pg_attribute " +
                        "left join pg_type on atttypid = pg_type.oid " +
                        "where attrelid='%s.%s'::REGCLASS",
                        schema, name);
        ResultSet rs = stat.executeQuery(sql);
        List<ColumnMeta> columns = new ArrayList<>();
        while (rs.next()) {
            ColumnMeta column = new ColumnMeta(rs.getString("attname"));
            column.setType(rs.getInt("atttypid"));
            column.setTypeName(rs.getString("typname"));
            column.setLength(Long.valueOf(rs.getObject("attlen").toString()));
            column.setPrecision(Long.valueOf(rs.getObject("attlen").toString()));
            column.setScale(0);
            column.setNullable(rs.getBoolean("attnotnull") ? 0 : 1);
            column.setRemarks(null);
            ArrayList<Object> array = (ArrayList<Object>) rs.getArray("attmissingval");
            if (array != null) {
                column.setDefaultValues(rs.getArray("attmissingval").toString());
            }
            columns.add(column);
        }
        return columns;
    }

    public String createUdtSql(String schema, String name, List<ColumnMeta> columns) {
        StringBuilder sb = new StringBuilder()
                .append("CREATE TYPE ").append(schema).append('.').append(name).append(" AS (");
        for (ColumnMeta column: columns) {
            sb.append(column.getName()).append(' ').append(column.getTypeName());
            if (column.getNullable() != 1) {
                sb.append(" NOT NULL");
            }
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(")");

        return sb.toString();
    }

    public List<String> getEnums(String schema) throws SQLException {
        Statement stat = conn.createStatement();
        String sql = String.format(
                "select t.typname " +
                        "from pg_type t " +
                        "join pg_namespace n on n.oid = t.typnamespace " +
                        "where t.typtype = 'e' and n.nspname = '%s'", schema);
        ResultSet rs = stat.executeQuery(sql);
        List<String> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getString("typname"));
        }
        return result;
    }

    public List<String> getEnumLabels(String name) throws SQLException {
        Statement stat = conn.createStatement();
        String sql = String.format(
                "select e.enumlabel " +
                        "from pg_type t " +
                        "join pg_enum e on t.oid = e.enumtypid " +
                        "where t.typname = '%s'", name);
        ResultSet rs = stat.executeQuery(sql);
        List<String> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getString("enumlabel"));
        }
        return result;
    }

    public String createEnumSql(String schema, String name, List<String> labels) {
        StringBuilder sb = new StringBuilder()
                .append("CREATE TYPE ").append(schema).append('.').append(name).append(" AS ENUM (");
        for (String label: labels) {
            sb.append("'").append(label).append("', ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append(")");

        return sb.toString();
    }

    public Tree partitionTree(String schema, String partitionTable) throws SQLException {
        Statement stat = conn.createStatement();
        String sql = String.format(
                "select relid, parentrelid, level, " +
                        "pg_get_partkeydef(relid) as keydef, " +
                        "pg_get_expr(relpartbound, oid) as bound " +
                        "from pg_partition_tree('%s.%s') " +
                        "left join pg_class on pg_class.oid = relid", schema, partitionTable);
        ResultSet rs = stat.executeQuery(sql);
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
        Statement stat = conn.createStatement();
        String sql = String.format(
                "select relid, parentrelid, isleaf, level, " +
                        "pg_get_partkeydef(relid) as keydef, " +
                        "pg_get_expr(relpartbound, oid) as bound " +
                        "from pg_partition_tree('%s.%s') " +
                        "left join pg_class on pg_class.oid = relid", schema, partitionTable);
        ResultSet rs = stat.executeQuery(sql);
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
//            System.out.println(pg.getTableColumns("manga", "students"));
            List<ColumnMeta> columns = pg.getUdtColumns("manga", "communication");
            System.out.println(columns);
            System.out.println(pg.createUdtSql("manga", "communication", columns));
            System.out.println(pg.getEnums("manga"));
            List<String> labels = pg.getEnumLabels("bug_status");
            System.out.println(labels);
            System.out.println(pg.createEnumSql("manga", "bug_status", labels));
            pg.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
