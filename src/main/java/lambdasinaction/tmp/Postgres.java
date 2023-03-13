package lambdasinaction.tmp;

import org.postgresql.util.PSQLException;

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

    public List<String> getUdts(String schema) throws SQLException {
        String sql = String.format(
                "select user_defined_type_name " +
                        "from information_schema.user_defined_types " +
                        "where user_defined_type_schema ='%s'", schema);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        List<String> names = new ArrayList<>();
        while (rs.next()) {
            names.add(rs.getString("user_defined_type_name"));
        }
        return names;
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
        String sql = String.format(
                "select t.typname " +
                        "from pg_type t " +
                        "join pg_namespace n on n.oid = t.typnamespace " +
                        "where t.typtype = 'e' and n.nspname = '%s'", schema);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        List<String> result = new ArrayList<>();
        while (rs.next()) {
            result.add(rs.getString("typname"));
        }
        return result;
    }

    public List<String> getEnumLabels(String schema, String name) throws SQLException {
        Statement stat = conn.createStatement();
//        String sql = String.format(
//                "select e.enumlabel " +
//                        "from pg_type t " +
//                        "join pg_enum e on t.oid = e.enumtypid " +
//                        "where t.typname = '%s'", name);
        String sql = String.format(
                "SELECT e.enumlabel FROM pg_catalog.pg_type t " +
                        "JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid " +
                        "WHERE t.typname = '%s' AND t.typnamespace = " +
                        "(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '%s')", name, schema);
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

    public List<String> getFunctions(String schema) throws SQLException {
        List<String> result = new ArrayList<>();
        String sql = String.format(
                "SELECT routine_name " +
                        "FROM information_schema.routines " +
                        "WHERE routine_type = 'FUNCTION' AND routine_schema = '%s'", schema);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        while (rs.next()) {
            result.add(rs.getString("routine_name"));
        }
        return result;
    }

    public List<String> getFunctionDDLs(String schema, String name) throws SQLException {
        List<String> result = new ArrayList<>();
        String sql = String.format(
                "SELECT pg_get_functiondef(p.oid) " +
                        "FROM pg_proc p " +
                        "JOIN pg_namespace n ON n.oid = p.pronamespace " +
                        "WHERE n.nspname = '%s' AND  p.proname = '%s'", schema, name);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        while (rs.next()) {
            result.add(rs.getString("pg_get_functiondef"));
        }
        return result;
    }

    public List<String> getTriggers(String schema) throws SQLException {
        List<String> result = new ArrayList<>();
        String sql = String.format(
                "SELECT tgname " +
                "FROM pg_trigger " +
                "WHERE tgrelid IN ( " +
                        "SELECT oid " +
                        "FROM pg_class " +
                        "WHERE relnamespace = ( " +
                                "SELECT oid " +
                                "FROM pg_namespace " +
                                "WHERE nspname='%s'))", schema);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        while (rs.next()) {
            result.add(rs.getString("tgname"));
        }
        return result;
    }

    public List<String> getTriggerDDLs(String schema, String name) throws SQLException {
        List<String> result = new ArrayList<>();
        String sql = String.format(
                "SELECT pg_get_triggerdef(t.oid) " +
                        "FROM pg_trigger t " +
                        "JOIN pg_class c ON t.tgrelid = c.relfilenode " +
                        "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                        "WHERE n.nspname = '%s' AND t.tgname = '%s'", schema, name);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        while (rs.next()) {
            result.add(rs.getString("pg_get_triggerdef"));
        }
        return result;
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

    public List<Map<String, Object>> partitionMap(String schema, String name) throws SQLException {
        String sql = String.format(
                "select relid, parentrelid, isleaf, level, " +
                        "pg_get_partkeydef(relid) as keydef, " +
                        "pg_get_expr(relpartbound, oid) as bound " +
                        "from pg_partition_tree('%s.%s') " +
                        "left join pg_class on pg_class.oid = relid", schema, name);
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(sql);
            if (!rs.isBeforeFirst()) {
                System.out.println("No partition");
                return null;
            }
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
        } catch (PSQLException e) {
            System.out.println(e.toString());
        }
        return list;
    }

    public List<Map<String, Object>> partitionMap2(String schema, String name) throws SQLException {
        String sql = String.format(
                "SELECT oid, 0 AS lvl, pg_get_partkeydef(oid) " +
                        "FROM pg_class " +
                        "WHERE relname = '%s' AND " +
                        "relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '%s')",
                name, schema);
        List<Map<String, Object>> result = new ArrayList<>();
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        int oid = 0;
        while (rs.next()) {
            String keyDef = rs.getString("pg_get_partkeydef");
            if (keyDef == null) {
                return null;		// no keyDef, not partition table
            }
            oid = rs.getInt("oid");
            Map<String, Object> map = new HashMap<>();
            map.put("name", schema + '.' + name);
            map.put("parent", null);
            map.put("isLeaf", false);
            map.put("level", rs.getInt("lvl"));
            map.put("keyDef", keyDef);
            map.put("bound", null);
            result.add(map);
        }

        sql = String.format(
                "WITH RECURSIVE Tree(inhrelid, inhparent, lvl) " +
                        "AS ( " +
                        "SELECT inhrelid, inhparent, 1 AS lvl, c.relname, c2.relname AS name2, " +
                        "pg_get_partkeydef(inhrelid), pg_get_expr(c.relpartbound, inhrelid) " +
                        "FROM pg_inherits " +
                        "JOIN pg_class c ON c.oid = inhrelid " +
                        "JOIN pg_class c2 ON c2.oid = inhparent " +
                        "WHERE inhparent = %d " +
                        "UNION ALL " +
                        "SELECT i.inhrelid, i.inhparent, t.lvl + 1, c.relname, c2.relname AS name2, " +
                        "pg_get_partkeydef(i.inhrelid), pg_get_expr(c.relpartbound, i.inhrelid) " +
                        "FROM pg_inherits i " +
                        "JOIN Tree t on i.inhparent = t.inhrelid " +
                        "JOIN pg_class c ON c.oid = i.inhrelid " +
                        "JOIN pg_class c2 ON c2.oid = i.inhparent " +
                        ")" +
                        "SELECT * FROM Tree ORDER BY lvl, inhrelid", oid);
        rs = stat.executeQuery(sql);
        while (rs.next()) {
            String keyDef = rs.getString("pg_get_partkeydef");
            Map<String, Object> map = new HashMap<>();
            map.put("name", schema + '.' + rs.getString("relname"));
            map.put("parent", schema + '.' + rs.getString("name2"));
            map.put("isLeaf", keyDef == null);
            map.put("level", rs.getInt("lvl"));
            map.put("keyDef", keyDef);
            map.put("bound", rs.getString("pg_get_expr"));
            result.add(map);
        }

        return result;
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

    public String getTableSpace(String schema, String name) throws SQLException {
        List<String> result = new ArrayList<>();
        String sql = String.format(
                "SELECT t.spcname " +
                        "FROM pg_tablespace t " +
                        "JOIN pg_class c ON c.reltablespace = t.oid " +
                        "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                        "WHERE n.nspname = '%s' AND c.relname = '%s'", schema, name);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(sql);
        if (! rs.isBeforeFirst()) {
            System.out.println("No table space");
            return null;
        }
        while (rs.next()) {
            result.add(rs.getString("spcname"));
        }
        return result.get(0);
    }

    public static void main(String... argv) {
        Postgres pg;
        try {
//            pg = new Postgres("localhost", 5432, "hue_d", "hue_u", "huepassword");
//            pg = new Postgres("192.168.55.250", 5432, "hue_d", "hue_u", "huepassword");
            pg = new Postgres("192.168.55.12", 5432, "manga", "manga", "manga");
            String[] types = new String[]{"TABLE", "PARTITIONED TABLE", "TYPE"};
            System.out.println(pg.tabList2("manga", null, types));
//            System.out.println(pg.inhTables("manga", "customers"));
//            List<Map<String, Object>> map = pg.partitionMap("manga", "customers");
//            System.out.println(map);
//            System.out.println(pg.partitionMap2("manga", "customers"));
//            System.out.println(pg.partitionMap2("manga", "fruit"));
//            System.out.println(pg.partitionTree2(map));
//            System.out.println(pg.getTableColumns("manga", "students"));
//            System.out.println(pg.getUdts("manga"));
//            List<ColumnMeta> columns = pg.getUdtColumns("manga", "communication");
//            System.out.println(columns);
//            System.out.println(pg.createUdtSql("manga", "communication", columns));
//            System.out.println(pg.getEnums("manga"));
//            List<String> labels = pg.getEnumLabels("manga", "bug_status");
//            System.out.println(labels);
//            System.out.println(pg.createEnumSql("manga", "bug_status", labels));
//            System.out.println(pg.getFunctions("manga"));
//            System.out.println(pg.getFunctionDDLs("manga", "counts"));
//            System.out.println(pg.getTriggers("manga"));
//            System.out.println(pg.getTriggerDDLs("manga", "check_update"));
            System.out.println(pg.getTableSpace("manga", "customers"));
            pg.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
