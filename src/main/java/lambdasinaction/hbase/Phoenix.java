package lambdasinaction.hbase;

import com.alibaba.druid.pool.DruidDataSource;
import lambdasinaction.tmp.ColumnMeta;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


public class Phoenix {
    private final Connection conn;

    public Phoenix(String hostName, int port) throws SQLException {
        String url;
        if (port == 2181) {
            url = String.format("jdbc:phoenix:%s:%d", hostName, port);
        } else if (port == 8765) {
            url = String.format("jdbc:phoenix:thin:url=http://%s:%d;serialization=PROTOBUF", hostName, port);
        } else {
            throw new RuntimeException();
        }
        Properties props = new Properties();
        props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
        props.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
        this.conn = DriverManager.getConnection(url, props);
    }

    public Phoenix(String hostName, int port, String driver) throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName(driver);
        ds.setUrl(String.format("jdbc:phoenix:thin:url=http://%s:%d;serialization=PROTOBUF", hostName, port));;
        this.conn = ds.getConnection();

//        Properties props = new Properties();
//        props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
//        props.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
//        ds.setConnectProperties(props);
    }

    public List<String> getSchemas() throws SQLException {
//        String sql = "SELECT * FROM SYSTEM.CATALOG";
        String sql = "SELECT TABLE_SCHEM FROM SYSTEM.CATALOG GROUP BY TABLE_SCHEM";
        List<String> result = new LinkedList<>();
        try(Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    result.add(rs.getString("TABLE_SCHEM"));
                }
            }
        }
        return result;
    }

    public List<String> getTables(String schema) throws SQLException {
        String sql;
        if (schema == null) {
            sql = "SELECT TABLE_NAME FROM SYSTEM.CATALOG " +
                    "WHERE TABLE_SCHEM is null AND DATA_TYPE is null";
        } else {
            sql = String.format(
                    "SELECT TABLE_NAME FROM SYSTEM.CATALOG " +
                            "WHERE TABLE_SCHEM = '%s' AND DATA_TYPE is null", schema);
        }
        List<String> result = new LinkedList<>();
        try(Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    result.add(rs.getString("TABLE_NAME"));
                }
            }
        }
        return result;
    }

    private ColumnMeta generateColumnMeta(ResultSetMetaData meta, int i) throws SQLException {
        String columnLabel = meta.getColumnLabel(i);
        int type = meta.getColumnType(i);
        String typeName = meta.getColumnTypeName(i);
        int length = meta.getPrecision(i);
        int scale = meta.getScale(i);
        int nullable = meta.isNullable(i);
        return new ColumnMeta(columnLabel, type, typeName, (long) length, (long) length,
                scale, nullable, null, null);
    }

    public List<ColumnMeta> getColumns(String schema, String name) throws SQLException {
        String sql = "SELECT \"fruit_id\", \"price\" FROM \"manga\".\"fruit\" " +
                "where 1=2";
        List<ColumnMeta> result = new LinkedList<>();
        try(Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                for (int i = 1; i <= meta.getColumnCount(); i ++ ) {
                    ColumnMeta column = generateColumnMeta(meta, i);
                    result.add(column);
                }
            }
        }
        return result;
    }

    public void close() throws SQLException {
        conn.close();
    }

    public void test() throws SQLException {
        PreparedStatement statement = conn.prepareStatement("select * from \"test\"");
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString("ROW"));
            System.out.println(rs.getString("a"));
            System.out.println(rs.getString("b"));
            System.out.println(rs.getString("c"));
        }
        statement.close();
    }

    public void test2() throws SQLException {
        PreparedStatement statement = conn.prepareStatement("select * from \"manga\".\"fruit\"");
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString("fruit_id"));
            System.out.println(rs.getString("name"));
            System.out.println(rs.getString("price"));
        }
        statement.close();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
//        Phoenix db = new Phoenix("localhost", 2181);
        String driver = "org.apache.phoenix.queryserver.client.Driver";
        Phoenix db = new Phoenix("192.168.55.250", 8765, driver);
//        Phoenix db = new Phoenix("192.168.55.12", 8765, driver);

//        db.test2();
//        System.out.println(db.getSchemas());
//        System.out.println(db.getTables(null));
//        System.out.println(db.getTables("manga"));
        System.out.println(db.getColumns("manga", "fruit"));
        db.close();
    }
}
