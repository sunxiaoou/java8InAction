package lambdasinaction.tmp;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
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

    public Phoenix(String hostName, int port, String driver)
            throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName(driver);
        ds.setUrl(String.format("jdbc:phoenix:thin:url=http://%s:%d;serialization=PROTOBUF", hostName, port));;

        Properties props = new Properties();
        props.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
        props.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
        this.conn = ds.getConnection();
        ds.setConnectProperties(props);
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
//        Phoenix db = new Phoenix("localhost", 8765);
//        Phoenix db = new Phoenix("192.168.55.250", 8765);
        Phoenix db = new Phoenix("192.168.55.250", 8765,
                "org.apache.phoenix.queryserver.client.Driver");

        db.test2();
        db.close();
    }
}
