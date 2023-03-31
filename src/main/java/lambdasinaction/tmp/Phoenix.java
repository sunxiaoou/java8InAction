package lambdasinaction.tmp;

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
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
        properties.setProperty("phoenix.schema.mapSystemTablesToNamespace", "true");
        this.conn = DriverManager.getConnection(url, properties);
    }

    public void close() throws SQLException {
        conn.close();
    }

    public void test() throws SQLException {
        PreparedStatement statement = conn.prepareStatement("select * from \"test\"");
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString("a"));
            System.out.println(rs.getString("b"));
            System.out.println(rs.getString("c"));
        }
        statement.close();
    }
    
    public static void main(String[] args) throws SQLException {
//        Phoenix db = new Phoenix("localhost", 2181);
        Phoenix db = new Phoenix("localhost", 8765);
        db.test();
        db.close();
        System.out.println("closed");
    }
}
