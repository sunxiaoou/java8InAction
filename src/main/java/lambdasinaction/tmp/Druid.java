package lambdasinaction.tmp;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.*;

public class Druid {
    private final Connection conn;

    public Druid(String hostName, String dbName, String user, String password, String driver)
            throws SQLException, ClassNotFoundException {
        // Load the MySQL JDBC driver
        Class.forName(driver);
        // Initialize the MySQL data source
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName(driver);
        ds.setUrl(String.format("jdbc:mysql://%s:3306/%s", hostName, dbName));
        ds.setUsername(user);
        ds.setPassword(password);

        this.conn = ds.getConnection();
    }

    public Connection get_connection() {
        return conn;
    }

    public void close() throws SQLException {
        conn.close();
    }

    public void test() throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from fruit");
        while (rs.next()) {
            System.out.println(rs.getString("fruit_id"));
            System.out.println(rs.getString("name"));
            System.out.println(rs.getString("price"));
        }
        statement.close();
    }

    public static void main(String... argv) {
        Druid db;
        try {
            String driver = "com.mysql.cj.jdbc.Driver";
            db = new Druid("localhost", "manga", "manga", "manga", driver);
            db.test();
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
