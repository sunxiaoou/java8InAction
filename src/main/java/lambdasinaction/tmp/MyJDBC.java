package lambdasinaction.tmp;

import java.sql.*;
import java.util.*;

public class MyJDBC {
    private final Connection connection;

    public MyJDBC(String hostName, String dbName, String user, String password) throws SQLException {
        String url = "jdbc:mysql://" + hostName + "/" + dbName;
        this.connection =  DriverManager.getConnection(url, user, password);
    }

    public Connection get_connection() {
        return connection;
    }

    public List<String> db_list() throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("SHOW DATABASES");
        List<String> databases = new ArrayList<>();
        while (rs.next()) {
            databases.add(rs.getString(1));
        }
        return databases;
    }

    public List<String> tab_list(String dbName) throws SQLException {
        connection.setCatalog(dbName);
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery("SHOW TABLES");
        List<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString(1));
        }
        return tables;
    }

    public static void main(String... argv) {
        MyJDBC jdbc;
        try {
            jdbc = new MyJDBC("localhost", "", "manga", "manga");
            jdbc.db_list().forEach(System.out::println);
            jdbc.tab_list("manga").forEach(System.out::println);
            System.exit(1);

            jdbc = new MyJDBC("localhost", "manga", "manga", "manga");
            Connection connection = jdbc.get_connection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from fruit");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("name"));
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
