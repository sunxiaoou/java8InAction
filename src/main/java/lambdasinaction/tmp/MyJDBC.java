package lambdasinaction.tmp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MyJDBC {
    public static void main(String... argv) {
        try {
            // String url = "jdbc:mysql://localhost:3306/manga";
            String url = "jdbc:mysql://192.168.55.11:3306/manga";
            Connection connection = DriverManager.getConnection(url, "manga", "manga");
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from fruit");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("name"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
