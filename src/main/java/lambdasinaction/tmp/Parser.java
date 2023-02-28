package lambdasinaction.tmp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;


public class Parser {
    private final JSONObject obj;

    public Parser(String path) throws FileNotFoundException {
        BufferedReader br = new BufferedReader(new FileReader(path));
        String jsonString = br.lines().collect(Collectors.joining("\n"));
        obj = JSON.parseObject(jsonString);
    }

    public Boolean isEntire() {
        return obj.getBoolean("entireDbSync");
    }

    public List<String> getSchemas() {
        Map<String, String> schemaMap =
                JSON.parseObject(obj.getJSONObject("schemaMap").toJSONString(), Map.class);
        return new ArrayList<>(schemaMap.keySet());
    }

    public List<String> getTables() {
        JSONArray tableMaps = obj.getJSONArray("tableMap");
        List<String> tables = new ArrayList<>();
        for (Object tableMap: tableMaps) {
            String table = ((JSONObject)tableMap).getString("srcTable");
            String schema = ((JSONObject)tableMap).getString("srcSchema");
            tables.add(schema + "." + table);
        }
        return tables;
    }

    public static void main(String[] args) throws IOException, SQLException {
        String path = "target/classes/lambdasinaction/tmp/schemaMap_policy.json";
        Parser parser = new Parser(path);

        List<String> jsonSchemas = parser.getSchemas();
        List<String> jsonTables = parser.getTables();
        System.out.println("entireDbSync - " + parser.isEntire());
        System.out.println("jsonSchemas - " + jsonSchemas);
        System.out.println("jsonTables - " + jsonTables);

        MyJDBC db = new MyJDBC("localhost", "", "manga", "manga");
        List<String> dbSchemas = db.schemaList();
        System.out.println("dbSchemas - " + dbSchemas);
        List<String> exclusive = new ArrayList<>(Arrays.asList("information_schema", "metastore", "mysql"));

        Set<String> schemaSet = new HashSet<>();
        List<String> tables = new ArrayList<>();
        for (String schema: dbSchemas) {
            if (! exclusive.contains(schema)) {
                List<String> dbTables = db.tabList(schema);
                for (String tab : dbTables) {
                    schemaSet.add(schema);
                    String table = schema + "." + tab;
                    if (parser.isEntire() || jsonSchemas.contains(schema) || jsonTables.contains(table)) {
                        tables.add(schema + "." + tab);
                    }
                }
            }
        }
        List<String> schemas = new ArrayList<>(schemaSet);
        System.out.println("schemas - " + schemas);
        System.out.println("tables - " + tables);
        List<String> unknownSchema = jsonSchemas.stream().filter(s -> ! schemas.contains(s)).collect(Collectors.toList());
        List<String> unknownTables = jsonTables.stream().filter(s -> ! tables.contains(s)).collect(Collectors.toList());
        System.out.println("unknownSchema - " + unknownSchema);
        System.out.println("unknownTables - " + unknownTables);

        db.close();
    }
}
