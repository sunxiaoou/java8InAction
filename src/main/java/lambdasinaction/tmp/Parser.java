package lambdasinaction.tmp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Parser {
    public static void main(String[] args) throws IOException, SQLException {
        String path = "target/classes/lambdasinaction/tmp/schemaMap_policy.json";
        BufferedReader br = new BufferedReader(new FileReader(path));
        String jsonString = br.lines().collect(Collectors.joining("\n"));

        JSONObject jsonObject = JSON.parseObject(jsonString);
        boolean entireDbSync = jsonObject.getBoolean("entireDbSync");
        Map<String, String> schemaMap =
                JSON.parseObject(jsonObject.getJSONObject("schemaMap").toJSONString(), Map.class);
        List<String> schemas = new ArrayList<>(schemaMap.keySet());
        JSONArray tableMaps = jsonObject.getJSONArray("tableMap");

        List<String> tables = new ArrayList<>();
        for (Object tableMap: tableMaps) {
            String table = ((JSONObject)tableMap).getString("srcTable");
            String schema = ((JSONObject)tableMap).getString("srcSchema");
            tables.add(schema + "." + table);
        }
        System.out.println(entireDbSync);
        System.out.println(schemas);
        System.out.println(tables);

        MyJDBC jdbc = new MyJDBC("localhost", "", "manga", "manga");
        List<String> tables2Sync = new ArrayList<>();
        List<String> schemaList = jdbc.schemaList();
        List<String> unknownSchema = new ArrayList<>();
        List<String> unknownTables = new ArrayList<>();

        List<String> exclusive = new ArrayList<>(Arrays.asList("information_schema", "metastore"));


        for (String schema: schemaList) {
            if (! exclusive.contains(schema)) {
                List<String> tabList = jdbc.tabList(schema);
                for (String tab : tabList) {
                    String table = schema + "." + tab;
                    if (entireDbSync || schemas.contains(schema) || tables.contains(table)) {
                        tables2Sync.add(table);
                    }
                }
            }
        }
        System.out.println(tables2Sync);
        jdbc.close();
    }
}
