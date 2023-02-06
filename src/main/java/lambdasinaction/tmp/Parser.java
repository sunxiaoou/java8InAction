package lambdasinaction.tmp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Parser {
    public static void main(String[] args) throws IOException {
        String path = "target/classes/lambdasinaction/tmp/schemaMap_policy.json";
        BufferedReader br = new BufferedReader(new FileReader(path));
        String jsonString = br.lines().collect(Collectors.joining("\n"));

        JSONObject jsonObject = JSON.parseObject(jsonString);
        boolean entireDbSync = jsonObject.getBoolean("entireDbSync");
        Map<String, String> schemaMap =
                JSON.parseObject(jsonObject.getJSONObject("schemaMap").toJSONString(), Map.class);
        List<String> schemas = new ArrayList<>(schemaMap.keySet());
        JSONArray tableMaps = jsonObject.getJSONArray("tableMap");

        Map<String, String> tables = new HashMap<>();
        for (Object tableMap: tableMaps) {
            String table = ((JSONObject)tableMap).getString("srcTable");
            String schema = ((JSONObject)tableMap).getString("srcSchema");
            tables.put(table, schema);
        }
        System.out.println(entireDbSync);
        System.out.println(schemas);
        System.out.println(tables);
    }
}
