package elastic.cara.rds.util;

import elastic.cara.rds.repository.QueryExecutor;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class QueryCreationUtility {

    private static Map<String, Map<String, Integer>> tableMap = new HashMap<>();

    @Autowired
    QueryExecutor queryExecutor;

    public static String safeTrim(String input, String columnName, String tableName) {

        try {
            Map<String, Integer> sizeMap = tableMap.get(tableName);
            int size = sizeMap.get(columnName);
            if((size > 2) && (input.trim().length()>size)){
                input = input.substring(0,(size-2)).concat("..");
            }
            return input;
        } catch (NullPointerException e) {
            return null;
        }
    }

    public void setTableColumnSizeMap(List<String> tables, boolean isObjReqPop){
        if(isObjReqPop){
            tables.add("OBJECT");
            tables.add("OBJECT_R");
        }
        for(String table:tables){
            Map<String, Integer> columnSizeMap = queryExecutor.getColumnSizeMap(table);
            tableMap.put(table, columnSizeMap);

        }
    }
}
