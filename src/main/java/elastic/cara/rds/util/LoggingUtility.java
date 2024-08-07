package elastic.cara.rds.util;

import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Locale;

@Component
public class LoggingUtility {

    @Getter
    private HashMap<String, MonitoringUtility> monitoringUtilityByTable = new HashMap<>();

    public void setMonitoringUtility(String tableName, MonitoringUtility monitoringUtility){
        monitoringUtilityByTable.put(tableName.toLowerCase(), monitoringUtility);
    }

    public MonitoringUtility getMonitoringUtility(String tableName){
        if(monitoringUtilityByTable.containsKey(tableName.toLowerCase())){
            return monitoringUtilityByTable.get(tableName.toLowerCase());
        }else{
            return new MonitoringUtility();
        }
    }
}
