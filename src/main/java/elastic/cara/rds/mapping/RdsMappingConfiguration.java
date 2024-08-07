package elastic.cara.rds.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import elastic.cara.rds.RdsApplication;
import elastic.cara.rds.config.RdsConfiguration;
import elastic.cara.rds.util.MonitoringUtility;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class RdsMappingConfiguration {

    @Autowired
    RdsConfiguration rdsConfiguration;


    private ConcurrentHashMap<String, JsonNode> rdsMappingDetails = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, JsonNode> rdsObjectMappingDetails = new ConcurrentHashMap<>();

    @Autowired
    MonitoringUtility commonMonitoringUtility;

    @PostConstruct
    public void init() {

        this.rdsMappingDetails = getRdsMappingDetails();
        this.rdsObjectMappingDetails = getRdsObjectMappingDetails();
    }

    public ConcurrentHashMap<String, JsonNode> getRdsMappingDetails(){
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String mappingFilepath = rdsConfiguration.getRdscaramappingfile();
            log.info("Current Working Directory=>"+System.getProperty("user.dir"));
            log.info("Mapping File Path => "+mappingFilepath);
            File file = new File(mappingFilepath);
            if(!file.exists()){
                String jarDir = new File(RdsApplication.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
                mappingFilepath = jarDir + File.separator + mappingFilepath;
                file = new File(mappingFilepath);
            }
            JsonNode jsonNodet = objectMapper.readTree(file);
            JsonNode jsonNode = jsonNodet.get("alias");
            for (Iterator<String> it = jsonNode.fieldNames(); it.hasNext(); ) {
                String alias = it.next();
                log.info("Alias added to Map=>"+alias);
                this.rdsMappingDetails.put(alias, jsonNode.get(alias));
            }
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getRdsMappingDetails method", e);
        }
        return rdsMappingDetails;
    }

    public Set<String> getAliasList(){
        return this.rdsMappingDetails.keySet();
    }

    public List<String> getTablenameList(String alias){
        List<String> tableNameList = new ArrayList<>();
        try {
            JsonNode aliasNode = rdsMappingDetails.get(alias);
            JsonNode tablenamesNode = aliasNode.get("tables");
            for (Iterator<String> it = tablenamesNode.fieldNames(); it.hasNext(); ) {
                String tableName = it.next();
                log.info("Tablename added to List=>" + tableName);
                tableNameList.add(tableName);
            }
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getTablenameList method", e);
        }
        return  tableNameList;
    }

    public boolean isFlush(String alias){
        JsonNode aliasNode = rdsMappingDetails.get(alias);
        return aliasNode.get("flush").booleanValue();
    }
    public boolean isObjectReqPop(String alias){
        JsonNode aliasNode = rdsMappingDetails.get(alias);
        return aliasNode.get("isObjectReqPop").booleanValue();
    }

    public List<String> getAllFieldList(String alias){
        List<String> fieldList = new ArrayList<>();
        try {
            JsonNode aliasNode = rdsMappingDetails.get(alias);
            JsonNode tablenamesNode = aliasNode.get("tables");
            for (Iterator<String> it = tablenamesNode.fieldNames(); it.hasNext(); ) {
                String tableName = it.next();
                JsonNode tableNode = tablenamesNode.get(tableName);
                JsonNode fieldsNode = tableNode.get("fields");
                for (Iterator<String> it1 = fieldsNode.fieldNames(); it1.hasNext(); ) {
                    String field = it1.next();
                    log.debug("field added to List=>" + field);
                    fieldList.add(field);
                }
            }
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getAllFieldList method", e);
        }
        return fieldList;
    }

    public List<String> getFieldList(String alias, String tableName){
        List<String> fieldList = new ArrayList<>();
        try {
            JsonNode aliasNode = rdsMappingDetails.get(alias);
            JsonNode tablenamesNode = aliasNode.get("tables");
            JsonNode tableNode = tablenamesNode.get(tableName);
            JsonNode fieldsNode = tableNode.get("fields");
            for (Iterator<String> it = fieldsNode.fieldNames(); it.hasNext(); ) {
                String field = it.next();
                log.debug("field added to List=>" + field);
                fieldList.add(field);
            }
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getFieldList method", e);
        }
        return fieldList;
    }

    public String getFieldType(String alias, String tablename, String field){
        JsonNode aliasNode = rdsMappingDetails.get(alias);
        JsonNode tablenamesNode = aliasNode.get("tables");
        JsonNode tableNode = tablenamesNode.get(tablename);
        JsonNode fieldsNode = tableNode.get("fields");
        JsonNode fieldNode = fieldsNode.get(field);
        return fieldNode.get("fieldtype").toString();
    }

    public String getColumnName(String alias, String tablename, String field){
        JsonNode aliasNode = rdsMappingDetails.get(alias);
        JsonNode tablenamesNode = aliasNode.get("tables");
        JsonNode tableNode = tablenamesNode.get(tablename);
        JsonNode fieldsNode = tableNode.get("fields");
        JsonNode fieldNode = fieldsNode.get(field);
        return fieldNode.get("columnname").toString();
    }

    public String getColumnType(String alias, String tablename, String field){
        JsonNode aliasNode = rdsMappingDetails.get(alias);
        JsonNode tablenamesNode = aliasNode.get("tables");
        JsonNode tableNode = tablenamesNode.get(tablename);
        JsonNode fieldsNode = tableNode.get("fields");
        JsonNode fieldNode = fieldsNode.get(field);
        return fieldNode.get("columntype").toString();
    }

    public Map<String, String> getFieldConfiguration(String alias,
                                                 String tablename, String field){
        Map<String, String> fieldConfigMap = new HashMap<>();
        try {
            JsonNode aliasNode = rdsMappingDetails.get(alias);
            JsonNode tablenamesNode = aliasNode.get("tables");
            JsonNode tableNode = tablenamesNode.get(tablename);
            JsonNode fieldsNode = tableNode.get("fields");
            JsonNode fieldNode = fieldsNode.get(field);
            fieldConfigMap.put("fieldtype", fieldNode.get("fieldtype").textValue());
            fieldConfigMap.put("columnname", fieldNode.get("columnname").textValue());
            fieldConfigMap.put("columntype", fieldNode.get("columntype").textValue());
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getFieldConfiguration method", e);
        }
        return fieldConfigMap;
    }

    public ConcurrentHashMap<String, JsonNode> getRdsObjectMappingDetails(){
        ObjectMapper objectMapper = new ObjectMapper();
        try{
            String objectMappingFilepath = rdsConfiguration.getRdscaraobjectmappingfile();
            //log.info("Current Working Directory=>"+System.getProperty("user.dir"));
            log.info("Object Mapping File Path => "+objectMappingFilepath);
            File file = new File(objectMappingFilepath);
            if(!file.exists()){
                String jarDir = new File(RdsApplication.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
                objectMappingFilepath = jarDir + File.separator + objectMappingFilepath;
                file = new File(objectMappingFilepath);
            }
            JsonNode jsonNode = objectMapper.readTree(file);
            JsonNode tablenamesNode = jsonNode.get("tables");
            for (Iterator<String> it = tablenamesNode.fieldNames(); it.hasNext(); ) {
                String tableName = it.next();
                JsonNode tableNode = tablenamesNode.get(tableName);
                JsonNode fieldsNode = tableNode.get("fields");
                log.info("Tablename added to Object Mapping Details=>" + tableName);
                rdsObjectMappingDetails.put(tableName,fieldsNode);
            }

        }catch (Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getRdsObjectMappingDetails method", e);
        }
        return rdsObjectMappingDetails;
    }

    public List<String> getObjectTableFieldList(){
        List<String> fieldList = new ArrayList<>();
        try {
            JsonNode fieldsNode = rdsObjectMappingDetails.get("OBJECT");
            fieldList = getObjectTableFieldList(fieldsNode);
        }catch (Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getObjectTableFieldList method", e);
        }
        return fieldList;
    }

    public List<String> getObject_RTableFieldList(){
        List<String> fieldList = new ArrayList<>();
        try {
            JsonNode fieldsNode = rdsObjectMappingDetails.get("OBJECT_R");
            fieldList = getObjectTableFieldList(fieldsNode);
        }catch (Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getObjectTableFieldList method", e);
        }
        return fieldList;
    }

    public List<String> getObjectTableFieldList(JsonNode fieldsNode){
        List<String> fieldList = new ArrayList<>();
        try{

            for (Iterator<String> it = fieldsNode.fieldNames(); it.hasNext(); ) {
                String field = it.next();
                log.debug("object field added to List=>" + field);
                fieldList.add(field);
            }
        }catch (Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getObjectTableFieldList method", e);
        }
        return fieldList;
    }

    public Map<String, String> getObjectTableFieldConfiguration(JsonNode fieldsNode, String field){
        Map<String, String> fieldConfigMap = new HashMap<>();
        try{
            JsonNode fieldNode = fieldsNode.get(field);
            fieldConfigMap.put("columnname", fieldNode.get("columnname").textValue());
            fieldConfigMap.put("columntype", fieldNode.get("columntype").textValue());
        }catch (Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getObjectTableFieldConfiguration method", e);
        }
        return fieldConfigMap;
    }

    public Map<String, String> getObjectTableFieldConfiguration(String field){
        Map<String, String> fieldConfigMap = new HashMap<>();
        try{
            JsonNode fieldsNode = rdsObjectMappingDetails.get("OBJECT");
            fieldConfigMap = getObjectTableFieldConfiguration(fieldsNode,field);
        }catch (Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getObjectTableFieldConfiguration method", e);
        }
        return fieldConfigMap;
    }

    public Map<String, String> getObject_RTableFieldConfiguration(String field){
        Map<String, String> fieldConfigMap = new HashMap<>();
        try{
            JsonNode fieldsNode = rdsObjectMappingDetails.get("OBJECT_R");
            fieldConfigMap = getObjectTableFieldConfiguration(fieldsNode,field);
        }catch (Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception will executing getObject_RTableFieldConfiguration method", e);
        }
        return fieldConfigMap;
    }
}
