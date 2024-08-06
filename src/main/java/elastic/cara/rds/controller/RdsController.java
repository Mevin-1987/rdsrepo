package elastic.cara.rds.controller;

import com.generiscorp.cara.model.query.CaraQueryResult;
import com.generiscorp.cara.rest.client.CaraClient;
import com.generiscorp.cara.rest.client.exception.CaraRESTException;
import elastic.cara.rds.config.RdsConfiguration;
import elastic.cara.rds.config.RdsDtConfiguration;
import elastic.cara.rds.mapping.RdsMappingConfiguration;
import elastic.cara.rds.repository.QueryExecutor;
import elastic.cara.rds.service.QueryCreator;
import elastic.cara.rds.util.MonitoringUtility;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@Slf4j
public class RdsController {

    @Autowired
    RdsMappingConfiguration rdsMappingConfiguration;

    @Autowired
    RdsConfiguration rdsConfiguration;

    @Autowired
    QueryCreator queryCreator;

    @Autowired
    QueryExecutor queryExecutor;

    @Autowired
    RdsDtConfiguration rdsDtConfiguration;

    private static String ADMIN_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJWSUtSQVBYfHByYXNoYW50aC52aWtyYW0iLCJ0IjoicyIsImV4cCI6MTc0ODY2NzYwMCwiY3JlYXRlZCI6IjIwMjQtMDUtMDhUMTU6NDg6MjIuOTM5NTA3NTUwWiJ9.sNXhZlp7LMvXc5-CdrfUOuWXdA8FnnyCY8oTnV7xy4s";

    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");

//    @GetMapping("/invoke-cara-rest-client")
//    public void updateCaraResponseInRds(){
//        String url  = rdsConfiguration.getCaraurl();
//        log.info("Cara Url=>"+url);
//        LocalDateTime tempLastUpdatedDateTime = rdsDtConfiguration.getLastUpdatedDT();
//        LocalDateTime startTime = LocalDateTime.now();
//        try (CaraClient CARA = CaraClient.builder().url(url).connect()) {
//            CARA.loginWithToken(ADMIN_TOKEN);
//            Set<String> aliasList = rdsMappingConfiguration.getAliasList();
//            for(String alias : aliasList) {
//                log.info("Alias=>"+ alias);
//                List<String> tableList = rdsMappingConfiguration.getTablenameList(alias);
//                String caraAuditQuery = queryCreator.getCaraAuditQuery(alias);
//                log.info("caraAuditQuery: "+caraAuditQuery);
//                CaraQueryResult auditQueryResult = null;
//                try {
//                    auditQueryResult = CARA.tools().executeQuery(caraAuditQuery);
//                } catch (CaraRESTException e) {
//                    MonitoringUtility.incrementErrorCount();
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//                queryCreator.deleteAuditQueryResult(auditQueryResult, tableList, alias);
//                tableList.forEach(tableName -> {
//                    List<String> fieldList = rdsMappingConfiguration.getFieldList(alias, tableName);
//                    String caraQuery = queryCreator.getCaraQuery(alias, tableName, fieldList);
//                    log.info("caraQuery=> "+caraQuery);
//                    //Run a query
//                    CaraQueryResult queryResult = null;
//                    try {
//                        queryResult = CARA.tools().executeQuery(caraQuery);
//                    } catch (CaraRESTException e) {
//                        MonitoringUtility.incrementErrorCount();
//                        e.printStackTrace();
//                        throw new RuntimeException(e);
//                    }
//                    queryCreator.createOracleQuery(queryResult, alias, tableName, fieldList);
////                    Map<String, Map<String, String>> oraQueryList = queryCreator.getOracleQuery(queryResult, alias, tableName, fieldList);
////                    oraQueryList.forEach((query,paramMap)  -> {
////                        log.debug("Oracle Query=>"+query);
////                        int updateCount = queryExecutor.executeUpdate(query,paramMap);
////                        if (updateCount>0)
////                            log.info(query + " is processed successfully. Update Count => "+updateCount);
////                        else
////                            log.info(query + " processing failed. Update Count => "+updateCount);
////                    });
//
//                });
//            }
//            LocalDateTime endTime = LocalDateTime.now();
//
//            Duration duration = Duration.between(startTime, endTime);
//            long hours = duration.toHours();
//            long minutes = duration.toMinutes();
//            long seconds = duration.getSeconds();
//            auditLogger.info("Total Execution Time=>"+hours+":"+minutes+":"+seconds);
//
//            rdsDtConfiguration.setLastUpdatedDT(LocalDateTime.now());
//        } catch (Exception e) {
//            MonitoringUtility.incrementErrorCount();
//            rdsDtConfiguration.setLastUpdatedDT(tempLastUpdatedDateTime);
//            log.error("Exception in RdsController ", e);
//            e.printStackTrace();
//        }
//        auditLogger.info("Total number of Insert queries in Regular Table is "+ queryCreator.getNoOfInsertQueryInRegtable());
//        auditLogger.info("Total number of Insert queried in _R Table is "+queryCreator.getNoOfInsertQueryIn_Rtable());
//        auditLogger.info("Total Number of Update queries in Regular table is "+queryCreator.getNoOfUpdateQueryInRegTable());
//        auditLogger.info("Total numberr of queries Executed is "+queryExecutor.getTotalQueriesExecuted());
//        auditLogger.info("Total number of Errors : "+ MonitoringUtility.getErrorCount());
//
//    }



//    @GetMapping("/aliaslist")
//    public void getAliasList() {
//        Set<String> aliasList = rdsMappingConfiguration.getAliasList();
//        for(String alias : aliasList) {
//            log.info("alias=>"+alias);
//        }
//    }
//
//    @GetMapping("/tablelist")
//    public void getTableList(){
//        Set<String> aliasList = rdsMappingConfiguration.getAliasList();
//        for(String alias : aliasList) {
//            log.info("Alias=>"+ alias);
//            List<String> tableList = rdsMappingConfiguration.getTablenameList(alias);
//            tableList.forEach(tableName -> {
//                log.info("tableName=>"+tableName);
//            });
//        }
//    }
//
//    @GetMapping("/fieldlist")
//    public void getFieldList(){
//        Set<String> aliasList = rdsMappingConfiguration.getAliasList();
//        for(String alias : aliasList) {
//            log.info("Alias=>"+ alias);
//            List<String> tableList = rdsMappingConfiguration.getTablenameList(alias);
//            tableList.forEach(tableName -> {
//                log.info("\nFields of "+tableName+" are given below:");
//                List<String> fieldList = rdsMappingConfiguration.getFieldList(alias, tableName);
//                fieldList.forEach(field -> {
//                    log.info("\nField=>"+field);
//
//                });
//            });
//        }
//    }
//
//    @GetMapping("/fieldMappingConfig")
//    public void getFieldMapopingConfig(){
//        Set<String> aliasList = rdsMappingConfiguration.getAliasList();
//        for(String alias : aliasList) {
//            log.info("Alias=>"+ alias);
//            List<String> tableList = rdsMappingConfiguration.getTablenameList(alias);
//            tableList.forEach(tableName -> {
//                log.info("\nFields of "+tableName+" are given below:");
//                List<String> fieldList = rdsMappingConfiguration.getFieldList(alias, tableName);
//                fieldList.forEach(field -> {
//                    log.info("\nMapping configuration of Field=>"+field+" is given below:");
//                    log.info("\nField Type: "+rdsMappingConfiguration.getFieldType(alias,tableName,field));
//                    log.info("\nColumn Name: "+rdsMappingConfiguration.getColumnName(alias,tableName,field));
//                    log.info("\nColumn Type: "+rdsMappingConfiguration.getColumnType(alias,tableName,field));
//
//                });
//            });
//        }
//    }
//
//    @GetMapping("/fieldlist-with-fieldtype")
//    public void getFieldListWithFieldType(){
//        String url  = rdsConfiguration.getCaraurl();
//        log.info("Cara Url=>"+url);
//        try (CaraClient CARA = CaraClient.builder().url(url).connect()) {
//            CARA.loginWithToken(ADMIN_TOKEN);
//            Set<String> aliasList = rdsMappingConfiguration.getAliasList();
//            for(String alias : aliasList) {
//                log.info("Alias=>"+ alias);
//                List<String> tableList = rdsMappingConfiguration.getTablenameList(alias);
//                tableList.forEach(tableName -> {
//                    List<String> fieldList = rdsMappingConfiguration.getFieldList(alias, tableName);
//                    String caraQuery = queryCreator.getCaraQuery(alias, tableName, fieldList);
//                    log.info("caraQuery=>"+caraQuery);
//                    //Run a query
//                    CaraQueryResult queryResult = null;
//                    try {
//                        queryResult = CARA.tools().executeQuery(caraQuery);
//                    } catch (CaraRESTException e) {
//                        throw new RuntimeException(e);
//                    }
//                    queryResult.getColumns().forEach(c -> log.info("Field=>"+c.getName()+"::::FieldType=>"+c.getType()));
//
//                });
//            }
//        } catch (Exception e) {
//            log.error("Exception in RdsController ", e);
//            e.printStackTrace();
//        }
//
//    }
//
//    @GetMapping("/fieldlist-with-fieldvalue")
//    public void getFieldListWithFieldValue(){
//        String url  = rdsConfiguration.getCaraurl();
//        log.info("Cara Url=>"+url);
//        try (CaraClient CARA = CaraClient.builder().url(url).connect()) {
//            CARA.loginWithToken(ADMIN_TOKEN);
//            Set<String> aliasList = rdsMappingConfiguration.getAliasList();
//            for(String alias : aliasList) {
//                log.info("Alias=>"+ alias);
//                List<String> tableList = rdsMappingConfiguration.getTablenameList(alias);
//                tableList.forEach(tableName -> {
//                    List<String> fieldList = rdsMappingConfiguration.getFieldList(alias, tableName);
//                    String caraQuery = queryCreator.getCaraQuery(alias, tableName, fieldList);
//                    log.info("caraQuery=>"+caraQuery);
//                    //Run a query
//                    CaraQueryResult queryResult = null;
//                    try {
//                        queryResult = CARA.tools().executeQuery(caraQuery);
//                    } catch (CaraRESTException e) {
//                        throw new RuntimeException(e);
//                    }
//                    log.info("tableName=> "+tableName);
//                    HashMap<String , String> colMap = new HashMap<>();
//                    queryResult.getResults().forEach(node->{
//                        int nodeId = 1;
//                        fieldList.forEach(field->{
//                            Map<String, String> fieldConfigurationMap = rdsMappingConfiguration.getFieldConfiguration(alias,
//                                    tableName, field);
//                            String fieldtype = fieldConfigurationMap.get("fieldtype");
//                            String columnName = fieldConfigurationMap.get("columnname");
//                            String columnType = fieldConfigurationMap.get("columntype");
//
//                            if(fieldtype.equalsIgnoreCase("String")){
//                                String tvalue = node.getString(field);
//                                if(tvalue!=null) {
//                                    String[] tvalueArr = tvalue.split(",");
//                                    for (int i = 0; i < tvalueArr.length; i++) {
//                                        String value = tvalueArr[i];
//                                        int length = 0;
//                                        if (value != null)
//                                            length = value.length();
//                                        if (colMap.get(columnName) != null) {
//                                            String  templen = colMap.get(columnName);
//                                            String[] lenArr = templen.split("#");
//                                            int len = Integer.parseInt(lenArr[0]);
//                                            if (len < length) {
//                                                colMap.put(columnName, length+"#"+value);
//                                            }
//                                        } else {
//                                            colMap.put(columnName, length+"#"+value);
//                                        }
//                                    }
//                                }
//
//                                //log.info("field=> "+field+" :: value=> "+value+" :: Value Length=> "+length
//                                        ///+" :: columnName=> "+columnName+" :: columnType=> "+columnType);
//                                //log.info("columnName=> "+columnName+" :: Value Length=> "+length);
//                            }else if(fieldtype.equalsIgnoreCase("Date")){
//                                //log.info("field=> "+field+" :: value=> "+node.getDate(field)
//                                        //+" :: columnName=> "+columnName+" :: columnType=> "+columnType);
//                            }else{
//                                throw new RuntimeException("Invalid fieldType=>"+fieldtype);
//                            }
//                        });
//
//
//                    });
//                    for (Map.Entry<String, String> entry : colMap.entrySet()) {
//                        String templen = entry.getValue();
//                        String[] lenArr = templen.split("#");
//                        log.info("ColumnName: " + entry.getKey() + ":: " +lenArr[0] );
//                        log.info("Value=> "+lenArr[1]);
//                    }
//
//                });
//            }
//        }catch (Exception e) {
//            log.error("Exception in RdsController ", e);
//            e.printStackTrace();
//        }
//    }
//
//    @GetMapping("/config-mapping")
//    public void getMapping() {
//        log.info("RDS Last Updated DateTime: " + rdsConfiguration.getRdslastupdateddatetime()
//                + "\nRDS Cara Mapping File: " + rdsConfiguration.getRdscaramappingfile()
//                + "\nCara URL: " + rdsConfiguration.getCaraurl());
//    }
}
