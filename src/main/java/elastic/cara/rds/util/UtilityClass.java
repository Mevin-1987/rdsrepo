package elastic.cara.rds.util;

import com.generiscorp.cara.model.query.CaraQueryRequest;
import com.generiscorp.cara.model.query.CaraQueryResult;
import com.generiscorp.cara.rest.client.CaraClient;
import com.generiscorp.cara.rest.client.exception.CaraRESTException;
import elastic.cara.rds.config.RdsConfiguration;
import elastic.cara.rds.config.RdsDtConfiguration;
import elastic.cara.rds.mapping.RdsMappingConfiguration;
import elastic.cara.rds.repository.QueryExecutor;
import elastic.cara.rds.service.QueryCreator;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

@Component
@Slf4j
public class UtilityClass {

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


    @Autowired
    QueryCreationUtility queryCreationUtility;

    @Autowired
    LoggingUtility loggingUtility;

    @Autowired
    MonitoringUtility monitoringUtility;



    private static final Logger auditLogger = LoggerFactory.getLogger("AUDIT");


//    public void updateCaraResponseInRdsNew(){
//        String url  = rdsConfiguration.getCaraurl();
//        int limit = rdsConfiguration.getCaraqueryrequestlimit();
//
//        log.info("Cara Url=>"+url);
//        log.debug("limit=>"+limit);
//        LocalDateTime tempLastUpdatedDateTime = rdsDtConfiguration.getLastUpdatedDT();
//        LocalDateTime startTime = LocalDateTime.now();
//        try (CaraClient CARA = CaraClient.builder().url(url).connect()) {
//            String encToken = rdsConfiguration.getToken();
//            String secret = rdsConfiguration.getSecret();
//            SecretKey secretKey = decodeEncodedKey(secret);
//            String token = decryptToken(encToken,secretKey);
//            CARA.loginWithToken(token);
//            Set<String> aliasList = rdsMappingConfiguration.getAliasList();
//            for(String alias : aliasList) {
//                log.info("Alias=>"+ alias);
//                List<String> tableList = rdsMappingConfiguration.getTablenameList(alias);
//                String caraAuditQuery = queryCreator.getCaraAuditQuery(alias);
//                log.info("caraAuditQuery: "+caraAuditQuery);
//                CaraQueryResult auditQueryResult = null;
//                try {
//
//                    CaraQueryRequest a  = new CaraQueryRequest();
//                    a.setQuery(caraAuditQuery);
//                    a.setLimit(limit);
//                    auditQueryResult = CARA.tools().executeQuery(a);
//                    log.debug("caraAuditQueryResult.count()=>"+auditQueryResult.getTotalCount());
//                } catch (CaraRESTException e) {
//                    monitoringUtility.incrementErrorCount();
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
//                        CaraQueryRequest a  = new CaraQueryRequest();
//                        a.setQuery(caraQuery);
//                        a.setLimit(limit);
//                        queryResult = CARA.tools().executeQuery(a);
//                        log.debug("caraQueryResult.count()=>"+queryResult.getTotalCount());
//                    } catch (CaraRESTException e) {
//                        monitoringUtility.incrementErrorCount();
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
//            monitoringUtility.incrementErrorCount();
//            rdsDtConfiguration.setLastUpdatedDT(tempLastUpdatedDateTime);
//            log.error("Exception in UtilityClass ", e);
//            e.printStackTrace();
//        }
//        auditLogger.info("Total number of insert queries created for _R table =>"+monitoringUtility.getNoOfInsertQueryInRTable());
//        auditLogger.info("Total number of insert queries created for Object_R table =>"+monitoringUtility.getNoOfInsertQueryInObjectRTable());
//        auditLogger.info("Total number of update queries created for Regular table =>"+monitoringUtility.getNoOfUpdateQueryInRegTable());
//        auditLogger.info("Total number of update queries created for Object table =>"+monitoringUtility.getNoOfUpdateQueryInObjectTable());
//        auditLogger.info("Total number of insert queries created for Regular table =>"+monitoringUtility.getNoOfInsertQueryInRegTable());
//        auditLogger.info("Total number of insert queries created for Object table =>"+monitoringUtility.getNoOfInsertQueryInObjectTable());
//        auditLogger.info("Total number of delete queries executed =>"+monitoringUtility.getNoOfDeleteQueryExecuted());
//        //auditLogger.info("Total number of queries executed =>"+monitoringUtility.getTotalQueriesExecuted());
//        auditLogger.info("Total number of Errors : "+ monitoringUtility.getErrorCount());
//        if(monitoringUtility.getErrorCount().get()>0){
//            rdsDtConfiguration.setLastUpdatedDT(tempLastUpdatedDateTime);
//        }
//    }

    public void updateCaraResponseInRds(){
        String url  = rdsConfiguration.getCaraurl();
        int limit = rdsConfiguration.getCaraqueryrequestlimit();

        log.info("Cara Url=>"+url);
        log.debug("limit=>"+limit);
        LocalDateTime tempLastUpdatedDateTime = rdsDtConfiguration.getLastUpdatedDT();
        LocalDateTime startTime = LocalDateTime.now();
        try (CaraClient CARA = CaraClient.builder().url(url).connect()) {
            String encToken = rdsConfiguration.getToken();
            String secret = rdsConfiguration.getSecret();
            SecretKey secretKey = decodeEncodedKey(secret);
            String token = decryptToken(encToken,secretKey);
            CARA.loginWithToken(token);
            Set<String> aliasList = rdsMappingConfiguration.getAliasList();
            for(String alias : aliasList) {
                log.info("Alias=>" + alias);
                List<String> tableList = rdsMappingConfiguration.getTablenameList(alias);
                boolean isObjReqPop = rdsMappingConfiguration.isObjectReqPop(alias);
                queryCreationUtility.setTableColumnSizeMap(tableList, isObjReqPop);
                String tableName = tableList.get(1);
                if(tableName.endsWith("_R")){
                    tableName = tableName.substring(0, tableName.length() - 2);
                }
                boolean isFlush = rdsMappingConfiguration.isFlush(alias);
                if(isFlush){
                    queryCreator.performFlushOperation(tableName, alias);
                }else{
                    String caraAuditQuery = queryCreator.getCaraAuditQuery(alias);
                    log.debug("caraAuditQuery: "+caraAuditQuery);
                    CaraQueryResult auditQueryResult = null;
                    try {

                        CaraQueryRequest a  = new CaraQueryRequest();
                        a.setQuery(caraAuditQuery);
                        a.setLimit(limit);
                        auditQueryResult = CARA.tools().executeQuery(a);
                        log.info("alias=>"+alias+" caraAuditQueryResult.count()=>"+auditQueryResult.getTotalCount());
                    } catch (CaraRESTException e) {
                        monitoringUtility.incrementErrorCount();
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    queryCreator.deleteAuditQueryResult(auditQueryResult, tableName, alias);
                }
                String caraQuery = queryCreator.getCaraQuery(alias, isFlush);

                log.debug("caraQuery=> "+caraQuery);
                CaraQueryResult queryResult = null;
                try {
                    CaraQueryRequest a  = new CaraQueryRequest();
                    a.setQuery(caraQuery);
                    a.setLimit(limit);
                    queryResult = CARA.tools().executeQuery(a);
                    rdsDtConfiguration.setLastUpdatedDT(LocalDateTime.now());
                    log.info("Alias=>"+alias+" caraQueryResult.count() =>"+queryResult.getTotalCount());
                } catch (CaraRESTException e) {
                    monitoringUtility.incrementErrorCount();
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                String RepeatingTable = tableName+"_R";
                String RegularTable = tableName;
                List<String> objectFieldList = null;
                List<String> object_rFieldList = null;
                List<String> regularTableFieldList = rdsMappingConfiguration.getFieldList(alias, RegularTable);
                List<String> repeatTableFieldList = rdsMappingConfiguration.getFieldList(alias, RepeatingTable);
                if(isObjReqPop){
                    objectFieldList = rdsMappingConfiguration.getObjectTableFieldList();
                    object_rFieldList = rdsMappingConfiguration.getObject_RTableFieldList();
                    queryCreator.createOracleQuery(queryResult, alias, tableName, objectFieldList, object_rFieldList,
                            regularTableFieldList, repeatTableFieldList);
                }else {

                    queryCreator.createOracleQuery(queryResult, alias, tableName, objectFieldList, object_rFieldList,
                            regularTableFieldList, repeatTableFieldList);
                }

            }
            LocalDateTime endTime = LocalDateTime.now();

            Duration duration = Duration.between(startTime, endTime);
            long hours = duration.toHours();
            long minutes = duration.toMinutes();
            long seconds = duration.getSeconds();
            auditLogger.info("Total Execution Time=>"+hours+":"+minutes+":"+seconds);



        } catch (Exception e) {
            monitoringUtility.incrementErrorCount();
            rdsDtConfiguration.setLastUpdatedDT(tempLastUpdatedDateTime);
            log.error("Exception in UtilityClass ", e);
            e.printStackTrace();
        }
        Map<String, MonitoringUtility> monitoringUtilityByTable = loggingUtility.getMonitoringUtilityByTable();
        for(String table : monitoringUtilityByTable.keySet()){
            MonitoringUtility monitoringUtility = monitoringUtilityByTable.get(table);
            if(table.endsWith("_R")){
                auditLogger.info(table +" Insert Query "+monitoringUtility.getInsertQuery());
                auditLogger.info(table +" Delete Query "+monitoringUtility.getDeleteQuery());
            }else{
                auditLogger.info(table +" Insert Query "+monitoringUtility.getInsertQuery());
                auditLogger.info(table +" Update Query "+monitoringUtility.getUpdateQuery());
                auditLogger.info(table +" Delete Query "+monitoringUtility.getDeleteQuery());
            }

        }

        //auditLogger.info("Total number of queries executed =>"+monitoringUtility.getTotalQueriesExecuted());
        auditLogger.info("Total number of Errors : "+ monitoringUtility.getErrorCount());
        if(monitoringUtility.getErrorCount().get()>0){
            rdsDtConfiguration.setLastUpdatedDT(tempLastUpdatedDateTime);
        }

    }


    public static String decryptToken(String encryptedText, SecretKey key) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] decodedBytes = Base64.getDecoder().decode(encryptedText);
        byte[] decryptedBytes = cipher.doFinal(decodedBytes);
        return new String(decryptedBytes);
    }

    public static SecretKey decodeEncodedKey(String keyStr) {
        byte[] decodedKey = Base64.getDecoder().decode(keyStr);
        return new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");
    }


}
