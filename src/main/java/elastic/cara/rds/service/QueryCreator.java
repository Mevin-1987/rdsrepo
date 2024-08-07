package elastic.cara.rds.service;

import com.generiscorp.cara.model.data.CaraProperties;
import com.generiscorp.cara.model.query.CaraQueryResult;
import elastic.cara.rds.config.RdsConfiguration;
import elastic.cara.rds.config.RdsDtConfiguration;
import elastic.cara.rds.mapping.RdsMappingConfiguration;
import elastic.cara.rds.repository.QueryExecutor;
import elastic.cara.rds.util.LoggingUtility;
import elastic.cara.rds.util.MonitoringUtility;
import elastic.cara.rds.util.QueryCreationUtility;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
public class QueryCreator {

    @Autowired
    RdsMappingConfiguration rdsMappingConfiguration;

    @Autowired
    RdsConfiguration rdsConfiguration;

    @Autowired
    RdsDtConfiguration rdsDtConfiguration;

    @Autowired
    QueryExecutor queryExecutor;

    @Autowired
    MonitoringUtility commonMonitorUtility;

    @Autowired
    LoggingUtility loggingUtility;


    public String getCaraAuditQuery(String alias){
        String query="";
        try{
            log.info("Generating cara audit trail query for " + alias);
            query = "select root_version_id from cara_audit_trail where event = '_delete'" +
                    " and type_name = '"+ alias + "' and time_stamp = '"+rdsDtConfiguration.getLastUpdatedDT()+"'";
        }catch (Exception e){
            commonMonitorUtility.incrementErrorCount();
            log.error("Exception in getCaraAuditQuery method ", e);
            e.printStackTrace();
        }
        return query;
    }

    public void deleteAuditQueryResult(CaraQueryResult auditQueryResult,
                                       String table, String alias){
        List<String> rootVersionIds = new ArrayList<>();
        auditQueryResult.getResults().forEach(node->{
            String rootVersionId = node.getString("root_version_id");
            rootVersionIds.add(rootVersionId);
        });
        if(rdsMappingConfiguration.isObjectReqPop(alias)) {
            String deleteQuery = "DELETE FROM OBJECT_R WHERE ROOT_VERSION_ID = :value";
            queryExecutor.deleteRootversionIdsFromTable(rootVersionIds, deleteQuery, "OBJECT_R");
            deleteQuery = "DELETE FROM OBJECT WHERE ROOT_VERSION_ID = :value";
            queryExecutor.deleteRootversionIdsFromTable(rootVersionIds, deleteQuery, "OBJECT");
        }

        String query = "DELETE FROM "+table+"_R WHERE ROOT_VERSION_ID = :value";
        queryExecutor.deleteRootversionIdsFromTable(rootVersionIds, query, table+"_R");
        query = "DELETE FROM "+table+" WHERE ROOT_VERSION_ID = :value";
        queryExecutor.deleteRootversionIdsFromTable(rootVersionIds, query, table);

    }

    public String getCaraQuery(String alias,boolean isFlush) {
        String query="";
        try {
            log.info("Generating cara query for " + alias);
            List<String> fieldList= rdsMappingConfiguration.getAllFieldList(alias);
            if (isFlush) {
                query = "select " + String.join(", ", fieldList) + " from " + alias;
            } else {
                query = "select " + String.join(", ", fieldList) + ", modified from " + alias
                        + " WHERE modified >= '" + rdsDtConfiguration.getLastUpdatedDT() + "'";
            }

        }catch(Exception e){
            commonMonitorUtility.incrementErrorCount();
            log.error("Exception in getCaraQuery method ", e);
            e.printStackTrace();
        }
        return query;
    }

//    public String getCaraQueryOld(String alias, String tableName, List<String> fieldList) {
//        String query="";
//        try {
//            log.info("Generating cara query for " + alias + " and table=> " + tableName);
//            boolean isFlush = false;
//            if(tableName.endsWith("_R")){
//                isFlush = true;
//            }else{
//                isFlush  = rdsMappingConfiguration.isFlush(alias);
//            }
//            if (isFlush) {
//                query = "select " + String.join(", ", fieldList) + " from " + alias;
//            } else {
//                query = "select " + String.join(", ", fieldList) + ", modified from " + alias
//                        + " WHERE modified >= '" + rdsDtConfiguration.getLastUpdatedDT() + "'";
//            }
//        }catch(Exception e){
//            monitoringUtility.incrementErrorCount();
//            log.error("Exception in getCaraQuery method ", e);
//            e.printStackTrace();
//        }
//        return query;
//    }

    public void performFlushOperation(String tableName, String alias){
        //When Flushing, mind the referential constraints – the order will
        // probably be OBJECT_R, OBJECT, Repeating table and finally regular table

        String RepeatingTable = tableName+"_R";
        String RegularTable = tableName;

        String selectQuery = "select DISTINCT ROOT_VERSION_ID from " + RepeatingTable;
        List<String> rootVersionIdList = queryExecutor.getRootVersionIdList(selectQuery);
        if (rdsMappingConfiguration.isObjectReqPop(alias)) {
            String deleteQuery = "DELETE FROM OBJECT_R WHERE ROOT_VERSION_ID = :value";
            queryExecutor.deleteRootversionIdsFromTable(rootVersionIdList, deleteQuery, "OBJECT_R");

        }
        String truncateQuery = "TRUNCATE TABLE " + RepeatingTable;
        queryExecutor.executeUpdate(truncateQuery);
        log.info("Rows are truncated from table=> " + RepeatingTable);
        selectQuery = "select ROOT_VERSION_ID from " + RegularTable;
        rootVersionIdList = queryExecutor.getRootVersionIdList(selectQuery);
        if (rdsMappingConfiguration.isObjectReqPop(alias)) {
            String deleteQuery = "DELETE FROM OBJECT WHERE ROOT_VERSION_ID = :value";
            queryExecutor.deleteRootversionIdsFromTable(rootVersionIdList, deleteQuery, "OBJECT");

        }
        truncateQuery = "TRUNCATE TABLE " + RegularTable;
        queryExecutor.executeUpdate(truncateQuery);
        log.info("Rows are truncated from table=> " + RegularTable);
    }

    public void createOracleQuery(CaraQueryResult queryResult, String alias, String tableName, List<String> objectFieldList,
                                  List<String> object_rFieldList, List<String> regularTableFieldList, List<String> repeatTableFieldList){
        AtomicInteger i= new AtomicInteger(0);
        try {
            queryResult.getResults().forEach(node -> {
                String rootVersionId = node.getString("root_version_id");
                log.debug(i.incrementAndGet() + "=>" + rootVersionId);

                //To identify how many queries are generated from this node
                AtomicInteger nodeId = new AtomicInteger(0);

                if (queryExecutor.checkRootObjectIdExist(tableName, rootVersionId)) {
                    createUpdateQuery(nodeId.incrementAndGet(), node, alias, tableName, rootVersionId,
                            objectFieldList, object_rFieldList, regularTableFieldList, repeatTableFieldList);
                    //queries = createUpdateQuery(nodeId.incrementAndGet(), node, alias, tableName, rootVersionId, fieldList);
                } else {
                    createInsertQuery(nodeId.incrementAndGet(), node, alias, tableName, rootVersionId,
                            objectFieldList, object_rFieldList, regularTableFieldList, repeatTableFieldList);
                    //queries = createInsertQuery(nodeId.incrementAndGet(), node, alias, tableName, fieldList, rootVersionId);
                }
            });
        }catch(Exception e){
            commonMonitorUtility.incrementErrorCount();
            log.error("Exception inside getOracleQuery method: ",e );
            e.printStackTrace();
        }


    }

//    public void createOracleQueryOld(CaraQueryResult queryResult,
//                                  String alias, String tableName,
//                                  List<String> fieldList) {
//        AtomicInteger i= new AtomicInteger(0);
//        //Map<String,Map<String, String>> queryList = new HashMap<>();
//        //Check for the flush value in mapping.json and truncate table if true
//        //truncate first _R table then if needed truncate regular table.
//        //This is the reason _R table mapping is place first in mapping.json
//        //List does not change the order in which the tablenames are added
//        //When Flushing, mind the referential constraints – the order will
//        // probably be OBJECT_R, OBJECT, _R table and finally regular table which does not end with _R in its name
//        try {
//            boolean isFlush = rdsMappingConfiguration.isFlush(alias);
//            if(!QueryCreationUtility.isTruncatedAlias(alias)){
//
//                String table = tableName;
//                if (table.endsWith("_R")) {
//                    table = table.substring(0, table.length() - 2);
//                }
//                String selectQuery = "";
//                if (isFlush) {
//                    selectQuery = "select ROOT_VERSION_ID from " + table;
//                } else {
//                    selectQuery = "select DISTINCT ROOT_VERSION_ID from " + table + "_R";
//                }
//                List<String> rootVersionIdList = queryExecutor.getRootVersionIdList(selectQuery);
//                if (rdsMappingConfiguration.isObjectReqPop(alias)) {
//                    String deleteQuery = "DELETE FROM OBJECT_R WHERE ROOT_VERSION_ID = :value";
//                    queryExecutor.deleteRootversionIdsFromTable(rootVersionIdList, deleteQuery);
//
//                }
//                String truncateQuery = "TRUNCATE TABLE " + table + "_R";
//                queryExecutor.executeUpdate(truncateQuery);
//                log.info("Rows are truncated from table=> " + table + "_R");
//                if (isFlush) {
//                    if (rdsMappingConfiguration.isObjectReqPop(alias)) {
//                        String deleteQuery = "DELETE FROM OBJECT WHERE ROOT_VERSION_ID = :value";
//                        queryExecutor.deleteRootversionIdsFromTable(rootVersionIdList, deleteQuery);
//                    }
//                    truncateQuery = "TRUNCATE TABLE " + table;
//                    queryExecutor.executeUpdate(truncateQuery);
//                    log.info("Rows are truncated from table=> " + table);
//                }
//                QueryCreationUtility.addAlias(alias);
//            }
//
//
//            List<String> objectFieldList = null;
//            List<String> object_rFieldList = null;
//            if(rdsMappingConfiguration.isObjectReqPop(alias)){
//                objectFieldList = rdsMappingConfiguration.getObjectTableFieldList();
//                object_rFieldList = rdsMappingConfiguration.getObject_RTableFieldList();
//            }
//            final List<String> finalObjFieldList = objectFieldList;
//            final List<String> finalObj_rFieldList = object_rFieldList;
//            //final boolean finalFlush  = isFlush;
//            //To identify how many queries are generated from this node
//            //This nodeId is attached along with the query created.
//            AtomicInteger nodeId = new AtomicInteger(0);
//            //queryResult.setOffset(queryResult.getOffset()+1);
//            //queryResult.setTotalCount(10);
//            //while(nodeId.get()<queryResult.getTotalCount()) {
//                queryResult.getResults().forEach(node -> {
//                    String rootVersionId = node.getString("root_version_id");
//                    log.debug(i.incrementAndGet() + "=>" + rootVersionId);
//
//                    //Supplier<Map<String, Map<String, String>>> arrayListSupplier = () -> {
//                    //Map<String, Map<String, String>> queries = new HashMap<>();
//                    try {
//
//                        if (isFlush) {
//                            createInsertQuery(nodeId.incrementAndGet(), node, alias, tableName, fieldList, rootVersionId, finalObjFieldList, finalObj_rFieldList);
//                            //queries = createInsertQuery(nodeId.incrementAndGet(), node, alias, tableName, fieldList, rootVersionId);
//                        } else if (queryExecutor.checkRootObjectIdExist(tableName, rootVersionId)) {
//                            createUpdateQuery(nodeId.incrementAndGet(), node, alias, tableName, rootVersionId, fieldList, finalObjFieldList, finalObj_rFieldList);
//                            //queries = createUpdateQuery(nodeId.incrementAndGet(), node, alias, tableName, rootVersionId, fieldList);
//                        } else {
//                            createInsertQuery(nodeId.incrementAndGet(), node, alias, tableName, fieldList, rootVersionId, finalObjFieldList, finalObj_rFieldList);
//                            //queries = createInsertQuery(nodeId.incrementAndGet(), node, alias, tableName, fieldList, rootVersionId);
//                        }
//                    } catch (Exception e) {
//                        monitoringUtility.incrementErrorCount();
//                        log.error("Exception while creating query", e);
//                        e.printStackTrace();
//                    }
//                    //return queries;
//                    //};
//
//                    // Use the supplier to get the ArrayList
//                    //queryList.putAll(arrayListSupplier.get());
//                });
//                //queryResult.setOffset(queryResult.getOffset()+1000);
//            //}
//        }catch(Exception e){
//            monitoringUtility.incrementErrorCount();
//            log.error("Exception inside getOracleQuery method: ",e );
//            e.printStackTrace();
//        }
//        //return queryList;
//    }
    //No processUpdateQueryFor_Rtable bacause uniqueness is determined using root_version_id and R_INDEX column
    // Where R_INDEX is not available in JSON So we won't update.
    //instead we delete then insert
    private void createUpdateQuery(int nodeId, CaraProperties node, String alias, String tableName, String rootVersionId,
                                   List<String> objectFieldList, List<String> object_rFieldList,
                                   List<String> regularTableFieldList, List<String> repeatTableFieldList) {
        //Map<String,Map<String, String>> queryList = new HashMap<>();
        try {
            List<String> rootVersionIds = new ArrayList<>();
            rootVersionIds.add(rootVersionId);
            if(rdsMappingConfiguration.isObjectReqPop(alias)) {
                String deleteQuery = "DELETE FROM OBJECT_R WHERE ROOT_VERSION_ID = :value";
                queryExecutor.deleteRootversionIdsFromTable(rootVersionIds, deleteQuery, "OBJECT_R");
            }
            String query = "DELETE FROM "+tableName+"_R WHERE ROOT_VERSION_ID = :value";
            queryExecutor.deleteRootversionIdsFromTable(rootVersionIds, query, tableName+"_R");
            processUpdateQueryForRegulartable(nodeId, node, alias, tableName, rootVersionId, regularTableFieldList,objectFieldList);
            processInsertQueryFor_Rtable(nodeId, node, alias, tableName+"_R", repeatTableFieldList, rootVersionId,object_rFieldList);
        }catch (Exception e){
            commonMonitorUtility.incrementErrorCount();
            log.error("Exception inside createUpdateQuery method ",e );
            e.printStackTrace();
        }

        //return queryList;
    }



    private void createInsertQuery(int nodeId, CaraProperties node, String alias, String tableName, String rootVersionId,
                                   List<String> objectFieldList, List<String> object_rFieldList,
                                   List<String> regularTableFieldList, List<String> repeatTableFieldList) {
        //Map<String,Map<String, String>> queryList = new HashMap<>();
        try {
            processInsertQueryForRegulartable(nodeId, node, alias, tableName, regularTableFieldList,objectFieldList);
            processInsertQueryFor_Rtable(nodeId, node, alias, tableName+"_R", repeatTableFieldList, rootVersionId,object_rFieldList);

        }catch(Exception e){
            commonMonitorUtility.incrementErrorCount();
            log.error("Exception in createInsertQuery method ",e);
            e.printStackTrace();
        }
        //return queryList;
    }

//    private void processInsertQueryForRegulartable(int nodeId, CaraProperties node,
//                                                              String alias, String tableName,
//                                                              List<String> fieldList, List<String> objectFieldList) {
//        //<Query,Map<paramIdx, paramvalue>>
//        //Map<String,Map<String, String>> queryListWithParam = new HashMap<>();
//        try {
//            boolean isObjectPopReq = rdsMappingConfiguration.isObjectReqPop(alias);
//            //to identity the query count sent for execution
//            int queryIdx = 0;
//            String query = "INSERT INTO " + tableName;
//            String objectQuery = "";
//            if(isObjectPopReq){
//                objectQuery = "INSERT INTO OBJECT";
//            }
//            StringJoiner colListQuery = new StringJoiner(", ", "(", ")");
//            StringJoiner valueListQuery = new StringJoiner(", ", "(", ")");
//
//            Map<String, String> paramMap = new HashMap<>();
//            AtomicInteger columnIndex = new AtomicInteger(1);
//            fieldList.forEach(field -> {
//
//                StringJoiner objColListQuery =null;
//                StringJoiner objValueListQuery=null;
//                Map<String, String> objParamMap = null;
//                AtomicInteger objColumnIndex = null;
//                if(isObjectPopReq) {
//                    objColListQuery = new StringJoiner(", ", "(", ")");
//                    objValueListQuery = new StringJoiner(", ", "(", ")");
//                    objParamMap = new HashMap<>();
//                    objColumnIndex = new AtomicInteger(1);
//                }
//                String objectColumnName = "";
//                String objectColumnType = "";
//                boolean isObjectField = isObjectPopReq && objectFieldList.contains(field);
//                if(isObjectField){
//                    Map<String, String> objFieldConfigurationMap = rdsMappingConfiguration.getObjectTableFieldConfiguration(field);
//                    objectColumnName = objFieldConfigurationMap.get("columnname");
//                    objectColumnType = objFieldConfigurationMap.get("columntype");
//                }
//                Map<String, String> fieldConfigurationMap = rdsMappingConfiguration.getFieldConfiguration(alias,
//                        tableName, field);
//                String fieldtype = fieldConfigurationMap.get("fieldtype");
//                String columnName = fieldConfigurationMap.get("columnname");
//                String columnType = fieldConfigurationMap.get("columntype");
//                log.debug("fieldtype => " + fieldtype + " columnname => " + columnName + " columntype => " + columnType);
//                if (fieldtype.equalsIgnoreCase("String")) {
//                    Optional<String> optionalString = Optional.ofNullable(node.getString(field));
//                    optionalString.ifPresent(fieldValue -> {
//
//                        log.debug("String-fieldValue=> " + fieldValue);
//                        if(isObjectField){
//                                objColListQuery.add(objectColumnName);
//                                objValueListQuery.add("?");
//                                objParamMap.put(objColumnIndex.getAndIncrement() + "#" + objectColumnType, fieldValue);
//                        }
//
//
//                        colListQuery.add(columnName);
//                        valueListQuery.add("?");
//                        paramMap.put(columnIndex.getAndIncrement() + "#" + columnType, fieldValue);
//                    });
//                } else if (fieldtype.equalsIgnoreCase("Date")) {
//                    Optional<LocalDate> optionalString = Optional.ofNullable(node.getDate(field));
//                    optionalString.ifPresent(fieldValue -> {
//                        log.debug("Date-fieldValue=> " + fieldValue);
//                        if(isObjectField){
//                            objColListQuery.add(objectColumnName);
//                            objValueListQuery.add("?");
//                            objParamMap.put(objColumnIndex.getAndIncrement() + "#" + objectColumnType, fieldValue.toString());
//                        }
//                        colListQuery.add(columnName);
//                        valueListQuery.add("?");
//                        paramMap.put(columnIndex.getAndIncrement() + "#" + columnType, fieldValue.toString());
//                    });
//                } else {
//                    throw new RuntimeException("Invalid fieldtype => " + fieldtype);
//                }
//
//            });
//            query = query + colListQuery + " VALUES " + valueListQuery;
//            objectQuery = objectQuery + objColListQuery + " VALUES " + objValueListQuery;
//            log.debug("Query added for Regular table=> " + query);
//            log.debug("Size of Param Map added for Regular table=> " + paramMap.size());
//            log.debug("Object Query added for Object table=> " + objectQuery);
//            log.debug("Size of Object Param Map added for Object table=> " + objParamMap.size());
//            queryIdx++;
//            //this log entry will be useful while search for a specific query
//            log.debug("Search Indx: "+nodeId + "->" + queryIdx);
//            //queryListWithParam.put(nodeId + "->" + queryIdx + "#" + query, paramMap);
//            MonitoringUtility.incrementNoOfInsertQueryInRegtable(queryIdx);
//            int updateCount = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + query,paramMap);
//            if (updateCount>0)
//                log.info(query + " is processed successfully. Update Count => "+updateCount);
//            else
//                log.info(query + " processing failed. Update Count => "+updateCount);
//            MonitoringUtility.incrementNoOfInsertQueryInObjecttable(queryIdx);
//            int updateCountObj = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + objectQuery,objParamMap);
//            if (updateCount>0)
//                log.info(query + " is processed successfully. Update Count => "+updateCountObj);
//            else
//                log.info(query + " processing failed. Update Count => "+updateCountObj);
//        }catch (Exception e){
//            MonitoringUtility.incrementErrorCount();
//            log.error("Exception inside processInsertQueryForregularTable ",e);
//            e.printStackTrace();
//        }
//        //return queryListWithParam;
//    }

    private void processInsertQueryForRegulartable(int nodeId, CaraProperties node, String alias, String tableName,
                                                   List<String> fieldList, List<String> objectFieldList) {
        try {
            boolean isObjectPopReq = rdsMappingConfiguration.isObjectReqPop(alias);
            int queryIdx = 0;

            String query = "INSERT INTO " + tableName;
            String objectQuery = isObjectPopReq ? "INSERT INTO OBJECT" : "";
            StringJoiner colListQuery = new StringJoiner(", ", "(", ")");
            StringJoiner valueListQuery = new StringJoiner(", ", "(", ")");

            StringJoiner objColListQuery = null;
            StringJoiner objValueListQuery = null;
            Map<String, String> objParamMap = null;
            AtomicInteger objColumnIndex = null;

            if (isObjectPopReq) {
                objColListQuery = new StringJoiner(", ", "(", ")");
                objValueListQuery = new StringJoiner(", ", "(", ")");
                objParamMap = new HashMap<>();
                objColumnIndex = new AtomicInteger(1);
            }

            Map<String, String> paramMap = new HashMap<>();
            AtomicInteger columnIndex = new AtomicInteger(1);

            for (String field : fieldList) {
                boolean isObjectField = isObjectPopReq && objectFieldList.contains(field);
                Map<String, String> fieldConfigurationMap = rdsMappingConfiguration.getFieldConfiguration(alias, tableName, field);
                String fieldtype = fieldConfigurationMap.get("fieldtype");
                String columnName = fieldConfigurationMap.get("columnname");
                String columnType = fieldConfigurationMap.get("columntype");

                if (isObjectField) {
                    Map<String, String> objFieldConfigurationMap = rdsMappingConfiguration.getObjectTableFieldConfiguration(field);
                    String objectColumnName = objFieldConfigurationMap.get("columnname");
                    String objectColumnType = objFieldConfigurationMap.get("columntype");

                    addFieldToQueries(node, field, fieldtype, columnName, columnType, colListQuery, valueListQuery, paramMap, columnIndex,
                            objectColumnName, objectColumnType, objColListQuery, objValueListQuery, objParamMap, objColumnIndex, tableName);
                } else {
                    addFieldToQueries(node, field, fieldtype, columnName, columnType, colListQuery, valueListQuery, paramMap, columnIndex, tableName);
                }
            }
            queryIdx++;
            //order of query execution will probably be OBJECT_R, OBJECT, _R table and finally regular table

            query += colListQuery + " VALUES " + valueListQuery;

            logQueryDetails(nodeId, queryIdx, query, paramMap,tableName);

            int updateCount = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + query, paramMap);
            logUpdateCount(query, updateCount);
            if (isObjectPopReq) {
                objectQuery += objColListQuery + " VALUES " + objValueListQuery;
                logObjectQueryDetails(nodeId, queryIdx, objectQuery, objParamMap, "OBJECT");

                int updateCountObj = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + objectQuery, objParamMap);
                logUpdateCount(objectQuery, updateCountObj);
            }


        } catch (Exception e) {
            commonMonitorUtility.incrementErrorCount();
            log.error("Exception inside processInsertQueryForRegularTable", e);
        }
    }

    private void addFieldToQueries(CaraProperties node, String field, String fieldtype, String columnName, String columnType,
                                   StringJoiner colListQuery, StringJoiner valueListQuery, Map<String, String> paramMap, AtomicInteger columnIndex,
                                   String objectColumnName, String objectColumnType, StringJoiner objColListQuery, StringJoiner objValueListQuery,
                                   Map<String, String> objParamMap, AtomicInteger objColumnIndex, String tableName) {
        //int colIdx = columnIndex.getAndIncrement();
        switch (fieldtype.toLowerCase()) {
            case "string":
                Optional.ofNullable(node.getString(field)).ifPresent(fieldValue -> {
                    log.debug("Reg: "+columnName+"=>"+fieldValue);
                    colListQuery.add(columnName);
                    valueListQuery.add("?");
                    paramMap.put(columnIndex.getAndIncrement() + "#" + columnType, QueryCreationUtility.safeTrim(fieldValue, columnName, tableName));
                    if (objColListQuery != null && objValueListQuery != null && objParamMap != null) {
                        //int objColIdx = objColumnIndex.getAndIncrement();
                        log.debug("Obj: "+objectColumnName+"=>"+fieldValue);
                        objColListQuery.add(objectColumnName);
                        objValueListQuery.add("?");
                        objParamMap.put(objColumnIndex.getAndIncrement() + "#" + objectColumnType, QueryCreationUtility.safeTrim(fieldValue, objectColumnName, "OBJECT"));
                    }
                });
                break;
            case "date":
                Optional.ofNullable(node.getDate(field)).ifPresent(fieldValue -> {
                    log.debug("Reg: "+columnName+"=>"+fieldValue);
                    colListQuery.add(columnName);
                    valueListQuery.add("?");
                    paramMap.put(columnIndex.getAndIncrement() + "#" + columnType, fieldValue.toString());
                    if (objColListQuery != null && objValueListQuery != null && objParamMap != null) {
                        //int objColIdx = objColumnIndex.getAndIncrement();
                        log.debug("Obj: "+objectColumnName+"=>"+fieldValue);
                        objColListQuery.add(objectColumnName);
                        objValueListQuery.add("?");
                        objParamMap.put(objColumnIndex.getAndIncrement() + "#" + objectColumnType, fieldValue.toString());
                    }
                });
                break;
            default:
                throw new RuntimeException("Invalid fieldtype => " + fieldtype);
        }
    }

    private void addFieldToQueries(CaraProperties node, String field, String fieldtype, String columnName, String columnType,
                                   StringJoiner colListQuery, StringJoiner valueListQuery, Map<String, String> paramMap, AtomicInteger columnIndex, String tableName) {
        addFieldToQueries(node, field, fieldtype, columnName, columnType, colListQuery, valueListQuery, paramMap, columnIndex,
                null, null, null, null, null, null, tableName);
    }

    private void logQueryDetails(int nodeId, int queryIdx, String query, Map<String, String> paramMap, String tableName) {
        log.debug("Query added for Regular table => " + query);
        log.debug("Size of Param Map added for Regular table => " + paramMap.size());
        //this log entry will be useful while search for a specific query
        log.debug("Search Indx: " + nodeId + "->" + queryIdx);
        MonitoringUtility monitoringUtility = loggingUtility.getMonitoringUtility(tableName);
        monitoringUtility.incrementInsertQuery(queryIdx);
        loggingUtility.setMonitoringUtility(tableName, monitoringUtility);
    }

    private void logObjectQueryDetails(int nodeId, int queryIdx, String query, Map<String, String> paramMap, String tableName) {
        log.debug("Query added for Object table => " + query);
        log.debug("Size of Param Map added for Object table => " + paramMap.size());
        log.debug("Search ObjIndx: " + nodeId + "->" + queryIdx);
        MonitoringUtility monitoringUtility = loggingUtility.getMonitoringUtility(tableName);
        monitoringUtility.incrementInsertQuery(queryIdx);
        loggingUtility.setMonitoringUtility(tableName,monitoringUtility);
    }

    private void logUpdateCount(String query, int updateCount) {
        if (updateCount > 0) {
            log.debug(query + " is processed successfully. Update Count => " + updateCount);
        } else {
            log.info(query + " processing failed. Update Count => " + updateCount);
        }
    }


//        private void processInsertQueryFor_Rtable(int nodeId, CaraProperties node,
//                                                         String alias, String tableName,
//                                                             List<String> fieldList, String rootVersionId,
//                                                   List<String> object_rFieldList) {
//        //<Query,Map<paramIdx, paramvalue>>
//        //Map<String,Map<String, String>> queryListWithParam = new HashMap<>();
//        try {
//            //flag to keep track if need to populate object table
//            boolean isObjectPopReq = rdsMappingConfiguration.isObjectReqPop(alias);
//            //To keep track of how many rows to be created in _R table
//            AtomicInteger maxRows = new AtomicInteger(0);
//            AtomicInteger objMaxRows = new AtomicInteger(0);
//            //<field,<Idx, fieldValue>>
//            //
//            Map<String, Map<Integer, String>> fieldValueMap = new HashMap<>();
//            Map<String, Map<Integer, String>> objFieldValueMap = new HashMap<>();
//
//            AtomicReference<String> productAuthObjId = new AtomicReference<>("");
//
//            fieldList.forEach(field -> {
//                boolean isContains = false;
//                if(isObjectPopReq) {
//
//                    if (object_rFieldList.contains(field)) {
//                        isContains = true;
//
//                    }
//                }
//                final boolean finalIsContains = isContains;
//                Map<String, String> objFieldConfigurationMap = rdsMappingConfiguration.getObject_RTableFieldConfiguration(field);
//                Map<String, String> fieldConfigurationMap = rdsMappingConfiguration.getFieldConfiguration(alias,
//                        tableName, field);
//                String fieldtype = fieldConfigurationMap.get("fieldtype");
//                log.debug("field Type => " + fieldtype);
//
//                if (fieldtype.equalsIgnoreCase("String")) {
//                    Optional<String> optionalString = Optional.ofNullable(node.getString(field));
//                    optionalString.ifPresent(fieldValue -> {
//                        //added this line to edit [1ff5227b-090d-4718-8e9f-65f99661496d] this value
//                        //INDICATION_UID's max length is 37 but here it is 38
//                        // So removed square braces to fix the issue
//                        fieldValue = fieldValue.replace("[", "").replace("]", "");
//
//                        //row won't insert if object_id is not passed because it is pk
//                        //So while creating insert query provide this value if null
//                        if (field.equalsIgnoreCase("object_id")) {
//                            productAuthObjId.set(fieldValue);
//                        }
//
//                        if (fieldValue.contains(",")) {
//                            log.debug("String-fieldValue=> " + fieldValue);
//                            String[] temp = fieldValue.split(",");
//                            Map<Integer, String> tempMap = new HashMap<>();
//                            for (int i = 0; i < temp.length; i++) {
//                                log.debug("iftemp[ " + i + " ]=>" + temp[i]);
//                                tempMap.put(i, temp[i]);
//                            }
//                            if (maxRows.get() < temp.length) {
//                                maxRows.set(temp.length);
//                                if(isObjectPopReq && finalIsContains){
//                                    objMaxRows.set(temp.length);
//                                }
//                            }
//                            log.debug("maxRows: " + maxRows.get() + " :: temp.length : " + temp.length);
//                            fieldValueMap.put(field, tempMap);
//
//                            if(isObjectPopReq && finalIsContains) {
//                                objFieldValueMap.put(field, tempMap);
//                                log.debug("objmaxRows: " + objMaxRows.get() + " :: temp.length : " + temp.length);
//                            }
//                        } else {
//                            String[] temp = {fieldValue};
//                            Map<Integer, String> tempMap = new HashMap<>();
//                            tempMap.put(0, temp[0]);
//                            fieldValueMap.put(field, tempMap);
//                            if(isObjectPopReq && finalIsContains) {
//                                objFieldValueMap.put(field, tempMap);
//                            }
//                        }
//
//                    });
//                } else if (fieldtype.equalsIgnoreCase("Date")) {
//                    Optional<LocalDate> optionalString = Optional.ofNullable(node.getDate(field));
//                    log.debug("Checking if date value is available=> " + node.getDate(field));
//                    optionalString.ifPresent(fieldValue -> {
//                        log.debug("Date-tmpStr-fieldValue=> " + fieldValue);
//                        String tempStr = fieldValue.toString();
//                        log.debug("Date-tmpStr=> " + tempStr);
//                        if (tempStr.contains(",")) {
//                            String[] temp = tempStr.split(",");
//                            Map<Integer, String> tempMap = new HashMap<>();
//                            for (int i = 0; i < temp.length; i++) {
//                                log.debug("elsetemp[ " + i + " ]=>" + temp[i]);
//                                tempMap.put(i, temp[i]);
//                            }
//                            if (maxRows.get() < temp.length) {
//                                maxRows.set(temp.length);
//                                if(isObjectPopReq && finalIsContains){
//                                    objMaxRows.set(temp.length);
//                                }
//                            }
//                            log.debug("maxRows: " + maxRows.get() + " :: temp.length : " + temp.length);
//                            fieldValueMap.put(field, tempMap);
//                            if(isObjectPopReq && finalIsContains) {
//                                objFieldValueMap.put(field, tempMap);
//                                log.debug("objmaxRows: " + objMaxRows.get() + " :: temp.length : " + temp.length);
//                            }
//                        } else {
//                            String[] temp = {fieldValue.toString()};
//                            Map<Integer, String> tempMap = new HashMap<>();
//                            tempMap.put(0, temp[0]);
//                            fieldValueMap.put(field, tempMap);
//                            if(isObjectPopReq && finalIsContains) {
//                                objFieldValueMap.put(field, tempMap);
//                            }
//                        }
//                    });
//                } else {
//                    throw new RuntimeException("Invalid fieldtype => " + fieldtype);
//                }
//
//            });
//
//            //To identity the query count sent for execution
//            int queryIdx = 0;
//            int objQueryIdx= 0;
//            //This loops for the max numbers of Rows for the longest comma seperated field value
//            //Number of values in comma seperated field value varies.
//            // Here maxRows will keep track of that count and loops maximum number of times to accomadate all the values
//            //One value in each column. null if there is no value for that column
//            int rIdx = 0;
//            for (int i = 0; i <= maxRows.get(); i++) {
//                final int currentRow = i;
//
//                log.debug("current Row: " + currentRow);
//                AtomicInteger columnIndex = new AtomicInteger(1);
//                Map<String, String> paramMap = new HashMap<>();
//                //Add R_INDEX because it is pk
//                StringJoiner colListQuery = new StringJoiner(", ", "(", ", R_INDEX)");
//                StringJoiner valueListQuery = new StringJoiner(", ", "(", ", ?)");
//                //For each column
//                fieldValueMap.forEach((field, multirowMapping) -> {
//                    Map<String, String> fieldConfigurationMap = rdsMappingConfiguration
//                            .getFieldConfiguration(alias, tableName, field);
//                    String columnName = fieldConfigurationMap.get("columnname");
//                    String columnType = fieldConfigurationMap.get("columntype");
//                    log.debug("columnname => " + columnName + " columntype => " + columnType);
//                    colListQuery.add(columnName);
//                    valueListQuery.add("?");
//                    String value = multirowMapping.get(currentRow);
//                    //ROOT_VERSION_ID and PRODUCT_AUTHORISATION_R_OBJECT_ID are pk
//                    // So do not fail to give value for them while creating insert query
//                    if (columnName.equalsIgnoreCase("ROOT_VERSION_ID") && value == null) {
//                        log.debug("root_version_id is set as value since value=>" + value + "for columnname=>" + columnName);
//                        value = rootVersionId;
//                    }
//                    if (columnName.equalsIgnoreCase("PRODUCT_AUTHORISATION_R_OBJECT_ID") && value == null) {
//                        log.debug("object_id is set as value since value=>" + value + "for columnname=>" + columnName);
//                        value = productAuthObjId.get();
//                    }
//
//                    log.debug("paramMap value: " + value);
//                    String key = columnIndex.getAndIncrement() + "#" + columnType;
//                    log.debug("paramMap key: " + key);
//                    paramMap.put(key, value);
//                });
//                String query = "INSERT INTO " + tableName + " " + colListQuery + " VALUES " + valueListQuery;
//                log.debug("Query added for _R table=> " + query);
//                log.debug("Size of Param Map added for _R table=> " + paramMap.size());
//                queryIdx++;
//                rIdx++;
//                //# is added to seperate the query from the nodeId and queryIdx during query execution
//                log.debug("rIdx:=>" + rIdx);
//                log.debug("Search Indx: "+nodeId + "->" + queryIdx);
//                //Add value for R_INDEX because it is pk
//                paramMap.put(columnIndex.getAndIncrement() + "#" + "Int", rIdx + "");
//                int updateCount = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + query,paramMap);
//                if (updateCount>0)
//                    log.info(query + " is processed successfully. Update Count => "+updateCount);
//                else
//                    log.info(query + " processing failed. Update Count => "+updateCount);
//            }
//            MonitoringUtility.incrementNoOfInsertQueryIn_Rtable(queryIdx);
//
//            int objRIdx = 0;
//            for (int i = 0; i <= objMaxRows.get(); i++) {
//                final int currentRow = i;
//
//                log.debug("current Row: " + currentRow);
//                AtomicInteger columnIndex = new AtomicInteger(1);
//                Map<String, String> paramMap = new HashMap<>();
//                //Add R_INDEX because it is pk
//                StringJoiner colListQuery = new StringJoiner(", ", "(", ", R_INDEX)");
//                StringJoiner valueListQuery = new StringJoiner(", ", "(", ", ?)");
//                //For each column
//                objFieldValueMap.forEach((field, multirowMapping) -> {
//                    Map<String, String> fieldConfigurationMap = rdsMappingConfiguration
//                            .getFieldConfiguration(alias, tableName, field);
//                    String columnName = fieldConfigurationMap.get("columnname");
//                    String columnType = fieldConfigurationMap.get("columntype");
//                    log.debug("columnname => " + columnName + " columntype => " + columnType);
//                    colListQuery.add(columnName);
//                    valueListQuery.add("?");
//                    String value = multirowMapping.get(currentRow);
//                    //ROOT_VERSION_ID and PRODUCT_AUTHORISATION_R_OBJECT_ID are pk
//                    // So do not fail to give value for them while creating insert query
//                    if (columnName.equalsIgnoreCase("ROOT_VERSION_ID") && value == null) {
//                        log.debug("root_version_id is set as value since value=>" + value + "for columnname=>" + columnName);
//                        value = rootVersionId;
//                    }
//                    if (columnName.equalsIgnoreCase("OBJECT_R_OBJECT_ID") && value == null) {
//                        log.debug("object_id is set as value since value=>" + value + "for columnname=>" + columnName);
//                        value = productAuthObjId.get();
//                    }
//
//                    log.debug("paramMap value: " + value);
//                    String key = columnIndex.getAndIncrement() + "#" + columnType;
//                    log.debug("paramMap key: " + key);
//                    paramMap.put(key, value);
//                });
//                String query = "INSERT INTO " + tableName + " " + colListQuery + " VALUES " + valueListQuery;
//                log.debug("Query added for _R table=> " + query);
//                log.debug("Size of Param Map added for _R table=> " + paramMap.size());
//                queryIdx++;
//                rIdx++;
//                //# is added to seperate the query from the nodeId and queryIdx during query execution
//                log.debug("rIdx:=>" + rIdx);
//                log.debug("Search Indx: "+nodeId + "->" + queryIdx);
//                //Add value for R_INDEX because it is pk
//                paramMap.put(columnIndex.getAndIncrement() + "#" + "Int", rIdx + "");
//                int updateCount = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + query,paramMap);
//                if (updateCount>0)
//                    log.info(query + " is processed successfully. Update Count => "+updateCount);
//                else
//                    log.info(query + " processing failed. Update Count => "+updateCount);
//            }
//            MonitoringUtility.incrementNoOfInsertQueryInObject_Rtable(objQueryIdx);
//
//        }catch (Exception e){
//            MonitoringUtility.incrementErrorCount();
//            log.error("Exception in processInsertQueryFor_RTable ",e );
//            e.printStackTrace();
//        }
//        //return queryListWithParam;
//    }

    private void processInsertQueryFor_Rtable(int nodeId, CaraProperties node,
                                              String alias, String tableName,
                                              List<String> fieldList, String rootVersionId,
                                              List<String> object_rFieldList) {
        try {
            //flag to keep track if need to populate object table
            boolean isObjectPopReq = rdsMappingConfiguration.isObjectReqPop(alias);
            //To keep track of how many rows to be created in _R table
            AtomicInteger maxRows = new AtomicInteger(0);
            AtomicInteger objMaxRows = new AtomicInteger(0);

            Map<String, Map<Integer, String>> fieldValueMap = new HashMap<>();
            Map<String, Map<Integer, String>> objFieldValueMap = new HashMap<>();
            AtomicReference<String> productAuthObjId = new AtomicReference<>("");

            fieldList.forEach(field -> {
                boolean isObjectField = isObjectPopReq && object_rFieldList.contains(field);
                processField(node, field, fieldValueMap, maxRows, productAuthObjId, isObjectField, objFieldValueMap, objMaxRows,
                        alias, tableName);
            });
            //order of query execution will probably be OBJECT_R, OBJECT, _R table and finally regular table

            int queryIdx = 1;
            int rIdx = 1;
            for (int i = 0; i < maxRows.get(); i++) {
                processInsertQuery(nodeId, alias, tableName, rootVersionId, fieldValueMap, queryIdx++, rIdx++, i, productAuthObjId);
            }
            MonitoringUtility monitoringUtility = loggingUtility.getMonitoringUtility(tableName);
            monitoringUtility.incrementInsertQuery(queryIdx);
            loggingUtility.setMonitoringUtility(tableName,monitoringUtility);
            if (isObjectPopReq) {
                int objQueryIdx = 1;
                int objRIdx = 1;
                for (int i = 0; i < maxRows.get(); i++) {
                    processInsertQueryForObject(nodeId, alias, "OBJECT_R", rootVersionId, objFieldValueMap, objQueryIdx++,
                            objRIdx++, i, productAuthObjId);
                }
                monitoringUtility = loggingUtility.getMonitoringUtility("OBJECT_R");
                monitoringUtility.incrementInsertQuery(objQueryIdx);
                loggingUtility.setMonitoringUtility("OBJECT_R", monitoringUtility);
            }


        } catch (Exception e) {
            commonMonitorUtility.incrementErrorCount();
            log.error("Exception in processInsertQueryFor_RTable ", e);
            e.printStackTrace();
        }
    }

    private void processField(CaraProperties node, String field, Map<String, Map<Integer, String>> fieldValueMap,
                              AtomicInteger maxRows, AtomicReference<String> productAuthObjId, boolean isObjectField,
                              Map<String, Map<Integer, String>> objFieldValueMap, AtomicInteger objMaxRows, String alias, String tableName) {
        Map<String, String> fieldConfig = rdsMappingConfiguration.getFieldConfiguration(alias, tableName, field);
        String fieldType = fieldConfig.get("fieldtype");
        String fieldValue = null;

        if (fieldType.equalsIgnoreCase("String")) {
            fieldValue = node.getString(field);
            if(fieldValue != null) {
                if (field != null) {
                    //added this line to edit [1ff5227b-090d-4718-8e9f-65f99661496d] this value
//                        //INDICATION_UID's max length is 37 but here it is 38
//                        // So removed square braces to fix the issue
                    fieldValue = fieldValue.replace("[", "").replace("]", "");
                    if (field.equalsIgnoreCase("object_id")) {
                        productAuthObjId.set(fieldValue.trim());
                    }
                }
            }
        } else if (fieldType.equalsIgnoreCase("Date")) {
            LocalDate dateValue = node.getDate(field);
            if (dateValue != null) {
                fieldValue = dateValue.toString();
            }
        } else {
            throw new RuntimeException("Invalid fieldtype => " + fieldType);
        }

        if (fieldValue != null) {

            processFieldValue(field, fieldValue, fieldValueMap, maxRows);
            if (isObjectField) {
                processFieldValue(field, fieldValue, objFieldValueMap, objMaxRows);
            }
        }
    }

    private void processFieldValue(String field, String fieldValue,
                                   Map<String, Map<Integer, String>> fieldValueMap,
                                   AtomicInteger maxRows) {
        Map<Integer, String> tempMap = new HashMap<>();
        String[] values = fieldValue.split(",");
        for (int i = 0; i < values.length; i++) {
            if(values[i]!=null)
                tempMap.put(i, values[i].trim());
        }
        fieldValueMap.put(field, tempMap);
        maxRows.set(Math.max(maxRows.get(), values.length));
    }

    private void processInsertQuery(int nodeId, String alias, String tableName, String rootVersionId,
                                    Map<String, Map<Integer, String>> fieldValueMap, int queryIdx, int rIdx, int currentRow,
                                    AtomicReference<String> productAuthObjId) {
        AtomicInteger columnIndex = new AtomicInteger(1);
        Map<String, String> paramMap = new HashMap<>();
        StringJoiner colListQuery = new StringJoiner(", ", "(", ", R_INDEX)");
        StringJoiner valueListQuery = new StringJoiner(", ", "(", ", ?)");


        fieldValueMap.forEach((field, multirowMapping) -> {
            Map<String, String> fieldConfig = rdsMappingConfiguration.getFieldConfiguration(alias, tableName, field);
            String columnName = fieldConfig.get("columnname");
            String columnType = fieldConfig.get("columntype");

            colListQuery.add(columnName);
            valueListQuery.add("?");

            String value = multirowMapping.getOrDefault(currentRow, null);
            String col = tableName+"_OBJECT_ID";
            if ("ROOT_VERSION_ID".equalsIgnoreCase(columnName) && value == null) {
                value = rootVersionId;
            } else if (col.equalsIgnoreCase(columnName) && value == null) {
                value = productAuthObjId.get();
            }
            log.debug("_R table=>"+columnName+"=>"+value);
            paramMap.put(columnIndex.getAndIncrement() + "#" + columnType, QueryCreationUtility.safeTrim(value, columnName, tableName));
        });

        String query = "INSERT INTO " + tableName + " " + colListQuery + " VALUES " + valueListQuery;
        log.debug("_RTable=>R_INDEX=>"+rIdx);
        paramMap.put(columnIndex.getAndIncrement() + "#" + "Int", Integer.toString(rIdx));
        int updateCount = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + query, paramMap);
        if (updateCount > 0) {
            log.debug(query + " is processed successfully. Update Count => " + updateCount);
        } else {
            log.info(query + " processing failed. Update Count => " + updateCount);
        }

    }

    private void processInsertQueryForObject(int nodeId, String alias, String tableName, String rootVersionId,
                                    Map<String, Map<Integer, String>> fieldValueMap, int queryIdx, int rIdx, int currentRow,
                                    AtomicReference<String> productAuthObjId) {
        AtomicInteger columnIndex = new AtomicInteger(1);
        Map<String, String> paramMap = new HashMap<>();
        StringJoiner colListQuery = new StringJoiner(", ", "(", ", R_INDEX)");
        StringJoiner valueListQuery = new StringJoiner(", ", "(", ", ?)");


        fieldValueMap.forEach((field, multirowMapping) -> {
            Map<String, String> fieldConfig = rdsMappingConfiguration.getObject_RTableFieldConfiguration(field);
            String columnName = fieldConfig.get("columnname");
            String columnType = fieldConfig.get("columntype");

            colListQuery.add(columnName);
            valueListQuery.add("?");

            String value = multirowMapping.getOrDefault(currentRow, null);
            if ("ROOT_VERSION_ID".equalsIgnoreCase(columnName) && value == null) {
                value = rootVersionId;
            } else if ("OBJECT_R_OBJECT_ID".equalsIgnoreCase(columnName) && value == null) {
                value = productAuthObjId.get();
            }
            log.debug("Obj_R table=>"+columnName+"=>"+value);
            paramMap.put(columnIndex.getAndIncrement() + "#" + columnType, QueryCreationUtility.safeTrim(value, columnName, "OBJECT_R"));
        });

        String query = "INSERT INTO " + tableName + " " + colListQuery + " VALUES " + valueListQuery;
        paramMap.put(columnIndex.getAndIncrement() + "#" + "Int", Integer.toString(rIdx));
        log.debug("Obj_RTable=>R_INDEX=>"+rIdx);
        int updateCount = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + query, paramMap);
        if (updateCount > 0) {
            log.debug(query + " is processed successfully. Update Count => " + updateCount);
        } else {
            log.info(query + " processing failed. Update Count => " + updateCount);
        }

    }


    private void processUpdateQueryForRegulartable(int nodeId, CaraProperties node, String alias, String tableName,
                                                                  String rootVersionId, List<String> fieldList,
                                                   List<String> objectFieldList) {
        //Map<String,Map<String, String>> queryList = new HashMap<>();
        try {
            boolean isObjectPopReq = rdsMappingConfiguration.isObjectReqPop(alias);
            AtomicReference<String> query = new AtomicReference<>("update " + tableName + " SET ");
            AtomicReference<String> objquery = new AtomicReference<>("update OBJECT SET ");
            int queryIdx = 0;
            int objQueryIdx = 0;
            Map<String, String> paramMap = new HashMap<>();
            Map<String, String> objParamMap = new HashMap<>();
            AtomicInteger columnIndex = new AtomicInteger(1);
            AtomicInteger objColumnIndex = new AtomicInteger(1);
            //filtered root_version_id because it is in the where clause of the update query
            fieldList.stream().filter(field -> !field.equalsIgnoreCase("root_version_id")).forEach(field -> {

                boolean isObjectField = isObjectPopReq && objectFieldList.contains(field);
                String objColumnName = "";
                String objColumnType = "";
                if(isObjectField){
                    Map<String, String> objFieldConfigurationMap = rdsMappingConfiguration.getObjectTableFieldConfiguration(field);
                    objColumnName = objFieldConfigurationMap.get("columnname");
                    objColumnType = objFieldConfigurationMap.get("columntype");
                    log.debug("columnname => " + objColumnName + " columntype => " + objColumnType);
                }
                final String finalObjColumnName = objColumnName;
                final String finalObjColumnType = objColumnType;
                Map<String, String> fieldConfigurationMap = rdsMappingConfiguration.getFieldConfiguration(alias,
                        tableName, field);
                String fieldtype = fieldConfigurationMap.get("fieldtype");
                String columnName = fieldConfigurationMap.get("columnname");
                String columnType = fieldConfigurationMap.get("columntype");
                log.debug("fieldtype => " + fieldtype + " columnname => " + columnName + " columntype => " + columnType);
                if (fieldtype.equalsIgnoreCase("String")) {
                    Optional<String> optionalString = Optional.ofNullable(node.getString(field));
                    optionalString.ifPresent(fieldValue -> {
                        if(isObjectField){
                            log.debug("String-fieldValue=> " + fieldValue);
                            objquery.set(objquery + finalObjColumnName + "= ?,");
                            objParamMap.put(objColumnIndex.getAndIncrement() + "#" + finalObjColumnType, QueryCreationUtility.safeTrim(fieldValue, finalObjColumnName, "OBJECT"));
                            log.debug("upObj table=>"+finalObjColumnName+"=>"+fieldValue);
                        }
                        log.debug("String-fieldValue=> " + fieldValue);
                        query.set(query + columnName + "= ?,");
                        paramMap.put(columnIndex.getAndIncrement() + "#" + columnType, QueryCreationUtility.safeTrim(fieldValue, columnName, tableName));
                        log.debug("upReg table=>"+columnName+"=>"+fieldValue);
                    });
                } else if (fieldtype.equalsIgnoreCase("Date")) {
                    Optional<LocalDate> optionalString = Optional.ofNullable(node.getDate(field));
                    optionalString.ifPresent(fieldValue -> {
                        if(isObjectField){
                            log.debug("String-fieldValue=> " + fieldValue);
                            objquery.set(objquery + finalObjColumnName + "= ?,");
                            objParamMap.put(objColumnIndex.getAndIncrement() + "#" + finalObjColumnType, fieldValue.toString());
                            log.debug("upObj table=>"+finalObjColumnName+"=>"+ fieldValue);
                        }
                        log.debug("Date-fieldValue=> " + fieldValue);
                        query.set(query + columnName + "= ?,");
                        paramMap.put(columnIndex.getAndIncrement() + "#" + columnType, fieldValue.toString());
                        log.debug("upReg table=>"+columnName+"=>"+fieldValue);
                    });
                } else {
                    throw new RuntimeException("Invalid fieldtype => " + fieldtype);
                }
            });
            //order of query execution will probably be OBJECT_R, OBJECT, _R table and finally regular table

            query.updateAndGet(str -> str.substring(0, str.length() - 1) + " WHERE ROOT_VERSION_ID = '" + rootVersionId + "'");
            log.debug("Query added For Reg Table=> " + query.get());
            log.debug("Param Map Size added For Reg Table=> " + paramMap.size());
            queryIdx++;
            log.debug("Search Indx: "+nodeId + "->" + queryIdx);
            //queryList.put(nodeId + "->" + queryIdx + "#" + query.get(), paramMap);
            MonitoringUtility monitoringUtility = loggingUtility.getMonitoringUtility(tableName);
            monitoringUtility.incrementUpdateQuery(queryIdx);
            loggingUtility.setMonitoringUtility(tableName, monitoringUtility);

            int updateCount = queryExecutor.executeUpdate(nodeId + "->" + queryIdx + "#" + query.get(), paramMap);
            if (updateCount>0)
                log.debug(query + " is processed successfully. Update Count => "+updateCount);
            else
                log.info(query + " processing failed. Update Count => "+updateCount);
            if(isObjectPopReq){
                objquery.updateAndGet(str -> str.substring(0, str.length() - 1) + " WHERE ROOT_VERSION_ID = '" + rootVersionId + "'");
                log.debug("Query added For Object Table=> " + objquery.get());
                log.debug("Param Map Size added For Object Table=> " + objParamMap.size());
                objQueryIdx++;
                log.debug("Search Indx: "+nodeId + "->" + objQueryIdx);
                //queryList.put(nodeId + "->" + queryIdx + "#" + query.get(), paramMap);
                monitoringUtility = loggingUtility.getMonitoringUtility("OBJECT");
                monitoringUtility.incrementUpdateQuery(objQueryIdx);
                loggingUtility.setMonitoringUtility("OBJECT", monitoringUtility);
                updateCount = queryExecutor.executeUpdate(nodeId + "->" + objQueryIdx + "#" +
                        objquery.get(), objParamMap);
                if (updateCount>0)
                    log.debug(objquery.get() + " is processed successfully. Update Count => "+updateCount);
                else
                    log.info(objquery.get() + " processing failed. Update Count => "+updateCount);
            }
        }catch (Exception e){
            commonMonitorUtility.incrementErrorCount();
            log.error("Exception in processUpdateQueryForRegularTable method",e);
            e.printStackTrace();
        }
        //return queryList;
    }

}
