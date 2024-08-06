package elastic.cara.rds.repository;


import elastic.cara.rds.util.LoggingUtility;
import elastic.cara.rds.util.MonitoringUtility;
import elastic.cara.rds.util.QueryExecutionUtility;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class QueryExecutor {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    MonitoringUtility commonMonitoringUtility;

    @Autowired
    LoggingUtility loggingUtility;

    @Transactional
    public int executeUpdate(String oraQuery, Map<String, String> paramMap){
        try{

            SimpleDateFormat dateFormatWithTime = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss");
            SimpleDateFormat dateFormatWithoutTime = new SimpleDateFormat("yyyy-MM-dd");
            String[] oraQueryArr = oraQuery.split("#");
            Query query = entityManager.createNativeQuery(oraQueryArr[1]);

            for (String key : paramMap.keySet()) {
                String[] keys = key.split("#");
                int idx = Integer.parseInt(keys[0]);
                String colType = keys[1];
                if(colType.equalsIgnoreCase("String")){
                    query.setParameter(idx,paramMap.get(key));
                }else if(colType.equalsIgnoreCase("Date")){
                    Date date = QueryExecutionUtility.parseDate(paramMap.get(key), dateFormatWithTime, dateFormatWithoutTime);
                    query.setParameter(idx, date);
                }else if(colType.equalsIgnoreCase("Int")){
                    int rIdx = Integer.parseInt(paramMap.get(key));
                    query.setParameter(idx, rIdx);
                }else{
                    throw new RuntimeException("Invalid colType");
                }

            }
            return query.executeUpdate();
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception while executing query: {}", oraQuery, e);
            e.printStackTrace();
            return 0;
        }

    }

    @Transactional
    public int executeUpdate(String oraQuery){
        try{
            log.debug("Executing query: {}", oraQuery);
            int count = entityManager.createNativeQuery(oraQuery).executeUpdate();
            return count;
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception while executing query: {}", oraQuery);
            e.printStackTrace();
            return 0;
        }
    }

    @Transactional
    public boolean checkRootObjectIdExist(String tableName, String rootObjectId) {
        String oraQuery = "SELECT COUNT(ROOT_VERSION_ID) FROM " + tableName + " WHERE ROOT_VERSION_ID = :rootObjectId";
        try {
            Number count = (Number) entityManager.createNativeQuery(oraQuery)
                    .setParameter("rootObjectId", rootObjectId)
                    .getSingleResult();
            log.debug("Count for ROOT_VERSION_ID {} in table {}: {}", rootObjectId, tableName, count);
            return count.intValue() > 0;
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            e.printStackTrace();
            log.error("Error checking rootObjectIdExist for table {}: {}", tableName, rootObjectId, e);
            throw new RuntimeException("Exception in checkRootObjectIdExist method");
        }
    }

    @Transactional
    public List<String> getRootVersionIdList(String query){
        Query nativeQuery = entityManager.createNativeQuery(query);
        return nativeQuery.getResultList();
    }

    @Transactional
    public void deleteRootversionIdsFromTable(List<String> rootVersionIdList, String deleteQuery, String tableName){
        MonitoringUtility monitoringUtility = loggingUtility.getMonitoringUtility(tableName);
        for (String value : rootVersionIdList) {
            Query query = entityManager.createNativeQuery(deleteQuery);
            query.setParameter("value", value);
            query.executeUpdate();
            monitoringUtility.incrementDeleteQuery(1);
        }
        loggingUtility.setMonitoringUtility(tableName,monitoringUtility);
    }

    @Transactional
    public Map<String, Integer> getColumnSizeMap(String table) {
        Map<String, Integer> columnSizeMap = new HashMap<>();
        String sql = "SELECT COLUMN_NAME, CHAR_LENGTH FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '"+table+"'";
        Query query = entityManager.createNativeQuery(sql);

        // Execute the query and get results
        List<Object[]> results = query.getResultList();
        for (Object[] row : results) {
            String columnName = (String) row[0];
            Integer charLength = ((Number) row[1]).intValue(); // Cast to Number first, then get the int value
            columnSizeMap.put(columnName, charLength);
        }
        return  columnSizeMap;
    }
}
