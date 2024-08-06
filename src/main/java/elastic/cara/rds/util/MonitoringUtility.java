package elastic.cara.rds.util;

import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class MonitoringUtility {

    @Getter
    private final AtomicInteger errorCount = new AtomicInteger();

    @Getter
    private final AtomicInteger totalQueriesExecuted = new AtomicInteger();

    @Getter
    private final AtomicInteger insertQuery = new AtomicInteger();

    @Getter
    private final AtomicInteger updateQuery = new AtomicInteger();

    @Getter
    private final AtomicInteger deleteQuery = new AtomicInteger();


    public void incrementErrorCount() {
        errorCount.incrementAndGet();
    }

    public void incrementTotalQueriesExecuted() {
        totalQueriesExecuted.incrementAndGet();
    }

    public void incrementInsertQuery(int queryIdx) {
        insertQuery.addAndGet(queryIdx);
    }

    public void incrementUpdateQuery(int qIdx) {
        updateQuery.addAndGet(qIdx);
    }

    public void incrementDeleteQuery(int qIdx) {
        deleteQuery.addAndGet(qIdx);
    }

}
