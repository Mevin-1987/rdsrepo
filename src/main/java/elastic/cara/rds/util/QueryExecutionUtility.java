package elastic.cara.rds.util;

import lombok.Getter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class QueryExecutionUtility {


    // Method to parse date using multiple formats
    public static Date parseDate(String dateStr, SimpleDateFormat... formatters) throws ParseException {
        ParseException lastException = null;
        for (SimpleDateFormat formatter : formatters) {
            try {
                return formatter.parse(dateStr);
            } catch (ParseException e) {
                lastException = e;
            }
        }
        throw lastException; // Rethrow the last exception if no format worked
    }



}
