package elastic.cara.rds.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Configuration
@ConfigurationProperties(prefix="config.mapping")
@Slf4j
@Data
public class RdsConfiguration {

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    LocalDateTime rdslastupdateddatetime;

    String lastupdateddtpropertiesfile;

    String rdscaramappingfile;

    String rdscaraobjectmappingfile;

    String caraurl;

    int caraqueryrequestlimit;

    String token;

    String secret;

}
