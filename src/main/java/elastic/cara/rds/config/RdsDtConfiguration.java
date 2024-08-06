package elastic.cara.rds.config;


import elastic.cara.rds.RdsApplication;
import elastic.cara.rds.util.MonitoringUtility;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.time.LocalDateTime;
import java.util.Properties;

@Component
@Slf4j
public class RdsDtConfiguration {

    @Autowired
    RdsConfiguration rdsConfiguration;

    private Properties properties;

    String propertiesFilePath;

    @Autowired
    MonitoringUtility commonMonitoringUtility;

    @PostConstruct
    public void init(){
        try {
            properties = new Properties();
            this.propertiesFilePath = rdsConfiguration.getLastupdateddtpropertiesfile();
            File file = new File(propertiesFilePath);
            if (!file.exists()) {
                String jarDir = new File(RdsApplication.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
                propertiesFilePath = jarDir + File.separator + propertiesFilePath;
                file = new File(propertiesFilePath);
            }
            try (FileInputStream in = new FileInputStream(file)) {
                properties.load(in);
            } catch (Exception e) {
                commonMonitoringUtility.incrementErrorCount();
                log.error("Exception occurred while loading properties from file: {}", propertiesFilePath, e);
            }
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception occurred while loading properties from file: {}", propertiesFilePath, e);
        }
    }

    public LocalDateTime getLastUpdatedDT(){
        return LocalDateTime.parse(properties.getProperty("config.mapping.rdslastupdateddatetime"));
    }

    public void setLastUpdatedDT(LocalDateTime date){
        try {
            properties.setProperty("config.mapping.rdslastupdateddatetime", date.toString());
            File file = new File(propertiesFilePath);
            if (!file.exists()) {
                String jarDir = new File(RdsApplication.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
                propertiesFilePath = jarDir + File.separator + propertiesFilePath;
                file = new File(propertiesFilePath);
            }
            try (FileOutputStream out = new FileOutputStream(file)) {
                properties.store(out, null);
            } catch (Exception e) {
                commonMonitoringUtility.incrementErrorCount();
                log.error("Exception occurred while storing properties to file: {}", propertiesFilePath, e);
            }
        }catch(Exception e){
            commonMonitoringUtility.incrementErrorCount();
            log.error("Exception occurred while storing properties to file: {}", propertiesFilePath, e);
        }
    }
}
