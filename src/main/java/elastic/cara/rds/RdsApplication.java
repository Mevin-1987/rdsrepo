package elastic.cara.rds;

import elastic.cara.rds.config.RdsConfiguration;
import elastic.cara.rds.util.MonitoringUtility;
import elastic.cara.rds.util.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;

import java.util.UUID;

@SpringBootApplication
@EnableConfigurationProperties(RdsConfiguration.class)
@Slf4j
public class RdsApplication implements ApplicationRunner {

	@Autowired
	UtilityClass utilityClass;

	@Autowired
	MonitoringUtility monitoringUtility;

	@Autowired
	private ApplicationContext applicationContext;

	public static void main(String[] args) {
		// Generate GUID and put it in MDC
		SpringApplication.run(RdsApplication.class, args);
	}



	@Override
	public void run(ApplicationArguments args) {
		try{
			String guid = UUID.randomUUID().toString();
			MDC.put("guid", guid);
			utilityClass.updateCaraResponseInRds();
			MDC.remove("guid");
		}catch(Exception e){
			monitoringUtility.incrementErrorCount();
			log.error("Exception in RdsApplication",e);
			e.printStackTrace();
		}finally {
			// Gracefully shut down the application context
			if (!isTestEnvironment()) {
				// Gracefully shut down the application context
				SpringApplication.exit(applicationContext, () -> 0);
			}
		}

	}

	private boolean isTestEnvironment() {
		// Check for a specific system property or environment variable to determine if it's a test environment
		String testEnv = System.getProperty("test.environment");
		return testEnv != null && testEnv.equalsIgnoreCase("true");
	}
}
