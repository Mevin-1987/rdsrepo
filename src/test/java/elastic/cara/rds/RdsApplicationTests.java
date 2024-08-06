package elastic.cara.rds;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class RdsApplicationTests {

	static {
		System.setProperty("test.environment", "true");
	}

	@Test
	void contextLoads() {
	}

}
