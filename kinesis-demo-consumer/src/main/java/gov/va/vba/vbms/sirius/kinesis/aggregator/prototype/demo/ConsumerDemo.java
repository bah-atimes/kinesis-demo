package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo;

import com.amazonaws.SDKGlobalConfiguration;
import gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.engine.ConsumerEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@Slf4j
@SpringBootApplication
public class ConsumerDemo {

	static {
		System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
		System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
		System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "testing123");
		System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "testing123");
	}

	@Autowired
	ConsumerEngine engine;

	public static void main(String[] args) {
		SpringApplication.run(ConsumerDemo.class, args);
	}

	@Bean
	public CommandLineRunner consumerCommandLineRunner(ApplicationContext ctx) {
		return args -> {
			engine.process();
		};
	}
}
