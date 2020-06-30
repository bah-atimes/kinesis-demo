package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.kinesis.agg.RecordAggregator;
import gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.engine.ProducerEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ProducerDemo {

	static {
		System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
		System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
		System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "testing123");
		System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "testing123");
	}

	@Autowired
	ProducerEngine engine;

	public static void main(String[] args) {
		SpringApplication.run(ProducerDemo.class, args);
	}


	/**
	 * Generates {@value #} events via KPL
	 * @param ctx the app context
	 * @return commandlinerunner
	 */
	@Bean
	public CommandLineRunner commandProducerLineRunner(ApplicationContext ctx) {
		return args -> {
			final RecordAggregator aggregator = new RecordAggregator();
			engine.generateAndPublish(aggregator);
		};
	}
}
