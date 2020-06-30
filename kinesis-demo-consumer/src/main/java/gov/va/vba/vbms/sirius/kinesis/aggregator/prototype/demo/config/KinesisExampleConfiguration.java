package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.config;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration to make localstack work for the KPL and Kinesis Binder
 */
@Configuration
public class KinesisExampleConfiguration {

    private final static String AWS_REGION = "us-east-1";
    private final static String URL = "https://localhost:4568";

    @Bean
    public AmazonKinesis amazonKinesis() {
        return AmazonKinesisClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(URL, AWS_REGION))
                .build();
    }
}
