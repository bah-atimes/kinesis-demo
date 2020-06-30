package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.config;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Configuration to make localstack work for the KPL and Kinesis Binder
 */
@Configuration
public class KinesisProducerExampleConfiguration {

    private final static String AWS_REGION = "us-east-1";
    private final static String URL = "https://localhost:4568";

    @Bean
    public AmazonKinesis amazonKinesis() {
        return AmazonKinesisClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(URL, AWS_REGION))
                .build();
    }

    @Bean
    public KinesisProducerConfiguration kinesisProducerConfiguration() throws URISyntaxException {
        URI kinesisUri = new URI(URL);
        return new KinesisProducerConfiguration()
                .setCredentialsProvider(new DefaultAWSCredentialsProviderChain())
                .setRegion(AWS_REGION)
                .setKinesisEndpoint(kinesisUri.getHost())
                .setKinesisPort(kinesisUri.getPort())
                .setVerifyCertificate(false);
    }

    @Bean
    public KinesisProducer kinesisProducer() throws URISyntaxException {
        return new KinesisProducer(kinesisProducerConfiguration());
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
