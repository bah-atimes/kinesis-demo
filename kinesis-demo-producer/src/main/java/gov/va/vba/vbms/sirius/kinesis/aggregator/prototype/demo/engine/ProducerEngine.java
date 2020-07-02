package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.engine;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.amazonaws.services.kinesis.AmazonKinesis;
import gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.config.ProducerConfig;
import gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.util.ProducerUtils;
import javafx.concurrent.Worker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
@Slf4j
public class ProducerEngine {

    @Autowired
    ProducerUtils util;

    @Autowired
    ProducerConfig config;

    AmazonKinesis amazonKinesis;

    ConcurrentLinkedQueue<AggRecord> queue;

    ProducerEngine(AmazonKinesis kinesis) {
        this.amazonKinesis = kinesis;
    }

    @PostConstruct
    private void createStreamIfMissing() {
        if (!amazonKinesis.listStreams().getStreamNames().contains(config.STREAM_NAME)) {
            amazonKinesis.createStream(config.STREAM_NAME, 1);
            log.info("stream " + config.STREAM_NAME + " created.");
        }
        queue = new ConcurrentLinkedQueue();

        WorkerBee bee = new WorkerBee(queue, amazonKinesis);
        Thread queenBee = new Thread(bee);
        queenBee.start();
    }

    public void generateAndPublish(RecordAggregator aggregator) {

        long count = 1;

        while(true) {
            for (int i = 1; i <= config.RECORDS_TO_TRANSMIT; i++) {

                long claimId = Math.abs(new Random().nextInt());
                byte[] data = util.randomData(count++, claimId, config.RECORD_SIZE_BYTES);

                // addUserRecord returns non-null when a full record is ready to
                // transmit
                try {
                    final AggRecord aggRecord = aggregator.addUserRecord(new Long(claimId % 10).toString(), data);
                    if (aggRecord != null) {
                        queue.add(aggRecord);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("Failed to add user record: " + e.getMessage());
                }
            }
            flushAndFinish(aggregator);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException ie) {
            }
        }
    }

    private void flushAndFinish(RecordAggregator aggregator) {
        AggRecord finalRecord = aggregator.clearAndGet();
        queue.add(finalRecord);
    }
}
