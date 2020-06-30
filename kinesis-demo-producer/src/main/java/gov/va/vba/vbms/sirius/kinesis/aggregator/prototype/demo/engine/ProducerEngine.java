package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.engine;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.amazonaws.services.kinesis.AmazonKinesis;
import gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.config.ProducerConfig;
import gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.util.ProducerUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Random;

@Component
@Slf4j
public class ProducerEngine {

    @Autowired
    ProducerUtils util;

    @Autowired
    ProducerConfig config;

    AmazonKinesis amazonKinesis;

    ProducerEngine(AmazonKinesis kinesis) {
        this.amazonKinesis = kinesis;
    }

    @PostConstruct
    private void createStreamIfMissing() {
        if (!amazonKinesis.listStreams().getStreamNames().contains(config.STREAM_NAME)) {
            amazonKinesis.createStream(config.STREAM_NAME, 1);
            log.info("stream " + config.STREAM_NAME + " created.");
        }
    }

    public void generateAndPublish(RecordAggregator aggregator) {

        long count = 1;

        while(true) {
//            String ehk = util.randomExplicitHashKey();
            for (int i = 1; i <= config.RECORDS_TO_TRANSMIT; i++) {

                long claimId = Math.abs(new Random().nextInt());
                byte[] data = util.randomData(count++, claimId, config.RECORD_SIZE_BYTES);

                // addUserRecord returns non-null when a full record is ready to
                // transmit
                try {
                    final AggRecord aggRecord = aggregator.addUserRecord(new Long(claimId % 10).toString(), data);
                    if (aggRecord != null) {
                        sendRecord(aggRecord);
//                        ehk = util.randomExplicitHashKey();
//                    ForkJoinPool.commonPool().execute(() -> {
//                        sendRecord(aggRecord);
//                    });
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
        // Do one final flush & send to get any remaining records that haven't
        // triggered a callback yet
        AggRecord finalRecord = aggregator.clearAndGet();
        sendRecord(finalRecord);
//        ForkJoinPool.commonPool().execute(() -> {
//            sendRecord(finalRecord);
//        });
//
//        // Wait up to 2 minutes for all the publisher threads to finish
//        log.info("Waiting for all transmissions to complete...");
//        ForkJoinPool.commonPool().awaitQuiescence(2, TimeUnit.MINUTES);
//        log.info("Transmissions complete.");
    }


    private void sendRecord(AggRecord aggRecord) {
        if (aggRecord == null || aggRecord.getNumUserRecords() == 0) {
            return;
        }

        log.info("Submitting record EHK=" + aggRecord.getExplicitHashKey() + " NumRecords="
                + aggRecord.getNumUserRecords() + " NumBytes=" + aggRecord.getSizeBytes());
        try {
            amazonKinesis.putRecord(aggRecord.toPutRecordRequest(config.STREAM_NAME));
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("Completed record EHK=" + aggRecord.getExplicitHashKey());
    }
}
