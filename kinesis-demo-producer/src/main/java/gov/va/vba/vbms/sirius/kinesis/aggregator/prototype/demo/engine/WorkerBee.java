package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.engine;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.services.kinesis.AmazonKinesis;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class WorkerBee implements Runnable {

    AmazonKinesis amazonKinesis;
    ConcurrentLinkedQueue<AggRecord> queue;


    public WorkerBee(ConcurrentLinkedQueue<AggRecord> queue, AmazonKinesis kinesis) {
        this.queue = queue;
        this.amazonKinesis = kinesis;
    }

    public void run() {
        while (true) {
            log.info("Hello from a thread!");

            if(queue.size() > 0) {
                Iterator<AggRecord> iter = queue.iterator();
                while(iter.hasNext()) {
                    AggRecord aggRecord = iter.next();
                    sendRecord(aggRecord);
                    iter.remove();
                }
            }

            try { Thread.sleep(1000); } catch (InterruptedException ie) {}
        }
    }

    private void sendRecord(AggRecord aggRecord) {
        if (aggRecord == null || aggRecord.getNumUserRecords() == 0) {
            return;
        }

        log.info("Submitting record EHK=" + aggRecord.getExplicitHashKey() + " NumRecords="
                + aggRecord.getNumUserRecords() + " NumBytes=" + aggRecord.getSizeBytes());
        try {
            synchronized (amazonKinesis) {
                amazonKinesis.putRecord(aggRecord.toPutRecordRequest("test_stream"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("Completed record EHK=" + aggRecord.getExplicitHashKey());
    }

}
