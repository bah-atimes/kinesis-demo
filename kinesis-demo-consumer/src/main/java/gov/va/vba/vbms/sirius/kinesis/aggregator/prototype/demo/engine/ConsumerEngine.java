package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.engine;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.*;
import gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.config.ConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class ConsumerEngine {

    @Autowired
    ConsumerConfig config;

    AmazonKinesis amazonKinesis;

    List<Shard> shards;

    ConsumerEngine(AmazonKinesis kinesis) {
        this.amazonKinesis = kinesis;
    }


    @PostConstruct
    public void init() {
        createStreamIfMissing(config.STREAM_NAME);
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName( config.STREAM_NAME );
        String exclusiveStartShardId = null;
        shards = new ArrayList<>();
        do {
            describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
            DescribeStreamResult describeStreamResult = amazonKinesis.describeStream( describeStreamRequest );
            shards.addAll( describeStreamResult.getStreamDescription().getShards() );
            if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            } else {
                exclusiveStartShardId = null;
            }
        } while ( exclusiveStartShardId != null );
    }

    private void createStreamIfMissing(String streamName) {
        if (!amazonKinesis.listStreams().getStreamNames().contains(streamName)) {
            amazonKinesis.createStream(streamName, 1);
            log.info("stream " + streamName + " created.");
        }
    }

    public void process() {
        while(true) {
            init();
            log.info("Number of shards:" + shards.size());
            for(Shard shard: shards) {
                getRecords(shard);
            }
        }
    }

    private void getRecords(Shard shard) {
        String shardIterator = getShardIndicator(shard);

        do {
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(3);

            GetRecordsResult getRecordsResult = amazonKinesis.getRecords(getRecordsRequest);
            shardIterator = getRecordsResult.getNextShardIterator();
            List<Record> records = getRecordsResult.getRecords();

            List<UserRecord> deaggregated = new RecordDeaggregator<Record>().deaggregate(records);
            if(deaggregated.size() > 0) {
                for (UserRecord userRecord : deaggregated) {
                    ByteBuffer buf = userRecord.getData();
                    String s = StandardCharsets.UTF_8.decode(buf).toString().trim();
                    log.info(s);
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        while(shardIterator != null);
    }

    private String getShardIndicator(Shard shard) {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(config.STREAM_NAME);
        getShardIteratorRequest.setShardId(shard.getShardId());
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

        GetShardIteratorResult getShardIteratorResult = amazonKinesis.getShardIterator(getShardIteratorRequest);
        return getShardIteratorResult.getShardIterator();
    }
}
