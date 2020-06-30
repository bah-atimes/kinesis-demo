/**
 * Kinesis Aggregation/Deaggregation Libraries for Java
 *
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.va.vba.vbms.sirius.kinesis.aggregator.prototype.demo.util;

import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;

/**
 * A set of utility functions for use by the sample Kinesis producer functions.
 *
 */
@Component
public class ProducerUtils
{
    // Use this is you want to send the same records every time (useful for testing)
    // private static final Random RANDOM = new Random(0);
    
    // Use this to send random records
    private static final Random RANDOM = new Random();

    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

    /**
     * @return A randomly generated partition key.
     */
    public String randomPartitionKey()
    {
        return UUID.randomUUID().toString();
    }
    
    /**
     * @return A randomly generated explicit hash key.
     */
    public String randomExplicitHashKey() {
        return new BigInteger(128, RANDOM).toString(10);
    }

    /**
     * Generate a new semi-random Kinesis record data byte array.
     * 
     * A sample record is a UTF-8 string that looks like:
     * 
     * RECORD 5 bnasdfnueghlasdhallaeeafelaijfjgwhgczmvc
     * 
     * @param sequenceNumber The sequence number of the record in the overall stream of records.
     * @param claimId dummy data
     * @param desiredLength The desired length of the record.
     * @return Kinesis record data with random alphanumeric characters.
     */
    public byte[] randomData(long sequenceNumber, long claimId, int desiredLength)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("RECORD: ");
        sb.append(sequenceNumber);
        sb.append(" CLAIM ID:");
        sb.append(claimId);
        sb.append(" ");
        while (sb.length() < desiredLength - 1)
        {
            sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
        }
        sb.append("\n");

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
