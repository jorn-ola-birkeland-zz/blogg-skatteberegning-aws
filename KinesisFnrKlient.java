package no.bekk.test.aws;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.s3.model.Region;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KinesisFnrKlient {

    public static final int ANTALL_FNR = 100_000;

        public static void main(String[] args) {

        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient();
        amazonKinesisClient.setRegion(Region.EU_Frankfurt.toAWSRegion());

        PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
        putRecordsRequest.setStreamName("FnrStream");
        List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();

        for(int i=0;i<ANTALL_FNR;i++) {

            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            final String fnr = String.format("%03d/%03d", i/1000, i % 1000);

            putRecordsRequestEntry.setData(ByteBuffer.wrap(fnr.getBytes()));
            putRecordsRequestEntry.setPartitionKey(fnr);
            putRecordsRequestEntryList.add(putRecordsRequestEntry);

            if((i+1)%100==0) {
                putRecordsRequest.setRecords(putRecordsRequestEntryList);
                PutRecordsResult putRecordsResult  = amazonKinesisClient.putRecords(putRecordsRequest);
                System.out.println(fnr + ". Feilet: " + putRecordsResult.getFailedRecordCount());

                putRecordsRequestEntryList.clear();
            }
        }
    }
}
