package ske.fastsetting.skatt;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

import java.util.*;
import java.util.stream.Stream;

import static com.amazonaws.services.s3.model.Region.EU_Frankfurt;

public class SkatteberegningDynamoDBKinesis {

    private static final int MAX_FNR = 25;
    private JaxbXmlMapper jaxbXmlMapper = new JaxbXmlMapper();

    public void recordHandler(KinesisEvent event) {

        System.out.println("Kinesis-hendelse mottatt. # dataelementer: " + event.getRecords().size());

        long start = System.currentTimeMillis();

        String[] fnrArray = event.getRecords().stream()
                .map(rec -> rec.getKinesis())
                .map(kinesis -> kinesis.getData().array())
                .map(bytes -> new String(bytes))
                .flatMap(s -> Stream.of(s.split(",")))   // Splitt komma-separerte fødselsnumre
                .toArray(size->new String[size]);

        final DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient().withRegion(EU_Frankfurt.toAWSRegion()));

        for (String[] fnrArrayDel : delArray(fnrArray, MAX_FNR)) {
            new Skatteberegner(dynamoDB, fnrArrayDel, jaxbXmlMapper).beregn();
        }

        final long ms = System.currentTimeMillis() - start;
        System.out.println("Kinesis-hendelse behandlet på " + ms + " ms, dvs " + ms / fnrArray.length + " ms/fnr");
    }

    private static Collection<String[]> delArray(String[] array, int antall) {
        List<String[]> lists = new ArrayList<>();
        for (int i = 0; i < array.length; i += antall) {
            int end = Math.min(array.length, i + antall);
            lists.add(Arrays.copyOfRange(array, i, end));
        }

        return lists;
    }
}
