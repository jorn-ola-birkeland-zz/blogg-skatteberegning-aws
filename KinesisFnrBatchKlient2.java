package ske.fastsetting.skatt;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.s3.model.Region;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;

/**
 * Created by jorn ola birkeland on 08.04.16.
 */
public class KinesisFnrBatchKlient {

    public static final int FNR_PER_ENTRY = 100;
    public static final int ENTRIES_PER_REQUEST = 100;

    private final int startFnr;
    private final int sluttFnr;
    private final String streamNavn;

    public KinesisFnrBatchKlient(int startFnr, int sluttFnr, String streamNavn) {
        this.startFnr = startFnr;
        this.sluttFnr = sluttFnr;
        this.streamNavn = streamNavn;
    }


    public static void main(String[] args) {
        new KinesisFnrBatchKlient(0,500_000,"FnrStream1").kjoer();
        new KinesisFnrBatchKlient(500_000,1_000_000,"FnrStream2").kjoer();
    }

    private void kjoer() {

        System.out.println("Starter skriving til "+streamNavn+". Fra "+startFnr+" til "+sluttFnr+ " fødselsnummer");
        long start = System.currentTimeMillis();

        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient();
        amazonKinesisClient.setRegion(Region.EU_Frankfurt.toAWSRegion());

        Random rnd = new Random();

        final int antallFnr = sluttFnr-startFnr;

        IntStream.range(startFnr, sluttFnr)
                .mapToObj(i -> String.format("%03d/%03d", i / 1000, i % 1000))
                .collect(groupingBy(s -> rnd.nextInt(antallFnr/ FNR_PER_ENTRY))).values().stream()
                .map(cs -> cs.stream().collect(joining(",")))
                .map(KinesisFnrBatchKlient::tilPutRecordsRequestEntry)
                .collect(groupingBy(pre -> rnd.nextInt(antallFnr/(FNR_PER_ENTRY * ENTRIES_PER_REQUEST)))).values().stream()
                .map(recs->KinesisFnrBatchKlient.tilPutRecordRequest(streamNavn,recs))
                .map(amazonKinesisClient::putRecords)
                .forEach(prr -> System.out.println("Feilet: " + prr.getFailedRecordCount()));


        long slutt = System.currentTimeMillis()-start;
        System.out.println("Skrevet "+antallFnr+ " fødselsnummer på "+slutt+ " ms, dvs " + (antallFnr*1000/slutt)+ " fnr/s");
    }

    private static PutRecordsRequestEntry tilPutRecordsRequestEntry(String data) {
        PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();

        putRecordsRequestEntry.setData(ByteBuffer.wrap(data.getBytes()));
        putRecordsRequestEntry.setPartitionKey(UUID.randomUUID().toString());

        return putRecordsRequestEntry;
    }

    private static PutRecordsRequest tilPutRecordRequest(String streamNavn, List<PutRecordsRequestEntry> records) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamNavn);
        putRecordsRequest.setRecords(records);

        return putRecordsRequest;
    }
}

