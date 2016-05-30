package ske.fastsetting.skatt;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static com.amazonaws.services.s3.model.Region.EU_Frankfurt;

public class DynamoDBRessursopplasterMultiTraad {

    public static final int ANTALL_TRAADER = 20;
    public static final int ANTALL_FNR = 1_000_000;

    private final BlockingQueue<Item> queue = new ArrayBlockingQueue<>(2000);
    private boolean fyllerKoe = true;

    public DynamoDBRessursopplasterMultiTraad() {
    }

    public static void main(String[] args) throws InterruptedException {
        new DynamoDBRessursopplasterMultiTraad().lastOppRessursTilTabell("skattegrunnlag", "Skattegrunnlag");
        new DynamoDBRessursopplasterMultiTraad().lastOppRessursTilTabell("skatteplikt", "Skatteplikt");
    }

    private void lastOppRessursTilTabell(String ressursnavn, String tabell) throws InterruptedException {

        byte[] dokument = hentRessurs(ressursnavn);

        ExecutorService executor = Executors.newFixedThreadPool(ANTALL_TRAADER);

        IntStream.range(0, ANTALL_TRAADER)
                .forEach(i -> executor.submit(new DynamoDBOpplaster(tabell, this)));

        IntStream.range(0, ANTALL_FNR)
                .mapToObj(i -> String.format("%03d/%03d", i / 1000, i % 1000))
                .map(fnr -> new Item().withPrimaryKey("fnr", fnr).withBinary("dok", dokument))
                .forEach(this::puttPaaKoe);

        fyllerKoe = false;

        executor.shutdown();
    }


    private void puttPaaKoe(Item item) {
        try {
            queue.put(item);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public List<Item> taMaks(int maxAntall) {
        List<Item> items = new ArrayList<>();

        while (items.size() < maxAntall) {
            try {
                items.add(queue.take());
            } catch (InterruptedException e) {
                break;
            }
        }

        return items;
    }

    public boolean kjoerer() {
        return this.fyllerKoe || queue.size() > 0;
    }

    private static class DynamoDBOpplaster implements Runnable {
        private final DynamoDB dynamoDB;
        private final String tabell;
        private DynamoDBRessursopplasterMultiTraad master;

        public DynamoDBOpplaster(String tabell, DynamoDBRessursopplasterMultiTraad master) {
            this.master = master;
            this.tabell = tabell;

            dynamoDB = new DynamoDB(new AmazonDynamoDBClient().withRegion(EU_Frankfurt.toAWSRegion()));
        }

        @Override
        public void run() {
            while (master.kjoerer()) {
                dynamoDB.batchWriteItem(new TableWriteItems(tabell).withItemsToPut(master.taMaks(25)));
            }
        }
    }


    private static byte[] hentRessurs(String ressursnavn) {

        InputStream is = DynamoDBRessursopplasterMultiTraad.class.getResourceAsStream("/" + ressursnavn);

        byte[] buffer = new byte[2048];
        int bytesRead;
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try {
            while ((bytesRead = is.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return output.toByteArray();
    }

}
