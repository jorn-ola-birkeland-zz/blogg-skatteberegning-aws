package ske.fastsetting.skatt;

import com.amazonaws.services.dynamodbv2.document.*;
import ske.fastsetting.manntall.skatteplikt.v1_0.SkattesubjektXml;
import ske.fastsetting.skatt.beregnetskatt.v1_0.BeregnetSkattXml;
import ske.fastsetting.skatt.beregnetskatt.v1_0.ObjectFactory;
import ske.fastsetting.skatt.forskudd.SkattyterKontekstForskuddBuilder;
import ske.fastsetting.skatt.skatteberegning.SkatteberegningService;
import ske.fastsetting.skatt.skattegrunnlag.v2_0.SkattegrunnlagXml;
import ske.fastsetting.skatt.uttrykk.domene.SkattyterKontekst;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class Skatteberegner {

    private static final String SKATTEGRUNNLAG_TABLE = "Skattegrunnlag";
    private static final String SKATTEPLIKT_TABLE = "Skatteplikt";
    private static final String BEREGNETSKATT_TABLE = "BeregnetSkatt";

    private final String[] fnrArray;
    private final DynamoDB dynamoDB;
    private final JaxbXmlMapper jaxbXmlMapper;

    Skatteberegner(DynamoDB dynamoDB, String[] fnrArray, JaxbXmlMapper jaxbXmlMapper) {
        this.jaxbXmlMapper = jaxbXmlMapper;
        this.fnrArray = fnrArray;
        this.dynamoDB = dynamoDB;
    }

    public void beregn() {

        long start = System.currentTimeMillis();

        System.out.println("Leser grunnlag fra DynamoDB: " + fnrArray.length);

        TableKeysAndAttributes skattegrunnlagTableKeysAndAttributes = new TableKeysAndAttributes(SKATTEGRUNNLAG_TABLE);
        TableKeysAndAttributes skattepliktTableKeysAndAttributes = new TableKeysAndAttributes(SKATTEPLIKT_TABLE);

        for (String fnr : fnrArray) {
            PrimaryKey pk = new PrimaryKey("fnr", fnr);

            skattegrunnlagTableKeysAndAttributes.addPrimaryKey(pk);
            skattepliktTableKeysAndAttributes.addPrimaryKey(pk);
        }

        BatchGetItemOutcome outcome = dynamoDB.batchGetItem(skattegrunnlagTableKeysAndAttributes, skattepliktTableKeysAndAttributes);

        System.out.println("Lest grunnlag fra DynamoDB: " + fnrArray.length + " stk " + (System.currentTimeMillis() - start) + " ms");

        if (outcome.getUnprocessedKeys().size() > 0) {
            System.out.println("Fikk ikke alle grunnlag. Rekjører IKKE. Mangler: " + outcome.getUnprocessedKeys());
        }

        start = System.currentTimeMillis();

        Map<String, SkattegrunnlagXml> skattegrunnlagMap = outcome.getTableItems().get(SKATTEGRUNNLAG_TABLE).stream()
                .collect(Collectors.toMap(
                        item -> item.getString("fnr"),
                        item -> jaxbXmlMapper.fraByteArray(item.getBinary("dok"), SkattegrunnlagXml.class)
                ));

        Map<String, SkattesubjektXml> skattepliktMap = outcome.getTableItems().get(SKATTEPLIKT_TABLE).stream()
                .collect(Collectors.toMap(
                        item -> item.getString("fnr"),
                        item -> jaxbXmlMapper.fraByteArray(item.getBinary("dok"), SkattesubjektXml.class)
                ));

        System.out.println("Deserialisert grunnlag: " + skattepliktMap.size() + " skatteplikt, " + skattegrunnlagMap.size() + " skattegrunnlag. " + (System.currentTimeMillis() - start) + " ms");

        start = System.currentTimeMillis();

        List<Item> items = new ArrayList<>();

        for (String fnr : fnrArray) {

            if (!skattegrunnlagMap.containsKey(fnr) || !skattepliktMap.containsKey(fnr)) {
                System.out.println("Mangler grunnlag for fnr " + fnr);
                continue;
            }

            SkattyterKontekst skattyterKontekst = SkattyterKontekstForskuddBuilder.forForskuddAar()
                    .medSkattegrunnlag(skattegrunnlagMap.get(fnr))
                    .medSkattesubjekt(skattepliktMap.get(fnr))
                    .bygg();

            BeregnetSkattXml beregnetSkattXml = SkatteberegningService.beregnSkatt(skattyterKontekst, false);

            byte[] data = jaxbXmlMapper.tilByteArray(
                    new ObjectFactory().createBeregnetSkatt(beregnetSkattXml),
                    BeregnetSkattXml.class);

            items.add(new Item().withPrimaryKey("fnr", fnr).withBinary("dok", data));
        }

        final long tid = System.currentTimeMillis() - start;
        System.out.println("Ferdig beregnet: " + fnrArray.length + " stk på " + tid + " ms, altså " + tid / fnrArray.length + " ms/fnr");

        start = System.currentTimeMillis();

        TableWriteItems beregnetSkattWriteItems = new TableWriteItems(BEREGNETSKATT_TABLE).withItemsToPut(items);

        dynamoDB.batchWriteItem(beregnetSkattWriteItems);

        System.out.println("Lagret til " + BEREGNETSKATT_TABLE + " " + (System.currentTimeMillis() - start) + " ms");
    }
}
