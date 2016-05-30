package no.bekk.eksempel;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

public class SkatteberegningKinesis {

    public void recordHandler(KinesisEvent event) {
        System.out.println("Kinesis-hendelse mottatt. # dataelementer " + event.getRecords().size());
    }

}
