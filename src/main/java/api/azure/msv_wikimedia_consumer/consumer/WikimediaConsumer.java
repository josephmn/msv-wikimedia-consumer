package api.azure.msv_wikimedia_consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class WikimediaConsumer {

    @KafkaListener(topics = "wikimedia-stream", groupId = "myGroup")
    public void consumeMsg(String msg) {
        log.info("Consuming the message from topic -> : wikimedia-stream :: '{}'", msg);
        // Please feel free to do anything you want with the data
    }
}
