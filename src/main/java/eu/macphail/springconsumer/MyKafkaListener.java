package eu.macphail.springconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MyKafkaListener {

    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaListener.class);

    @KafkaListener(id = "spring-consumer", topics = "tracking.energy.rawevent", containerFactory = "batchFactory")
    public void listen(List<String> messages) {
        for(String message : messages) {
            processMessage(message);
        }
    }

    private void processMessage(String message) {
        try {
            Thread.sleep(100); // simulate message processing time
            LOG.info(message);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
