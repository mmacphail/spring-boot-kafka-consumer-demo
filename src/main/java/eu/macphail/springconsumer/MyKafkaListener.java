package eu.macphail.springconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class MyKafkaListener {

    private static final Logger LOG = LoggerFactory.getLogger(MyKafkaListener.class);

    @KafkaListener(id = "spring-consumer", topics = "tracking.energy.rawevent", containerFactory = "batchFactory")
    public void listen(List<String> messages) {
        //LOG.info("Records polled: {}", messages.size());
        for(String message : messages) {
            processMessage(message);
        }
        messagesProcessed.addAndGet(messages.size());
        LOG.info("messages processed: {}", messagesProcessed.get());
        LOG.info("errors: {}", errors.get());
    }

    private final Random random = new Random();

    private final AtomicLong messagesProcessed = new AtomicLong(0L);
    private final AtomicLong errors = new AtomicLong(0L);

    private void processMessage(String message) {
        try {
            Thread.sleep(100); // simulate message processing time
            //LOG.info(message);
            if(random.nextInt(100) == 5) {
                errors.addAndGet(1);
                throw new RuntimeException("got 5");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
