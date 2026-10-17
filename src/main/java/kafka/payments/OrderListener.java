package kafka.payments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Component
public class OrderListener {
    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderListener(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "orders")
    public void onOrderCreated(OrderCreated order){
        if (order.totalAmount().compareTo(new BigDecimal("1000")) > 0){
            var failed = new PaymentResult(order.orderId(), "failed" ,UUID.randomUUID().toString(), order.totalAmount(), Instant.now());
            kafkaTemplate.send("payments", order.orderId(),failed);
            log.info("Payment FAILED for {}", order.orderId());
        }else{
            var done = new PaymentResult(order.orderId(), "done" ,UUID.randomUUID().toString(), order.totalAmount(), Instant.now());
            kafkaTemplate.send("payments", order.orderId(), done);
            log.info("Payment COMPLETED for {}", order.orderId());

        }
    }
}
