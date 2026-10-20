package kafka.payments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Component
public class OrderListener {
    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProcessedOrders processedOrders;

    public OrderListener(KafkaTemplate<String, Object> kafkaTemplate, ProcessedOrders processedOrders) {
        this.kafkaTemplate = kafkaTemplate;
        this.processedOrders = processedOrders;
    }

    @KafkaListener(topics = "orders")
    @Transactional
    public void onOrderCreated(OrderCreated order) throws Exception {
        if (!processedOrders.markProcessed(order.orderId())) {
            log.info("Skipping duplicate order {}", order.orderId());
            return;
        }

        if (order.totalAmount().compareTo(new BigDecimal("1000")) > 0){
            var failed = new PaymentResult(order.orderId(), "failed" ,UUID.randomUUID().toString(),"amount > 1000", Instant.now());
            kafkaTemplate.send("payments", order.orderId(),failed).get();
            log.info("Payment FAILED for {}", order.orderId());
        }else{
            var done = new PaymentResult(order.orderId(), "done" ,UUID.randomUUID().toString(), null, Instant.now());
            kafkaTemplate.send("payments", order.orderId(), done).get();
            log.info("Payment COMPLETED for {}", order.orderId());

        }
    }
}
