package kafka.payments;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentResult(
        String orderId,
        String status,
        String paymentId,
        BigDecimal amount,
        Instant completedAt
        ) {

}
