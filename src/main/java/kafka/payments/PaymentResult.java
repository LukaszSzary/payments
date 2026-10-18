package kafka.payments;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentResult(
        String orderId,
        String status,
        String paymentId,
        String reason,
        Instant completedAt
        ) {

}
