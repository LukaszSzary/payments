package kafka.payments;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;

@Service
public class ProcessedOrders {
    private final JdbcTemplate jdbc;

    public ProcessedOrders(JdbcTemplate jdbc){
        this.jdbc = jdbc;
    }

    public boolean markProcessed(String orderId){
        try{
            jdbc.update("INSERT INTO processed_orders(order_id, processed_at) VALUES (?,?)",
                    orderId, Timestamp.from(Instant.now()));
            return true;
        } catch (DuplicateKeyException e){return false;}

    }
}
