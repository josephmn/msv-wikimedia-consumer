package api.azure.msv_wikimedia_consumer.consumer;

//import api.azure.msv_wikimedia_consumer.entity.WikimediaChangeEventJdbc;
import api.azure.msv_wikimedia_consumer.entity.WikimediaChangeEventReactive;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

//import java.sql.Timestamp;
import java.time.Instant;
//import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class WikimediaConsumer {
    /**
     * The {@code ObjectMapper} instance is used for converting JSON strings into Java objects
     * and vice versa. It plays a crucial role in the deserialization process of messages
     * received from Kafka, allowing them to be easily processed and stored in the database.
     */
//    private final ObjectMapper objectMapper;
//    private final JdbcTemplate jdbcTemplate;
//
//    @KafkaListener(topics = "wikimedia-stream", groupId = "myGroup")
//    public void consumeMsg(String msg) {
//        log.info("Consuming the message from topic -> : wikimedia-stream :: '{}'", msg);
//        // Please feel free to do anything you want with the data
//
//        try {
//            WikimediaChangeEventJdbc event = objectMapper.readValue(msg, WikimediaChangeEventJdbc.class);
//
//            String sql = "INSERT INTO wikimedia_changes (id, vtitle, vuser, timestamp, vtype, comment) VALUES (?, ?, ?, ?, ?, ?)";
//            jdbcTemplate.update(sql,
//                    event.getId(),
//                    event.getTitle(),
//                    event.getUser(),
//                    new Timestamp(event.getTimestamp() * 1000),
//                    event.getType(),
//                    event.getComment()
//            );
//
//            System.out.println("Event inserted into database: " + event.getId());
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
//    }

    /**
     * The {@code DatabaseClient} instance is utilized for performing reactive database operations
     * such as executing SQL queries asynchronously. It is an essential component in the process
     * of storing parsed Wikimedia change events into the database in a non-blocking manner.
     */
    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;

    private static final Logger logger = LoggerFactory.getLogger(WikimediaConsumer.class);

    @KafkaListener(topics = "wikimedia-stream", groupId = "myGroup")
    public Mono<Void> consume(String msg) {
        return Mono.just(msg)
                .flatMap(this::parseEvent)
                .flatMap(this::insertEvent)
                .onErrorResume(e -> {
                    log.error("Error while processing message: {}", msg, e);
                    return Mono.empty();
                });
    }

    private Mono<WikimediaChangeEventReactive> parseEvent(String message) {
        try {
            WikimediaChangeEventReactive event = objectMapper.readValue(message, WikimediaChangeEventReactive.class);
            event.generateEventId();
            return Mono.just(event);
        } catch (Exception e) {
            return Mono.empty(); // Retorna un Mono vacío en caso de error para no detener el flujo.
        }
    }

    private Mono<Void> insertEvent(WikimediaChangeEventReactive event) {
        logger.debug("Inserting event: {}", event);
        // UUID id = UUID.randomUUID();
        // event.setEventId(id);
        try {
            // Parsear y almacenar el evento usando DatabaseClient
            DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("INSERT INTO wikimedia_changes_flux (id, veventid, vtitle, vuser, vtimestamp, vtype, comment) VALUES (:id, :eventId, :title, :user, :timestamp, :type, :comment)")
                .bind("id", event.getId())
                .bind("eventId", event.getEventId())
                .bind("title", event.getTitle())
                .bind("timestamp", Instant.ofEpochSecond(event.getTimestamp()))
                .bind("type", event.getType())
                .bind("comment", event.getComment());
            if (event.getUser() != null) {
                spec = spec.bind("user", event.getUser()); // when get user
            } else {
                spec = spec.bindNull("user", String.class); // when get user null, use bindNull
            }
            return spec.fetch()
                .rowsUpdated()
                .doOnSuccess(count -> logger.debug("Inserted {} rows", count))
                .then();
        } catch (Exception e) {
            return Mono.error(e);
        }
    }
}
