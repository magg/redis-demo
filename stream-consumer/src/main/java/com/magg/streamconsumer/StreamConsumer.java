package com.magg.streamconsumer;

import com.magg.model.TransactionModel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@RequiredArgsConstructor
public class StreamConsumer implements StreamListener<String, ObjectRecord<String, TransactionModel>> {

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    private static final long MAX_RETRY = 3L;

    @Value("${stream.consumer-group-name}")
    private String streamConsumerGroupName;

    @Value("${stream.consumer-name}")
    private String streamConsumerName;

    @Value("${stream.key}")
    private String streamKey;
    @Value("${stream.failure-list-key}")
    private String failureList;

    @Override
    @SneakyThrows
    public void onMessage(ObjectRecord<String, TransactionModel> record) {
        log.info(InetAddress.getLocalHost().getHostName() + " - consumed :" + record.getValue());


        log.info("message {} ", record.getValue());

        atomicInteger.incrementAndGet();

        redisTemplate
            .opsForStream()
            .acknowledge(streamConsumerGroupName, record)
            .subscribe(System.out::println);

    }

    @Scheduled(fixedRate = 10000)
    public void showPublishedEventsSoFar(){
        log.info("Total Consumer :: " + atomicInteger.get());
    }


    @Scheduled(fixedRate = 4000)
    public void processPendingMessage() {

        Mono<PendingMessages> messages = redisTemplate.opsForStream().pending(streamKey,
            streamConsumerGroupName, Range.unbounded(),20);
        messages.subscribe(pendingMessages -> pendingMessages.forEach(

            (pendingMessage -> {
                claimMessage(pendingMessage);
                processMessage(pendingMessage);
            }))

        );
    }

    /**
     * claim the message
     *
     * @param pendingMessage
     */
    private void claimMessage(PendingMessage pendingMessage) {


        List<String> recordIds = List.of(pendingMessage.getIdAsString());

        RecordId result = recordIds.stream().map(RecordId::of).toArray(RecordId[]::new)[0];

        redisTemplate.getConnectionFactory().getReactiveConnection().streamCommands().xClaim(
            ByteBuffer.wrap(streamKey.getBytes(StandardCharsets.UTF_8)),
            streamConsumerGroupName,
            streamConsumerName,
            Duration.ofMillis(20),result
           );

        log.info("Message: " + pendingMessage.getIdAsString() + " has been claimed by " + streamConsumerGroupName + ":" + streamConsumerName);
    }

    /**
     * If the maximum retry count lapses, then add the message into error list and acknowledge the message (remove from
     * the pending list).
     * Else process the message and acknowledge it
     */
    private void processMessage(PendingMessage pendingMessage) {

        log.info("Processing pending message by consumer {}", streamConsumerName);

        Flux<MapRecord<String, Object, Object>> messagesToProcess = redisTemplate.opsForStream().range(streamKey,
            Range.closed(pendingMessage.getIdAsString(), pendingMessage.getIdAsString()));

        messagesToProcess
            .subscribe(message -> {
            testProcess(message.getValue(), pendingMessage);
        });
    }

    public void testProcess(Map<Object, Object> kvp, PendingMessage pendingMessage) {


        if (pendingMessage.getTotalDeliveryCount() > MAX_RETRY)
        {
            ack(pendingMessage.getIdAsString());
            redisTemplate.opsForList().rightPush(failureList, kvp.toString());
            log.info("Message has been added into failure list and acknowledged : {}", pendingMessage.getIdAsString());
        } else {
            try {
                String name = (String) kvp.get("name");
                Integer id = Integer.valueOf((String) kvp.get("id"));
                TransactionModel light = new TransactionModel(id, name);

                //process
                log.info("message procssed: {}", light);
                log.info("Message has been processed after retrying");
                ack(pendingMessage.getIdAsString());
            } catch (Exception ex) {
                //log the exception and increment the number of errors count
                Integer id = Integer.valueOf((String) kvp.get("id"));
                log.error("Failed to process the message: {} ",id, ex);
            }
        }

    }

    private void ack(String id) {
        redisTemplate
            .opsForStream()
            .acknowledge(streamKey, streamConsumerGroupName, id)
            .subscribe(System.out::println);
    }

}
