package com.magg.service;

import com.magg.model.QueueDto;
import com.magg.model.TransactionModel;
import com.magg.repository.QueueRepository;
import com.magg.repository.ZsetRepository;
import io.lettuce.core.RedisBusyException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@RequiredArgsConstructor
public class LoadBalancerConsumer implements
    StreamListener<String, ObjectRecord<String, TransactionModel>>,
    InitializingBean,
    DisposableBean
{
    private StreamMessageListenerContainer<String, ObjectRecord<String, TransactionModel>> listenerContainer;
    private Subscription subscription;
    private final RedisConnectionFactory redisConnectionFactory;

    private AtomicInteger atomicInteger = new AtomicInteger(0);
    private AtomicInteger atomicIntegerPublished = new AtomicInteger(0);


    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ZsetRepository zsetRepository;
    private final QueueRepository queueRepository;

    private final String WORKER_STREAM_PREFIX = "transaction-events-";

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

        loadBalanceQueuesFromMainQueue(record.getValue());

        atomicInteger.incrementAndGet();

        redisTemplate
            .opsForStream()
            .acknowledge(streamConsumerGroupName, record)
            .subscribe(System.out::println);

    }

    @Scheduled(fixedRate = 10000)
    public void showPublishedEventsSoFar(){
        log.info("Total Consumer :: " + atomicInteger.get());
        log.info("Total Produced :: " + atomicIntegerPublished.get());
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

                loadBalanceQueuesFromMainQueue(light);

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

    private void loadBalanceQueuesFromMainQueue(TransactionModel event) {
        String data = event.getName();
        QueueDto queueDto = queueRepository.get(data);
        if (queueDto != null) {

            String streamName = WORKER_STREAM_PREFIX + queueDto.getName();

            publishEvent(event, streamName);

            zsetRepository.incr(queueDto.getName());
        } else {

            Set<String> myset = zsetRepository.getOrdered();
            for (String s :myset) {
                Double res = zsetRepository.get(s);
                log.info("queue {}, score {}", s, res);
            }
            String queue = myset.iterator().next();
            zsetRepository.incr(queue);
            QueueDto dto = new QueueDto(data, queue);
            queueRepository.create(dto);
            String streamName = WORKER_STREAM_PREFIX + dto.getName();


            publishEvent(event, streamName);
        }
        log.info("Data - " + data + " load balanced - ");
    }

    public void publishEvent(TransactionModel event, String streamName){
        log.info("Event Details :: "+event);
        ObjectRecord<String, TransactionModel> record = StreamRecords.newRecord()
            .ofObject(event)
            .withStreamKey(streamName);
        this.redisTemplate
            .opsForStream()
            .add(record)
            .subscribe(System.out::println);
        atomicIntegerPublished.incrementAndGet();
    }


    @Override
    public void destroy() throws Exception
    {
        if (subscription != null) {
            subscription.cancel();
        }

        if (listenerContainer != null) {
            listenerContainer.stop();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, TransactionModel>> options = StreamMessageListenerContainer
            .StreamMessageListenerContainerOptions
            .builder()
            .pollTimeout(Duration.ofSeconds(1))
            .targetType(TransactionModel.class)
            .build();

        this.listenerContainer = StreamMessageListenerContainer
            .create(redisConnectionFactory, options);

        createConsumerGroup(redisConnectionFactory);

        // if you use listenerContainer.receive
        // Every message must be acknowledged using StreamOperations.acknowledge(Object, String, String...) after processing.
        // otherwise, you could use listenerContainer.receiveAutoAck which on
        // Every message is acknowledged when received.
        this.subscription = listenerContainer
            .receive(
                Consumer.from(streamConsumerGroupName, streamConsumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                this);

        subscription.await(Duration.ofSeconds(2));
        listenerContainer.start();

    }

    private void createConsumerGroup(RedisConnectionFactory redisConnectionFactory) {

        try {
            redisConnectionFactory.getConnection()
                .xGroupCreate(streamKey.getBytes(), streamConsumerGroupName, ReadOffset.from("0-0"), true);
        } catch (RedisSystemException exception) {
            if (Objects.requireNonNull(exception.getRootCause()).getClass().equals(RedisBusyException.class))
            {
                log.info("STREAM - Redis group already exists, skipping Redis group creation");
            } else {
                log.warn(exception.getCause().getMessage());
            }
        }
    }
}
