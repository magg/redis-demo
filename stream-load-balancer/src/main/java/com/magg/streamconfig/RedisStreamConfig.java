package com.magg.streamconfig;


import com.magg.model.TransactionModel;
import io.lettuce.core.RedisBusyException;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.net.UnknownHostException;
import java.time.Duration;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class RedisStreamConfig
{
    @Value("${stream.key}")
    private String streamKey;

    @Value("${stream.consumer-group-name}")
    private String streamConsumerGroupName;

    @Value("${stream.consumer-name}")
    private String streamConsumerName;

    private final StreamListener<String, ObjectRecord<String, TransactionModel>> streamListener;

    @Bean
    public Subscription subscription(RedisConnectionFactory redisConnectionFactory) throws UnknownHostException {

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, TransactionModel>> options = StreamMessageListenerContainer
            .StreamMessageListenerContainerOptions
            .builder()
            .pollTimeout(Duration.ofSeconds(1))
            .targetType(TransactionModel.class)
            .build();


        StreamMessageListenerContainer<String, ObjectRecord<String, TransactionModel>>  listenerContainer = StreamMessageListenerContainer
            .create(redisConnectionFactory, options);

        createConsumerGroup(redisConnectionFactory);

        // if you use listenerContainer.receive
        // Every message must be acknowledged using StreamOperations.acknowledge(Object, String, String...) after processing.
        // otherwise, you could use listenerContainer.receiveAutoAck which on
        // Every message is acknowledged when received.
        Subscription subscription = listenerContainer
            .receive(
                Consumer.from(streamConsumerGroupName, streamConsumerName),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                streamListener);

        listenerContainer.start();
        return subscription;
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
