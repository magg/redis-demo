package com.magg.publisher;

import com.magg.model.TransactionModel;
import com.magg.repository.EventRepository;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventPublisher
{
    private AtomicInteger atomicInteger = new AtomicInteger(0);

    @Value("${stream.key}")
    private String streamKey;

    private final EventRepository eventRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public EventPublisher(EventRepository eventRepository, ReactiveRedisTemplate<String, String> redisTemplate) {
        this.eventRepository = eventRepository;
        this.redisTemplate = redisTemplate;
    }

    public void publishEvent(TransactionModel event){
        log.info("Event Details :: "+event);
        ObjectRecord<String, TransactionModel> record = StreamRecords.newRecord()
            .ofObject(event)
            .withStreamKey(streamKey);
        this.redisTemplate
            .opsForStream()
            .add(record)
            .subscribe(System.out::println);
        atomicInteger.incrementAndGet();
    }

    @Scheduled(fixedRateString= "${publish.rate}")
    public void publish() {
        //TransactionModel transactionModel = this.eventRepository.getRandomTransactionModel();

        int index = atomicInteger.get();

        if (index < 25) {
            TransactionModel transactionModel = this.eventRepository.geTransactionModel(index);
            log.info("transactionModel Details :: "+transactionModel);
            publishEvent(transactionModel);
        }

    }

    @Scheduled(fixedRate = 10000)
    public void showPublishedEventsSoFar(){
        log.info("Total Events :: " +atomicInteger.get());
    }
}

