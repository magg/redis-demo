package com.magg.repository;

import com.magg.model.QueueDto;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;


@Repository
@Slf4j
public class QueueRepository
{

    public static final String HASH_NAME = "statements.queue.hash";

    private HashOperations<String, String, QueueDto> hashOperations;

    public QueueRepository(RedisTemplate<String, QueueDto> queueTemplate) {
        this.hashOperations = queueTemplate.opsForHash();
    }

    public void create(QueueDto queueDto) {
        hashOperations.put(HASH_NAME, queueDto.getValue(), queueDto);
        log.info(String.format("queue with name %s saved", queueDto.getValue()));
    }

    public QueueDto get(String value) {
        return (QueueDto) hashOperations.get(HASH_NAME, value);
    }

    public Map<String, QueueDto> getAll(){
        return hashOperations.entries(HASH_NAME);
    }

    public void update(QueueDto queueDto) {
        hashOperations.put(HASH_NAME, queueDto.getValue(), queueDto);
        log.info(String.format("queue with ID %s updated", queueDto.getValue()));
    }

    public void delete(String name) {
        hashOperations.delete(HASH_NAME, name);
        log.info(String.format("queue with ID %s deleted", name));
    }
}
