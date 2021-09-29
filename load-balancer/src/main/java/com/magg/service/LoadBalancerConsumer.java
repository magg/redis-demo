package com.magg.service;

import com.magg.model.QueueDto;
import com.magg.repository.QueueRepository;
import com.magg.repository.ZsetRepository;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LoadBalancerConsumer
{

    private static final String LIST_NAME = "demo_list";
    private final String PENDING_WORKER_LIST_NAME = "statements.pending.queue.";

    private final StringRedisTemplate redisTemplate;
    private final RedisTemplate<String, QueueDto> queueTemple;

    private final ListOperations<String, String> listOps;
    private final QueueRepository queueRepository;
    private final ZsetRepository zsetRepository;

    Queue<Integer> q = new LinkedList<>();


    public LoadBalancerConsumer(
        StringRedisTemplate redisTemplate,
        RedisTemplate<String, QueueDto> queueTemple,
        QueueRepository queueRepository,
        ZsetRepository zsetRepository) {
        this.redisTemplate = redisTemplate;
        this.queueTemple = queueTemple;
        this.listOps = redisTemplate.opsForList();
        this.queueRepository = queueRepository;
        this.zsetRepository = zsetRepository;
        q.add(1);
        q.add(2);
    }

    @Scheduled(fixedRate = 5000)
    public void listen() {

        String queueName = "statements.pending.load.balance";
        try {
            String data = listOps.rightPop(LIST_NAME);

            Thread.sleep(5000);
            if (data != null && !data.isEmpty()) {
                loadBalanceQueuesFromMainQueue(data);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    private void loadBalanceQueuesFromMainQueue(String data) {
        QueueDto queueDto = queueRepository.get(data);

        if (queueDto != null) {
            listOps.leftPush(PENDING_WORKER_LIST_NAME +queueDto.getName(), data);
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
            listOps.leftPush(PENDING_WORKER_LIST_NAME+dto.getName(), data);
        }
        log.info("Data - " + data + " load balanced - ");
    }

    private void loadBalanceQueuesFromPendingQueue(String data) {
        QueueDto queueDto = queueRepository.get(data);

        if (queueDto != null) {
            listOps.leftPush(PENDING_WORKER_LIST_NAME +queueDto.getName(), data);
            zsetRepository.incr(queueDto.getName());
        } else {
            Set<String> myset = zsetRepository.getOrdered();
            myset.forEach(System.out::println);
            String queue = myset.iterator().next();
            zsetRepository.incr(queue);
            QueueDto dto = new QueueDto(data, queue);
            queueRepository.create(dto);
            listOps.leftPush(PENDING_WORKER_LIST_NAME+dto.getName(), data);
        }
        log.info("Data - " + data + " load balanced - ");
    }



}
