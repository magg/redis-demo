package com.magg.service;

import com.magg.model.QueueDto;
import com.magg.repository.QueueRepository;
import com.magg.repository.ZsetRepository;
import java.time.Duration;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@Scope("application")
@Slf4j
public class LoadBalancerConsumer implements Runnable
{

    private static final String LIST_NAME = "demo_list";
    private final String PENDING_WORKER_LIST_NAME = "statements.pending.queue.";

    private final StringRedisTemplate redisTemplate;
    private final RedisTemplate<String, QueueDto> queueTemple;

    private final ListOperations<String, String> listOps;
    private final QueueRepository queueRepository;
    private final ZsetRepository zsetRepository;

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
    }


    public void listen() {

        while (true) {
            String queueName = "statements.pending.load.balance";
            try {
                String data = listOps.rightPop(LIST_NAME, Duration.ofMinutes(1L));

                if (data != null && !data.isEmpty()) {
                    loadBalanceQueuesFromMainQueue(data);
                }

            } catch (QueryTimeoutException e) {
                log.info("lb timeout nothing to process");
            }
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


    @Override
    public void run()
    {
        listen();
    }
}
