package com.magg.consumer.service;

import com.magg.repository.QueueRepository;
import com.magg.repository.ZsetRepository;
import java.time.Duration;
import java.util.Set;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Slf4j
@Component
@Scope("application")
public class RedisReceiver implements Runnable
{
    @Value("${spring.application.name}")
    private String appName;

    private String queueName;

    private final String LIST_NAME = "statements.pending.queue.";

    private final StringRedisTemplate redisTemplate;

    private final ListOperations<String, String> listOps;
    private final QueueRepository queueRepository;
    private final ZsetRepository zsetRepository;

    @PostConstruct
    public void init() {
        Set<String> workerSet = zsetRepository.getAll();
        if (!workerSet.contains(appName)) {
            zsetRepository.add(appName);
        }
    }

    public RedisReceiver(StringRedisTemplate redisTemplate, QueueRepository queueRepository, ZsetRepository zsetRepository)
    {
        this.redisTemplate = redisTemplate;
        this.listOps = redisTemplate.opsForList();
        this.queueRepository = queueRepository;
        this.zsetRepository = zsetRepository;
    }

    public void listen()
    {

        String pending = LIST_NAME + appName;

        queueName = LIST_NAME + appName;

        while (true) {
            try {
                String data = listOps.rightPopAndLeftPush(pending, queueName, Duration.ofMinutes(1L) );

                if (data != null && !data.isEmpty()) {
                    Thread.sleep(8000);
                    String str = listOps.rightPop(queueName); //block, remove the temporary Queue
                    zsetRepository.decr(appName);
                    log.info("Data - " + str + " received through Redis List - ");
                    queueRepository.delete(data);
                }

            } catch (QueryTimeoutException e) {
                log.info("timeout");
            } catch (InterruptedException ie) {
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void run()
    {
        listen();
    }
}