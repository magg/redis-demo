package com.magg.service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.ReactiveListOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Slf4j
@Service
public class RedisReciever
{

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisReciever.class);

    private static final String LIST_NAME = "demo_list";

    private final StringRedisTemplate redisTemplate;

    private final ListOperations<String, String> reactiveListOps;


    public RedisReciever(StringRedisTemplate redisTemplate)
    {
        this.redisTemplate = redisTemplate;
        this.reactiveListOps = redisTemplate.opsForList();
    }

    @Scheduled(fixedRate = 5000)
    public void listen(){
        try {
            //Take out the message and put it in the temporary queue

            String data = reactiveListOps.rightPopAndLeftPush(LIST_NAME, "tmp-queue");

            Thread.sleep(1000);
            //reactiveListOps.rightPop("tmp-queue");//Non-blocking

            //Blocking brpop, block when there is no data in the List, the parameter 0 means that the block will continue until data appears in the List
            String str = reactiveListOps.rightPop("tmp-queue"); //block, remove the temporary Queue

            log.info("Data - " + str + " received through Redis List - ");


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }
}