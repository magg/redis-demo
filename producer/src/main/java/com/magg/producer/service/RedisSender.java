package com.magg.producer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSender.class);

    private static final String LIST_NAME = "demo_list";

    private final StringRedisTemplate redisTemplate;

    private ListOperations<String, String> reactiveListOps;


    public RedisSender(StringRedisTemplate redisTemplate)
    {
        this.redisTemplate = redisTemplate;
    }

    public void sendDataToRedisQueue(String input) {
        reactiveListOps = redisTemplate.opsForList();

        reactiveListOps.leftPush(LIST_NAME, input);
        LOGGER.info("Data - " + input + " sent through Redis List - " + input);
    }
}