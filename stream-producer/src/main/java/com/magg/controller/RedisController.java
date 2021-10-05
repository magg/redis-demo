package com.magg.controller;

import com.magg.model.TransactionModel;
import com.magg.publisher.EventPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/message")
public class RedisController
{

    @Autowired
    private EventPublisher sender;

    @PostMapping("/send")
    public String sendDataToRedisQueue(@RequestBody TransactionModel input) {
        sender.publishEvent(input);
        return "successfully sent";
    }
}