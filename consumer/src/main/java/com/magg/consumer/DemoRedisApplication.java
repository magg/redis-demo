package com.magg.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DemoRedisApplication
{

	public static void main(String[] args) {
		SpringApplication.run(DemoRedisApplication.class, args);
	}

}
