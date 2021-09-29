package com.magg.consumer;

import com.magg.config.RedisConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Import(RedisConfiguration.class)
@ComponentScan(basePackages = {"com.magg.repository", "com.magg.util", "com.magg.config", "com.magg.consumer"})
public class ConsumerApplication
{

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

}
