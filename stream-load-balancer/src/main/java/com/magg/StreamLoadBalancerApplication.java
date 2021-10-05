package com.magg;

import com.magg.config.RedisConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Import(RedisConfiguration.class)
public class StreamLoadBalancerApplication
{

	public static void main(String[] args) {
		SpringApplication.run(StreamLoadBalancerApplication.class, args);
	}

}
