package com.magg.consumer.service;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class ExecutorBase
{
    private final TaskExecutor taskExecutor;
    private final ApplicationContext applicationContext;


    public ExecutorBase(TaskExecutor taskExecutor, ApplicationContext applicationContext)
    {
        this.taskExecutor = taskExecutor;
        this.applicationContext = applicationContext;
    }

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        RedisReceiver classToRun = applicationContext.getBean(RedisReceiver.class);
        taskExecutor.execute(classToRun);
    }
}
