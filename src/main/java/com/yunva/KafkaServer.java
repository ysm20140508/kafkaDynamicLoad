package com.yunva;

import com.yunva.business.dao.JdbcUtils;
import com.yunva.kafka.consumer.entity.ConsumerConfig;
import com.yunva.kafka.consumer.factory.ConsumerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Created by Administrator on 2015-08-25.
 */
public class KafkaServer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaServer.class);

    public static void main(String[] args) {
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("spring/spring.xml");
        context.registerShutdownHook();

        JdbcUtils jdbcUtils = (JdbcUtils) context.getBean("jdbcUtils");
        ConsumerConfig consumerConfig = (ConsumerConfig) context.getBean("consumerConfig");

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        ConsumerThread consumerThread = new ConsumerThread(jdbcUtils, consumerConfig);
        scheduledExecutorService.scheduleAtFixedRate(consumerThread, 1, 2, TimeUnit.MINUTES);

        logger.info("com.yunva.KafkaServer start successful...");
    }
}
