package com.it.jxnu;

import com.it.jxnu.kafka.consumer.entity.ConsumerConfig;
import com.it.jxnu.business.dao.JdbcUtils;
import com.it.jxnu.zookeeper.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


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
//        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
//        ConsumerThread consumerThread = new ConsumerThread(jdbcUtils, consumerConfig);
//        scheduledExecutorService.scheduleAtFixedRate(consumerThread, 1, 2, TimeUnit.MINUTES);
        ZookeeperClient zookeeperClient = new ZookeeperClient(consumerConfig, jdbcUtils);
        zookeeperClient.init();
        logger.info("com.yunva.KafkaServer start successful...");
    }
}
