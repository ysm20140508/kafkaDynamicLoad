package com.yunva.kafka.consumer.entity;


import com.yunva.business.dao.JdbcUtils;
import com.yunva.kafka.consumer.factory.ThreadFactory;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Administrator on 2015-08-25.
 */
public class Consumer extends Thread {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private ConsumerConnector connector;
    private JdbcUtils jdbcUtils;
    private boolean Running = true;
    private Map<String, Integer> topicMap;
    private Map<String, ConsumerTemplate> consumerTemplateMap;


    public Consumer(ConsumerConfig consumerConfig, JdbcUtils jdbcUtils, Map<String, ConsumerTemplate> consumerTemplateMap, Map<String, Integer> topicMap) {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", consumerConfig.getZookeeper_connect());
        properties.setProperty("zookeeper.session.timeout.ms", consumerConfig.getZookeeper_session_timeout_ms());
        properties.setProperty("group.id", "test");
        this.connector = kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(properties));
        this.jdbcUtils = jdbcUtils;
        this.topicMap = topicMap;
        this.consumerTemplateMap = consumerTemplateMap;
    }

    public void run() {
        logger.info("Consumer Thread is Running...");
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicMap);
        for (String topic : topicMap.keySet()) {
            ConsumerTemplate consumerTemplate = consumerTemplateMap.get(topic);
            String tableName = consumerTemplate.getTableName();
            String fieldName = consumerTemplate.getFieldName();
            Integer insertLimit = consumerTemplate.getInsertLimit();
            Integer insertHeartbeat = consumerTemplate.getInsertHeartbeat();
            Integer threads = consumerTemplate.getThrads();
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            int index = 0;
            for (KafkaStream<byte[], byte[]> stream : streams) {
                if (index < threads && Running) {
                    Thread thread = new DateInsertion(stream, jdbcUtils, tableName, fieldName, insertLimit, insertHeartbeat);
                    ThreadFactory.getThread().execute(thread);
                }
            }
        }
    }

    /**
     * 停止线程
     */
    public void stopThread() {
        Running = false;
        this.connector.shutdown();
        this.interrupt();
    }
}


