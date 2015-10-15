package com.yunva.kafka.consumer.entity;


import com.yunva.business.dao.JdbcUtils;
import com.yunva.kafka.consumer.factory.ThreadFactory;
import com.yunva.utill.SysContant;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2015-08-25.
 */
public class Consumer extends Thread {
    private  final Logger logger= LoggerFactory.getLogger(Consumer.class);
    private  ConsumerConnector connector;
    private  String topic;
    private  Integer threads;
    private  String threadName ;
    private  String tableName;
    private  String fieldName;
    private  JdbcUtils jdbcUtils;
    private  ExecutorService executorService;
    private  boolean Running=true;


    public Consumer(ConsumerConfig consumerConfig,ConsumerTemplate consumerTemplate,JdbcUtils jdbcUtils)
    {
        logger.info("init Consumer:ConsumerTemplate{}", consumerTemplate);
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", consumerConfig.getZookeeper_connect());
        properties.setProperty("zookeeper.session.timeout.ms", consumerConfig.getZookeeper_session_timeout_ms());
        properties.setProperty("group.id", consumerTemplate.getGroupId());
        this.connector = kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(properties));
        this.jdbcUtils=jdbcUtils;
        this.topic = consumerTemplate.getTopic();
        this.threadName=consumerTemplate.getThreadName();
        this.threads=consumerTemplate.getThrads();
        this.tableName=consumerTemplate.getTableName();
        this.fieldName=consumerTemplate.getFieldName();
        executorService = Executors.newFixedThreadPool(threads);
    }

    public void run() {
        logger.info("Consumer Thread is Running...");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        int index=0;
        for (KafkaStream<byte[], byte[]> stream : streams) {
           if(index<threads && Running){
               Thread thread=new DateInsertion(stream,jdbcUtils,tableName,fieldName);
               ThreadFactory.getIntstant().put(threadName+(++index),thread) ;
               executorService.execute(thread);
           }
        }
    }

    /**
     * 停止线程
     */
    public  void stopThread(){
        Running=false;
        this.connector.shutdown();
        this.interrupt();
    }
}


