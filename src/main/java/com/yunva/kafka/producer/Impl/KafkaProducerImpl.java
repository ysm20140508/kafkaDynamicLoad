package com.yunva.kafka.producer.Impl;

import com.yunva.kafka.producer.KafkaProducer;
import com.yunva.kafka.producer.ProducerConfig;
import com.yunva.utill.ObjectUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2015-07-07.
 */
@Service
public class KafkaProducerImpl<K, V> implements KafkaProducer<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerImpl.class);

    @Resource
    private ProducerConfig producerConfig;
    private Producer producer;

    @PostConstruct
    public void init() {
        Properties properties = ObjectUtils.parseObject(producerConfig);
        kafka.producer.ProducerConfig producerConfig1 = new kafka.producer.ProducerConfig(properties);
        producer = new Producer(producerConfig1);
        logger.info("producer inited...");
    }

    /**
     * @param topic
     * @param key   The default partitioning strategy is hash(key)%numPartitions. If the key is null, then a random broker partition is picked��
     * @param datas
     */
    public void send(String topic, K key, List<V> datas) {
        try {
            producer.send(new KeyedMessage(topic, key, datas));
        } catch (Exception e) {
            logger.error("producer send {} to {} failure", topic, datas);
        }
    }

    /**
     * @param topic
     * @param datas
     */
    public void send(String topic, List<V> datas) {
        try {
            producer.send(new KeyedMessage(topic, datas));
        } catch (Exception e) {
            logger.error("producer send {} to {} failure", topic, datas);
        }
    }

    /**
     * @param topic
     * @param data
     */
    public void send(String topic, V data) {
        try {
            producer.send(new KeyedMessage<K, V>(topic, data));
        } catch (Exception e) {
            logger.error("producer send {} to {} failure", topic, data);
        }
    }

}
