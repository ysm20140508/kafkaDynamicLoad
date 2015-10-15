package com.yunva.kafka.producer;

/**
 * Created by Administrator on 2015-07-07.
 */

import java.util.List;

/**
 * @param <K>
 * @param <V>
 */
public interface KafkaProducer<K, V> {
    /**
     * @param topic
     * @param data
     */
    public void send(String topic, V data);

    /**
     * @param topic
     * @param datas
     */
    public void send(String topic, List<V> datas);

    /**
     * @param topic
     * @param key
     * @param datas
     */
    public void send(String topic, K key, List<V> datas);
}
