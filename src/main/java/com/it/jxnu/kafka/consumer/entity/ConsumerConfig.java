package com.it.jxnu.kafka.consumer.entity;

/**
 * Created by Administrator on 2015-07-07.
 */
public class ConsumerConfig {
    private String group_id;
    private String zookeeper_connect;
    private String zookeeper_session_timeout_ms;

    public String getGroup_id() {
        return group_id;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public String getZookeeper_connect() {
        return zookeeper_connect;
    }

    public void setZookeeper_connect(String zookeeper_connect) {
        this.zookeeper_connect = zookeeper_connect;
    }

    public String getZookeeper_session_timeout_ms() {
        return zookeeper_session_timeout_ms;
    }

    public void setZookeeper_session_timeout_ms(String zookeeper_session_timeout_ms) {
        this.zookeeper_session_timeout_ms = zookeeper_session_timeout_ms;
    }
}
