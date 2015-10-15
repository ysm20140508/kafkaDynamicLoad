package com.yunva.kafka.consumer.entity;

/**
 * Created by Administrator on 2015-08-25.
 */
public class ConsumerTemplate {
    private String groupId;
    private String topic;
    private String tableName;
    private Integer thrads;
    private String threadName;
    private String fieldName;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getThrads() {
        return thrads;
    }

    public void setThrads(Integer thrads) {
        this.thrads = thrads;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
