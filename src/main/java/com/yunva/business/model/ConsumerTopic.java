package com.yunva.business.model;

import java.util.Date;

/**
 * User: xiang_xiang
 * Date: 2015-10-14
 * PackageName: com.yunva.business.model
 * Github: https://github.com/ysm20140508
 */
public class ConsumerTopic {
    private Integer id;
    private String name;
    private String topic;
    private String group;
    private String tableName;
    private Integer threadStatus;
    private Integer threadCount;
    private Integer updateStatus;
    private Date createTime;
    private String fieldName;
    private Integer insertLimit;
    private Integer insertHeartbeat;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Integer getThreadStatus() {
        return threadStatus;
    }

    public void setThreadStatus(Integer threadStatus) {
        this.threadStatus = threadStatus;
    }

    public Integer getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(Integer threadCount) {
        this.threadCount = threadCount;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Integer getUpdateStatus() {
        return updateStatus;
    }

    public void setUpdateStatus(Integer updateStatus) {
        this.updateStatus = updateStatus;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Integer getInsertHeartbeat() {
        return insertHeartbeat;
    }

    public void setInsertHeartbeat(Integer insertHeartbeat) {
        this.insertHeartbeat = insertHeartbeat;
    }

    public Integer getInsertLimit() {
        return insertLimit;
    }

    public void setInsertLimit(Integer insertLimit) {
        this.insertLimit = insertLimit;
    }
}
