package com.it.jxnu.kafka.consumer.entity;

import com.it.jxnu.utill.ObjectUtils;
import com.it.jxnu.utill.SysContant;
import com.it.jxnu.business.dao.JdbcUtils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by Administrator on 2015-08-29.
 */
public class DateInsertion extends Thread {
    private final Logger logger = LoggerFactory.getLogger(DateInsertion.class);
    private KafkaStream<byte[], byte[]> stream;
    private JdbcUtils jdbcUtils;
    private volatile boolean isRunning = true;
    private String tableName;
    private String fieldName;
    private Integer insertLimit;
    private Integer insertHeartbeat;

    public DateInsertion(KafkaStream<byte[], byte[]> stream, JdbcUtils jdbcUtils, String tableName, String fieldName, Integer insertLimit, Integer insertHeartbeat) {
        this.stream = stream;
        this.tableName = tableName;
        this.jdbcUtils = jdbcUtils;
        this.fieldName = fieldName;
        this.insertLimit = insertLimit;
        this.insertHeartbeat = insertHeartbeat;
    }

    public void run() {
        logger.info("DateInsertion Thread is Running...");
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        long endTime = System.currentTimeMillis();
        int index = 0;
        String sql = "";
        while (isRunning && it.hasNext()) {
            long curreyTime = System.currentTimeMillis();
            Long timeInterval = curreyTime - endTime;
            try {
                if (index < insertLimit * SysContant.INSERTSIZE && timeInterval < insertHeartbeat * SysContant.INSERTHEATBEAT) {
                    if (index == 0) {
                        sql = "insert into " + tableName + " VALUES " + ObjectUtils.parseString(new String(it.next().message(), "utf-8"), fieldName);
                    } else {
                        sql += "," + ObjectUtils.parseString(new String(it.next().message(), "utf-8"), fieldName);
                    }
                    index++;
                } else if ((index == insertLimit * SysContant.INSERTSIZE || timeInterval >= insertHeartbeat * SysContant.INSERTHEATBEAT)) {
                    if (StringUtils.isNotEmpty(sql)) {
                        logger.info("insert into {}", index);
                        jdbcUtils.insert(sql);
                        index = 0;
                        sql = "";
                    }
                }
                endTime = System.currentTimeMillis();
            } catch (Exception e) {
                logger.error("{}", e);
            }
        }
    }

    /**
     * 停止线程
     */

    public void stopThread() {
        isRunning = false;
    }
}
