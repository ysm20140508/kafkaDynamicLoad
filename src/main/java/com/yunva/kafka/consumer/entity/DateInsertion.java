package com.yunva.kafka.consumer.entity;

import com.yunva.business.dao.JdbcUtils;
import com.yunva.utill.ObjectUtils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Created by Administrator on 2015-08-29.
 */
public class DateInsertion extends Thread {
    private final Logger logger = LoggerFactory.getLogger(DateInsertion.class);
    private KafkaStream<byte[], byte[]> stream;
    private JdbcUtils jdbcUtils;
    private boolean isRunning = true;
    private String tableName;
    private String fieldName;

    public DateInsertion(KafkaStream<byte[], byte[]> stream, JdbcUtils jdbcUtils,String tableName,String fieldName) {
        this.stream = stream;
        this.tableName=tableName;
        this.jdbcUtils = jdbcUtils;
        this.fieldName=fieldName;
    }

    public void run() {
        logger.info("DateInsertion Thread is Running...");
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext())
            if (isRunning) {
                try {
                    jdbcUtils.insert(ObjectUtils.parseString(new String(it.next().message(), "utf-8"),tableName,fieldName));
                } catch (UnsupportedEncodingException e) {
                }
            }

    }

    /**
     * 停止线程
     */
    public void stopThread() {
        isRunning = false;
        this.interrupt();
    }
}
