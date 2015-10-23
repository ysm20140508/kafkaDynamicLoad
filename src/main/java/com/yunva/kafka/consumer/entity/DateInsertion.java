package com.yunva.kafka.consumer.entity;

import com.yunva.business.dao.JdbcUtils;
import com.yunva.utill.ObjectUtils;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.util.concurrent.atomic.AtomicInteger;


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

    public DateInsertion(KafkaStream<byte[], byte[]> stream, JdbcUtils jdbcUtils, String tableName, String fieldName) {
        this.stream = stream;
        this.tableName = tableName;
        this.jdbcUtils = jdbcUtils;
        this.fieldName = fieldName;
    }

    public void run() {
        logger.info("DateInsertion Thread is Running...");
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        long endTime = 0l;
        int index = 0;
        String sql = "";
        while (it.hasNext())
            if (isRunning) {
                long curreyTIme = System.currentTimeMillis();
                try {
                    if (index < 50000) {
                        if (index == 0) {
                            sql = "insert into " + tableName + " VALUES " + ObjectUtils.parseString(new String(it.next().message(), "utf-8"), tableName, fieldName);
                        } else {
                            sql += "," + ObjectUtils.parseString(new String(it.next().message(), "utf-8"), tableName, fieldName);
                        }
                        index++;
                    } else if ((index == 50000 || (endTime - curreyTIme) > 600000L)) {
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
        this.interrupt();
    }
}
