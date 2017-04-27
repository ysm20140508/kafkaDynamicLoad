package com.it.jxnu.zookeeper;

import com.it.jxnu.business.model.ConsumerTopic;
import com.it.jxnu.kafka.consumer.entity.Consumer;
import com.it.jxnu.kafka.consumer.entity.ConsumerConfig;
import com.it.jxnu.kafka.consumer.entity.DateInsertion;
import com.it.jxnu.kafka.consumer.factory.ThreadFactory;
import com.it.jxnu.business.dao.JdbcUtils;
import com.it.jxnu.kafka.consumer.entity.ConsumerTemplate;
import com.it.jxnu.utill.FastjsonUtills;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: xiang_xiang
 * Date: 2015-10-24
 * PackageName: com.yunva.zookeeper
 * Github: https://github.com/ysm20140508
 */
public class ZookeeperClient implements Watcher {
    private Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);
    private ConsumerConfig consumerConfig;
    private JdbcUtils jdbcUtils;
    private static ZooKeeper zk;

    public ZookeeperClient(ConsumerConfig consumerConfig, JdbcUtils jdbcUtils) {
        this.consumerConfig = consumerConfig;
        this.jdbcUtils = jdbcUtils;
    }

    public void init() {
        try {
            logger.info("init Zookeeper");
            zk = new ZooKeeper(consumerConfig.getZookeeper_connect(), Integer.parseInt(consumerConfig.getZookeeper_session_timeout_ms()), new ZookeeperClient(consumerConfig, jdbcUtils));
            Stat stat = zk.exists("/kafkaConsumers", true);
            if (stat == null) {
                zk.create("/kafkaConsumers", "kafkaConsumers".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("create Zookeeper Node /kafkaConsumers");
            }
            List<String> childrenList = zk.getChildren("/kafkaConsumers", true);
            initThread(childrenList);
            while (true) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage());
        }

    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (Event.EventType.NodeChildrenChanged == event.getType()) {
                List<String> childrenList = zk.getChildren("/kafkaConsumers", true);
                initThread(childrenList);
            }
        } catch (Exception e) {
            logger.error("{}", e.getMessage());
        }
    }

    public void initThread(List<String> childrenList) {
        try {
            Map<String, ConsumerTemplate> consumerTemplateMap = new HashMap<String, ConsumerTemplate>();
            Map<String, Integer> topicMap = new HashMap<String, Integer>();
            for (String key : (Set<String>) ThreadFactory.getIntstant().keySet()) {
                String threadName = key;
                Thread thread = (Thread) ThreadFactory.getIntstant().get(key);
                ThreadFactory.getIntstant().remove(threadName);
                if (thread instanceof DateInsertion) {
                    ((DateInsertion) thread).stopThread();
                } else if (thread instanceof Consumer) {
                    ((Consumer) thread).stopThread();
                }
            }
            for (String children : childrenList) {
                if (StringUtils.isNotEmpty(children) && !ThreadFactory.getIntstant().containsKey(children)) {
                    Stat stat = new Stat();
                    String path = "/kafkaConsumers/" + children;
                    logger.info("obtain kafka node {} content ", path);
                    ConsumerTopic consumerTop = FastjsonUtills.parseObject(zk.getData(path, false, stat), ConsumerTopic.class);
                    if (consumerTop == null || StringUtils.isEmpty(consumerTop.getTopic())) continue;
                    ConsumerTemplate consumerTemplate = new ConsumerTemplate();
                    consumerTemplate.setThreadName(children);
                    consumerTemplate.setThrads(consumerTop.getThreadCount());
                    consumerTemplate.setTopic(consumerTop.getTopic());
                    consumerTemplate.setGroupId(consumerTop.getGroup());
                    consumerTemplate.setTableName(consumerTop.getTableName());
                    consumerTemplate.setFieldName(consumerTop.getFieldName());
                    consumerTemplate.setInsertLimit(consumerTop.getInsertLimit());
                    consumerTemplate.setInsertHeartbeat(consumerTop.getInsertHeartbeat());
                    consumerTemplateMap.put(consumerTop.getTopic(), consumerTemplate);
                    topicMap.put(consumerTop.getTopic(), consumerTop.getThreadCount());
                }
            }
            if (childrenList.size() > 0) {
                Consumer consumer = new Consumer(consumerConfig, jdbcUtils, consumerTemplateMap, topicMap);
                logger.info("open consumer thread ");
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                executorService.submit(consumer);
                ThreadFactory.getIntstant().put("kafkaConsumer", consumer);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
