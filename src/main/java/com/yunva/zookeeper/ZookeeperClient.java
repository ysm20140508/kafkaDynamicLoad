package com.yunva.zookeeper;

import com.yunva.business.dao.JdbcUtils;
import com.yunva.business.model.ConsumerTopic;
import com.yunva.kafka.consumer.entity.Consumer;
import com.yunva.kafka.consumer.entity.ConsumerConfig;
import com.yunva.kafka.consumer.entity.ConsumerTemplate;
import com.yunva.kafka.consumer.entity.DateInsertion;
import com.yunva.kafka.consumer.factory.ThreadFactory;
import com.yunva.utill.FastjsonUtills;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
            logger.error("{}",e.getMessage());
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
            logger.error("{}",e.getMessage());
        }
    }

    public void initThread(List<String> childrenList) {
        try {
            String chiledrenName = "";
            for (String children : childrenList) {
                chiledrenName += children+",";
            }
            for (String key : (Set<String>) ThreadFactory.getIntstant().keySet()) {
                if (StringUtils.isNotEmpty(chiledrenName) && chiledrenName.indexOf(key) == -1) {
                    String threadName = key;
                    if(!ThreadFactory.getIntstant().containsKey(key)) continue;
                    Consumer consumer = (Consumer) ThreadFactory.getIntstant().get(key);
                    consumer.stopThread();
                    ThreadFactory.getIntstant().remove(threadName);
                    Integer threads = consumer.getThreads();
                    for (int index = threads; index > 0; index--) {
                        String indexThreadName = threadName + index;
                        if (ThreadFactory.getIntstant().containsKey(indexThreadName)) {
                            DateInsertion dateInsertion = (DateInsertion) ThreadFactory.getIntstant().get(indexThreadName);
                            dateInsertion.stopThread();
                            ThreadFactory.getIntstant().remove(indexThreadName);
                        }
                    }
                } else if (StringUtils.isEmpty(chiledrenName)) {
                    String threadName = key;
                    if(!ThreadFactory.getIntstant().containsKey(key)) continue;
                    Consumer consumer = (Consumer) ThreadFactory.getIntstant().get(key);
                    consumer.stopThread();
                    ThreadFactory.getIntstant().remove(threadName);
                    Integer threads = consumer.getThreads();
                    for (int index = threads; index > 0; index--) {
                        String indexThreadName = threadName + index;
                        if (ThreadFactory.getIntstant().containsKey(indexThreadName)) {
                            DateInsertion dateInsertion = (DateInsertion) ThreadFactory.getIntstant().get(indexThreadName);
                            dateInsertion.stopThread();
                            ThreadFactory.getIntstant().remove(indexThreadName);
                        }
                    }
                }
            }
            for (String children : childrenList) {
                if (!ThreadFactory.getIntstant().containsKey(children)) {
                    Stat stat = new Stat();
                    String path = "/kafkaConsumers/" + children;
                    logger.info("obtain kafka node {} content ",path);
                    ConsumerTopic consumerTop = FastjsonUtills.parseObject(zk.getData(path, false, stat),ConsumerTopic.class);
                    ConsumerTemplate consumerTemplate = new ConsumerTemplate();
                    consumerTemplate.setThreadName(children);
                    consumerTemplate.setThrads(consumerTop.getThreadCount());
                    consumerTemplate.setTopic(consumerTop.getTopic());
                    consumerTemplate.setGroupId(consumerTop.getGroup());
                    consumerTemplate.setTableName(consumerTop.getTableName());
                    consumerTemplate.setFieldName(consumerTop.getFieldName());
                    consumerTemplate.setInsertLimit(consumerTop.getInsertLimit());
                    consumerTemplate.setInsertHeartbeat(consumerTop.getInsertHeartbeat());
                    Consumer consumer = new Consumer(consumerConfig, consumerTemplate, jdbcUtils);
                    logger.info("open consumer thread ");
                    ExecutorService executorService = Executors.newSingleThreadExecutor();
                    executorService.submit(consumer);
                    ThreadFactory.getIntstant().put(children, consumer);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
