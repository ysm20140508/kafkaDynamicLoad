package com.yunva.business.dao;

import com.yunva.business.model.ConsumerTopic;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: xiang_xiang
 * Date: 2015-10-14
 * PackageName: com.yunva.business.dao
 * Github: https://github.com/ysm20140508
 */
@Service(value = "jdbcUtils")
public class JdbcUtils {
    @Resource
    JdbcTemplate jdbcTemplate;

    /**
     * obtain consumer
     *
     * @return
     */
    public List<ConsumerTopic> getUpdatedConsumer() {
        List<ConsumerTopic> consumerTopicList = new ArrayList<ConsumerTopic>();
        String sql = "SELECT * FROM tbl_kafka_consumer_info WHERE update_status = 1";
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> map : mapList) {
            ConsumerTopic consumerTopic = new ConsumerTopic();
            consumerTopic.setName((String) map.get("name"));
            consumerTopic.setTopic((String) map.get("topic"));
            consumerTopic.setGroup((String) map.get("group"));
            consumerTopic.setTableName((String) map.get("table_name"));
            Integer threadStatus = (Integer) map.get("thread_status");
            Integer threadCount = (Integer) map.get("thread_count");
            Integer updateStatus = (Integer) map.get("update_status");
            Object id = (Object) map.get("id");
            consumerTopic.setThreadStatus(threadStatus);
            consumerTopic.setThreadCount(threadCount);
            consumerTopic.setUpdateStatus(updateStatus);
            consumerTopic.setId(Integer.parseInt(id.toString()));
            consumerTopic.setFieldName((String) map.get("field_name"));
            consumerTopicList.add(consumerTopic);
        }
        return consumerTopicList;
    }

    /**
     * update status
     */
    public void updateSuccess(int id) {
        String sql = "UPDATE tbl_kafka_consumer_info SET update_status = 0 WHERE update_status = 1 and id=?";
        jdbcTemplate.update(sql, new Integer[]{id});
    }

    /**
     * batch insert
     *
     * @param sqls
     */
    public void batchInsert(String[] sqls) {
        jdbcTemplate.batchUpdate(sqls);
    }

    /**
     * insert
     *
     * @param sql
     */
    public void insert(String sql) {
        jdbcTemplate.execute(sql);
    }

}
