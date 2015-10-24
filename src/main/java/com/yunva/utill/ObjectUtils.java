package com.yunva.utill;

import com.yunva.kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2015-07-07.
 */
public class ObjectUtils {
    /**
     * @param producerConfig
     * @return
     */
    public static Properties parseObject(ProducerConfig producerConfig) {
        Properties properties = new Properties();
        Class zclass = producerConfig.getClass();
        Field[] fields = zclass.getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName().replace("_", ".");
            field.setAccessible(true);
            try {
                String value = (String) field.get(producerConfig);
                properties.put(fieldName, value);
            } catch (IllegalAccessException e) {
                return null;
            }
        }
        return properties;
    }

    /**
     * 根据域名和消息组装成SQL
     *
     * @param message
     * @param fieldNames
     * @return
     */
    public static String parseString(String message, String fieldNames) {
        String sql = "";
        if (StringUtils.isEmpty(fieldNames)) return "";
        String[] names = fieldNames.split(",");
        int length = names.length;
        for (int index = 0; index < length; index++) {
            if (message.indexOf(names[index]) == -1) continue;
            if ((index < length - 1) && message.indexOf(names[index + 1]) == -1) continue;
            if (index == 0) {
                if (length > 1) {
                    String value1 = message.substring((names[index] + ":").length(), message.indexOf("|" + names[index + 1]));
                    sql += "'" + value1 + "'";
                }
            }
            if (index > 0 && index < length - 1) {
                String value = message.substring(message.indexOf(names[index]) + (names[index] + ":").length(), message.indexOf("|" + names[index + 1]));
                sql += ", '" + value + "'";
            }
            if (index == length - 1) {
                String value = message.substring(message.indexOf(names[index]) + (names[index] + ":").length());
                if (index == 0) {
                    sql += "'" + value + "'";
                } else {
                    sql += ", '" + value + "'";
                }
            }
        }
        sql = "( " + sql + " )";
        return sql;
    }
}
