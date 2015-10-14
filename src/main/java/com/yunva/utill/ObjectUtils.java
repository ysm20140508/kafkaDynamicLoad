package com.yunva.utill;

import com.yunva.kafka.producer.ProducerConfig;

import java.lang.reflect.Field;
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
}
