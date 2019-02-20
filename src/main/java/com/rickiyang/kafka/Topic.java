package com.rickiyang.kafka;

/**
 * @Author yangyue
 * @Date Created in 下午4:17 2018/11/23
 * @Modified by:
 * @Description:
 **/
import com.rickiyang.avro.User;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.EnumSet;

public enum Topic {
    USER("user-info-topic", new User());

    public final String topicName;
    public final SpecificRecordBase topicType;

    Topic(String topicName, SpecificRecordBase topicType) {
        this.topicName = topicName;
        this.topicType = topicType;
    }

    public static Topic matchFor(String topicName) {
        return EnumSet.allOf(Topic.class).stream()
                .filter(topic -> topic.topicName.equals(topicName))
                .findFirst()
                .orElse(null);
    }
}
