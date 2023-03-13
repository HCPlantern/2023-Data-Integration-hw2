package com.nju.allinplantern.flink.pojo;

import com.nju.allinplantern.flink.pojo.eventbody.EventBody;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
/**
 * 交易类
 */
public class Event {
    private String eventDate;

    private String eventType;

    private EventBody eventBody;

    private String tableName;
}
