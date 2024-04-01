package com.windowforsun.kafka.streams.windowing.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LinkMonitor {
    private String name;
    private String ip;
    @Builder.Default
    private Long downCount = 0L;
    private String codes;
    private LinkStatusEnum status;
}
