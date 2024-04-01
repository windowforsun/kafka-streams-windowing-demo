package com.windowforsun.kafka.streams.windowing.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MyEventAgg {
    @Builder.Default
    private Long firstSeq = Long.MAX_VALUE;
    @Builder.Default
    private Long lastSeq = Long.MIN_VALUE;
    @Builder.Default
    private Long count = 0L;
    @Builder.Default
    private String str = "";
}
