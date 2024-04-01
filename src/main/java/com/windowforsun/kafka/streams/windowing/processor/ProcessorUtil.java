package com.windowforsun.kafka.streams.windowing.processor;

import com.windowforsun.kafka.streams.windowing.model.*;

public class ProcessorUtil {

    public static MyEventAgg aggregateMyEvent(String key, MyEvent myEvent, MyEventAgg aggregateMyEvent) {
        Long firstSeq = aggregateMyEvent.getFirstSeq();
        Long lastSeq = aggregateMyEvent.getLastSeq();
        Long count = aggregateMyEvent.getCount();
        String str = aggregateMyEvent.getStr();

        if(count == null || count <= 0) {
            firstSeq = myEvent.getSeq();
            str = "";
            count = 0L;
            lastSeq = Long.MIN_VALUE;
        }

        lastSeq = Long.max(lastSeq, myEvent.getSeq());
        str = str.concat(myEvent.getStr());
        count++;

        return MyEventAgg.builder()
                .firstSeq(firstSeq)
                .lastSeq(lastSeq)
                .count(count)
                .str(str)
                .build();
    }

    public static MyEventAgg mergeMyEvent(String key, MyEventAgg agg1, MyEventAgg agg2) {
        Long firstSeq = 0L;
        Long lastSeq = 0L;
        Long count = 0L;
        String str = "";

        if (agg1 != null) {
            firstSeq = agg1.getFirstSeq();
            lastSeq = agg1.getLastSeq();
            count = agg1.getCount();
            str = agg1.getStr();
        }

        if(agg2 != null) {
            firstSeq = Long.min(firstSeq, agg2.getFirstSeq());
            lastSeq = Long.max(lastSeq, agg2.getLastSeq());
            count += agg2.getCount();
            str = str.concat(agg2.getStr());
        }

        return MyEventAgg.builder()
                .firstSeq(firstSeq)
                .lastSeq(lastSeq)
                .count(count)
                .str(str)
                .build();
    }

    public static LinkSummary aggregate(String key, Link link, LinkSummary linkSummary) {
        Long upCount = linkSummary.getUpCount();
        Long downCount = linkSummary.getDownCount();
        Long toggleCount = linkSummary.getToggleCount();
        String codes = linkSummary.getCodes();
        LinkStatusEnum status = linkSummary.getStatus();

        if (codes == null) {
            codes = "";
        }

        codes = codes.concat(link.getCode());

        if(link.getStatus() == LinkStatusEnum.DOWN) {
            downCount++;
        } else {
            upCount++;
        }

        if(status != null && link.getStatus() != status) {
            toggleCount++;
        }

        LinkSummary newLinkSummary = LinkSummary.builder()
                .name(link.getName())
                .downCount(downCount)
                .upCount(upCount)
                .codes(codes)
                .toggleCount(toggleCount)
                .status(link.getStatus())
                .build();

        return newLinkSummary;
    }
}

/**
 * kraft
 * apache kafka 의 새로운 클러스터 관리 모드로, 기존 zookeeper의 의존성 없이 kafka 클러스터를 관리할 수 있는 방식
 * kafka 메타 데이터 관리(브로커, 토픽, 파티션)을 위해 zookeeper 를 사용했지만, kraft 는 이런 의존성을 제거하고 kafka 자체에서 메타데이터를 관리한다.
 *
 * 기본적으로 zookeeper 에 토픽, 파티션, 레플리카 정보를 저장
 * 왜 카프카 컨트롤러를 변경하게 됐을 까
 *
 * 컨트롤러 역할 kafka broker 중 하나로 클러스터의 메타데이터를 관리하고 리더 선출 파티션 할당 등을 수행하는 특별한 브로커(단 한개만 존재)
 * zookeeper 역할 컨트롤러 선출과 브로커 클러스터 메타데이터(토픽, 파티션, 레플리카,..) 저장
 * 쿼럼 : 분산 시스템에서 과반수의 합의를 이루기 위해 필요한 최소한의 노드 수, 리더 선출 데이터 복제, 상태 변경과 같은 결정을 내릴 때 사용
 *
 * as-is
 * - zookeeper - kafka
 * - 컨트롤러, 브로커, 주키퍼 간에 데이터 불일치가 발생 할 수 있다.
 * - 컨트롤러가 변경 혹은 재시작 될 때마다 주키퍼로부터 모든 브로커와 파티션 메타 데이터를 읽어와야 한다. 그리고 다시 모든 브로커에게 전송한다. 컨트롤러 재시작이 느려질 수 있다.
 * - 메타데이터관리 체계가 복잡하다. 어떤건 컨트롤러가 어떤건 브로커가 어떤거 주키퍼가 한다.
 * - 주키퍼는 카프카 클러스터의 필수 구성이므로 운영을 위해서는 필수적으로 관련 기본 지식이 있어야 한다. 즉 2개의 분산 시스템(주키퍼, 카프카)에 대한 지식이 필요하다.
 *
 *
 * to-be
 * - active controller(1) - follow controller(n-1)
 * - 메타데이터를 로그 기반 아키텍쳐로 관리한다. 로그에 클러스터 메타데이터의 변경 내역을 저장한다.
 * - 액티브 컨트롤러는 메타데이터 로그의 리더이고 이를 복제하는 팔로우 컨트롤러로 구성된다. 그리고 이제 자체적으로 리더를 선출하고 모든 브로커는 최신상태를 팔로우 하므로 리더 재선출에서도 긴 재시작 시간이 필요 없다.
 * - 액티브 컨트롤러와 팔로우 컨트롤러간 복제는 컨슈머와 같이 polling 방식으로 이뤄진다.
 * - 새로운 브로커가 시작되면 주키퍼에 등록하는 것이 아니라 컨트롤러 쿼럼에 등록한다.
 *
 *
 * 2.8 에 시범 도입
 * 3.3 정식 출시
 * 3.4 브리지 릴리즈 기능 지원 구 -> 신 마이그레이션(ea)
 * 3.5 zk deprecated, 구 -> 신 마이그레이션 preview
 * 3.6 구신 -> 마이그레이션(ga)
 * 3.7 마지막 zk 모드 릴리즈
 * 4.0 only kraft
 */
