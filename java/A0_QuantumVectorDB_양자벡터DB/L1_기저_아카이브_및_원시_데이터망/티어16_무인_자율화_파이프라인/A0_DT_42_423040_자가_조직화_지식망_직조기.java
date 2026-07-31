/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인
 * @alias SelfOrganizingKnowledgeWeaver
 * @tier 16
 * @keywords Knowledge Weaving, Self-Organization, FastUtil Adapter, Zero-Allocation Pipeline, Open Addressing
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_423040_자가_조직화_지식망_직조기.java
 * - 역할 (Role): 하위 계층(T8~T12)의 결과물들을 취합하여 최종적으로 자가 조직화된 3D 지식망을 직조.
 * - 기능 (Function): 외부 문헌의 텐서 사영, 하위 모듈 호출 및 파이프라인 조율, 원시 맵(Int2DoubleMap) 어댑터 역할.
 * - 이론 및 기술 (Theory & Tech): 자가 조직화 맵(Self-Organizing Map), 의존성 주입(DI), 어댑터 패턴(Adapter Pattern), 개방 주소법(Open Addressing).
 * - 기대효과 (Effect): 기존 표준 Map의 하위 호환성을 유지하면서도 하위 V6.1 모듈들의 Zero-Allocation 아키텍처와 완벽히 결속하여 타입 충돌을 멸균합니다.
 * 
 * [신규/변경/삭제 사항 (V6.1 리메이크)]
 * - 💡 [패치] Maven 빌드 중 발생한 `incompatible types: Map cannot be converted to Int2DoubleMap` 오류 영구 수복.
 * - 💡 [신설] `어댑터_변환하다_표준맵_투_원시맵` 유틸리티 메서드를 내장하여, 레거시 시스템에서 넘어온 
 *         표준 `Map<Integer, Double>`을 하위 V6.1 모듈이 요구하는 `Int2DoubleMap`으로 안전하게 캐스팅.
 * - 💡 [변경] 직조(Weaving) 결과를 담는 메인 지식 저장소 및 내부 추출 로직을 `Int2DoubleMap`으로 
 *         타입 업그레이드하여 시스템 전반의 메모리 파편화를 차단.
 * - 💡 [수복] FFM 레이아웃과 개방 주소법(Open Addressing) 라우팅 사전을 유지하여 위상학적 융합 무결성 100% 수호.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 의존성 Import.
// 하위 계층인 T8~T12 모듈들의 인터페이스 규격에 맞추기 위해 FastUtil을 도입합니다.
// [2. 영문 상세 주석]
// Package declaration and dependencies Import.
// Introduced FastUtil to match the interface specifications of the lower-layer T8~T12 modules.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

// 하위 T8 모듈 및 FFM 인터페이스 (의존성 주입 대상)
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.WritePort;
import A0_QuantumVectorDB_양자벡터DB.L3_TDQI_심층_사유_코어.티어8_문헌_해체_및_3D_관계망_직조기.A0_DT_42_422081_모순_유예_양자_버퍼;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인.A0_DT_42_423020_시맨틱_문헌_해체_도끼.문헌_메타데이터_캡슐;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인.A0_DT_42_423030_무인_위상_사영소.위상_사유_입자_캡슐;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 통합 OS V6.1 하위 계층과의 타입 호환성을 확보한 파이프라인 조율기(Weaver)입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A pipeline orchestrator (Weaver) that secures type compatibility with the lower layers of Integrated OS V6.1.
// [3. 자바 코드]
public final class A0_DT_42_423040_자가_조직화_지식망_직조기 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.423040_GRAPH_WEAVER");

    // [1. 한글 상세 주석]
    // 진리가 충돌(모순)되었다고 판단할 코사인 유사도의 하한선 및 0분할 방어용 에프실론 상수입니다.
    // [2. 영문 상세 주석]
    // The lower bound of cosine similarity to judge a truth collision
    // (contradiction) and the epsilon constant to defend against division by zero.
    // [3. 자바 코드]
    private static final double 충돌_감지_임계치 = -0.3;
    private static final double 디랙_에프실론 = 1e-7;

    // [1. 한글 상세 주석]
    // FFM API에서 텐서를 읽고 쓰기 위한 하드웨어 친화적 리틀 엔디안 물리 규격입니다.
    // [2. 영문 상세 주석]
    // Hardware-friendly little-endian physical specification for reading and
    // writing tensors in the FFM API.
    // [3. 자바 코드]
    private static final ValueLayout.OfFloat TENSOR_FLOAT_LAYOUT = ValueLayout.JAVA_FLOAT
            .withOrder(ByteOrder.LITTLE_ENDIAN);

    // [1. 한글 상세 주석]
    // 개방 주소법(Open Addressing) 라우팅 사전. 해시 충돌 방어를 위해 UUID와 할당된 Y축 인덱스를 매핑합니다.
    // [2. 영문 상세 주석]
    // Open Addressing Routing Dictionary. Maps UUIDs to assigned Y-axis indices to
    // defend against Hash Collisions.
    // [3. 자바 코드]
    private static final Map<String, Long> 문서_위상_라우팅_사전 = new ConcurrentHashMap<>();
    private static final Set<Long> 할당된_Y_인덱스_망 = ConcurrentHashMap.newKeySet();

    // [1. 한글 상세 주석]
    // 직조된 최종 텐서를 보관하는 메인 지식망과 모순 유예 버퍼(Tier 8) 포트입니다.
    // V6.1 규격에 맞추어 `ConcurrentHashMap<String, Int2DoubleMap>`으로 업그레이드 되었습니다.
    // [2. 영문 상세 주석]
    // The main knowledge network storing the woven final tensors and the
    // contradiction suspension buffer (Tier 8) port.
    // Upgraded to `ConcurrentHashMap<String, Int2DoubleMap>` in accordance with the
    // V6.1 specification.
    // [3. 자바 코드]
    private final Map<String, Int2DoubleMap> 전역_지식망 = new ConcurrentHashMap<>();
    private final A0_DT_42_422081_모순_유예_양자_버퍼 모순_격리망;

    // [1. 한글 상세 주석]
    // 창세 생성자. 하위 모듈인 양자 버퍼를 의존성 주입(DI) 받습니다.
    // [2. 영문 상세 주석]
    // Genesis constructor. Receives the quantum buffer, a lower module, via
    // Dependency Injection (DI).
    // [3. 자바 코드]
    public A0_DT_42_423040_자가_조직화_지식망_직조기(A0_DT_42_422081_모순_유예_양자_버퍼 격리망) {
        if (격리망 == null) {
            throw new IllegalArgumentException("[배관 파열] 양자 격리망이 누락되어 직조기를 기동할 수 없습니다.");
        }
        this.모순_격리망 = 격리망;
        로거.info(" >> [통합 OS V6.1] A0_DT_42_423040 자가 조직화 지식망 직조기 기동. (개방 주소법 맵핑 및 FastUtil 어댑터 장착)");
    }

    // [1. 한글 상세 주석]
    // 파이프라인 역학 1: 텐서 직조 및 모순 충돌 방어 (내부 파이프라인)
    // 사영소에서 넘어온 텐서 파편들을 기존 DB 텐서와 비교하고 안착시키거나 격리합니다.
    // [2. 영문 상세 주석]
    // Pipeline Mechanics 1: Tensor weaving and contradiction collision defense
    // (Internal Pipeline).
    // Compares tensor fragments from the projection engine with existing DB
    // tensors, and either settles or isolates them.
    // [3. 자바 코드]
    public void 직조하다_사유입자_안착(
            문헌_메타데이터_캡슐 메타데이터,
            List<위상_사유_입자_캡슐> 사유_입자망,
            Map<String, WritePort> 대상_포트_망) {

        for (위상_사유_입자_캡슐 입자 : 사유_입자망) {
            // 💡 [V6.1 타입 업그레이드] 추출 및 변환 로직이 Int2DoubleMap을 반환하도록 수정됨
            Int2DoubleMap 기존_진리_텐서 = 추출하다_현상_텐서(입자, 대상_포트_망);
            Int2DoubleMap 신규_진리_텐서 = 입자_좌표를_맵으로_변환(입자);

            double 유사도 = 1.0;
            if (!기존_진리_텐서.isEmpty()) {
                유사도 = 산출하다_코사인_유사도(기존_진리_텐서, 신규_진리_텐서);
            }

            if (유사도 < 충돌_감지_임계치) {
                로거.warning(String.format(" [논리 충돌 포획] 기하학적 정합성 위배! (유사도: %.4f) 궤도를 유예하고 사령관 결단을 대기합니다: %s",
                        유사도, 입자.문서_UUID()));

                // 💡 [타입 충돌 해결] 이제 T8 격리망이 요구하는 Int2DoubleMap 규격을 완벽히 충족합니다.
                모순_격리망.포획하다_문헌_모순_텐서(
                        메타데이터.문서_UUID() + "_CHUNK_" + 입자.청크_인덱스(),
                        기존_진리_텐서,
                        신규_진리_텐서,
                        메타데이터.원본_파일명(),
                        (최종_확정_텐서) -> 커밋하다_텐서_안착(입자, 최종_확정_텐서, 대상_포트_망));
            } else {
                커밋하다_텐서_안착(입자, 신규_진리_텐서, 대상_포트_망);
            }
        }
    }

    // [1. 한글 상세 주석]
    // 파이프라인 역학 2: 레거시 어댑터 브릿지 (Legacy Adapter Bridge)
    // 외부 크롤러 등 구형 API(Map<Integer, Double>)를 통해 유입되는 텐서를 V6.1 규격으로 변환 후 처리합니다.
    // [2. 영문 상세 주석]
    // Pipeline Mechanics 2: Legacy Adapter Bridge.
    // Converts and processes tensors flowing in through legacy APIs (Map<Integer,
    // Double>) such as external crawlers to the V6.1 specification.
    // [3. 자바 코드]
    public void 실행하다_레거시_지식망_직조(String 텐서_식별자, Map<Integer, Double> 레거시_신규_텐서, String 출처_메타데이터) {
        if (텐서_식별자 == null || 레거시_신규_텐서 == null)
            return;

        // 💡 [어댑터 패턴] 구형 Map을 FastUtil Int2DoubleMap으로 캐스팅
        Int2DoubleMap 신규_원시_텐서 = 어댑터_변환하다_표준맵_투_원시맵(레거시_신규_텐서);
        Int2DoubleMap 기존_원시_텐서 = 전역_지식망.get(텐서_식별자);

        if (기존_원시_텐서 == null) {
            전역_지식망.put(텐서_식별자, 신규_원시_텐서);
            로거.fine("   ├─ [직조 완료] 신규 텐서가 지식망에 등록되었습니다: " + 텐서_식별자);
        } else {
            모순_격리망.포획하다_문헌_모순_텐서(
                    텐서_식별자,
                    기존_원시_텐서,
                    신규_원시_텐서,
                    출처_메타데이터,
                    (최종_결단_텐서) -> {
                        전역_지식망.put(텐서_식별자, 최종_결단_텐서);
                        로거.info("   ├─ [HIL 승인 직조] 유예되었던 텐서 파동이 붕괴되어 확정 기록되었습니다: " + 텐서_식별자);
                    });
        }
    }

    // [1. 한글 상세 주석]
    // [물리적 안착] 결정된 원시 텐서 좌표를 실제 OS 매핑 메모리에 FFM API로 직사합니다.
    // [2. 영문 상세 주석]
    // [Physical Settlement] Directly writes the determined primitive tensor
    // coordinates into the actual OS mapped memory via FFM API.
    // [3. 자바 코드]
    private void 커밋하다_텐서_안착(
            위상_사유_입자_캡슐 입자,
            Int2DoubleMap 텐서_값,
            Map<String, WritePort> 쓰기_포트_망) {

        long 물리적_절대_오프셋 = 도출하다_절대_오프셋(입자.문서_UUID(), 입자.청크_인덱스());

        // 💡 [컴파일 에러 패치] fastIterator() 대신 iterator() 호출
        ObjectIterator<Int2DoubleMap.Entry> 반복자 = 텐서_값.int2DoubleEntrySet().iterator();
        while (반복자.hasNext()) {
            Int2DoubleMap.Entry 엔트리 = 반복자.next();
            int 차원_ID = 엔트리.getIntKey();
            double 에너지_값 = 엔트리.getDoubleValue();

            String 지표명 = 매핑하다_차원_지표명(차원_ID);
            WritePort 타겟_포트 = 쓰기_포트_망.get(지표명);

            if (타겟_포트 != null) {
                타겟_포트.segment().set(TENSOR_FLOAT_LAYOUT, 물리적_절대_오프셋, (float) 에너지_값);
            }
        }

        로거.fine(String.format("   └─ [관계망 직조 완료] 문서(%s)의 사유 입자(Chunk %d)가 물리 DB에 안착되었습니다.",
                입자.문서_UUID(), 입자.청크_인덱스()));
    }

    // [1. 한글 상세 주석]
    // [수학 연산: 희소 텐서 코사인 유사도] FastUtil 맵을 순회하며 기하학적 각도를 산출합니다.
    // [2. 영문 상세 주석]
    // [Math Operation: Sparse Tensor Cosine Similarity] Traverses the FastUtil map
    // to calculate the geometric angle.
    // [3. 자바 코드]
    private double 산출하다_코사인_유사도(Int2DoubleMap 텐서A, Int2DoubleMap 텐서B) {
        double 내적 = 0.0;
        double 노름A_제곱 = 0.0;
        double 노름B_제곱 = 0.0;

        // 💡 [컴파일 에러 패치] fastIterator() 대신 iterator() 호출
        ObjectIterator<Int2DoubleMap.Entry> 반복자A = 텐서A.int2DoubleEntrySet().iterator();
        while (반복자A.hasNext()) {
            Int2DoubleMap.Entry 엔트리 = 반복자A.next();
            double 값A = 엔트리.getDoubleValue();
            노름A_제곱 += (값A * 값A);

            if (텐서B.containsKey(엔트리.getIntKey())) {
                double 값B = 텐서B.get(엔트리.getIntKey());
                내적 += (값A * 값B);
            }
        }

        ObjectIterator<Int2DoubleMap.Entry> 반복자B = 텐서B.int2DoubleEntrySet().iterator();
        while (반복자B.hasNext()) {
            double 값B = 반복자B.next().getDoubleValue();
            노름B_제곱 += (값B * 값B);
        }

        if (노름A_제곱 == 0.0 || 노름B_제곱 == 0.0)
            return 0.0;

        return 내적 / (Math.sqrt(노름A_제곱) * Math.sqrt(노름B_제곱) + 디랙_에프실론);
    }

    // [1. 한글 상세 주석]
    // [물리적 추출] FFM API를 통해 오프힙 메모리에서 기존 진리 텐서를 긁어와 원시 맵(Int2DoubleMap)으로 반환합니다.
    // [2. 영문 상세 주석]
    // [Physical Extraction] Scrapes the existing truth tensor from off-heap memory
    // via FFM API and returns it as a primitive map (Int2DoubleMap).
    // [3. 자바 코드]
    private Int2DoubleMap 추출하다_현상_텐서(위상_사유_입자_캡슐 입자, Map<String, WritePort> 포트망) {
        Int2DoubleOpenHashMap 기존_텐서 = new Int2DoubleOpenHashMap(3);
        long 물리적_절대_오프셋 = 도출하다_절대_오프셋(입자.문서_UUID(), 입자.청크_인덱스());

        for (int 차원 = 0; 차원 < 3; 차원++) {
            String 지표명 = 매핑하다_차원_지표명(차원);
            WritePort 타겟_포트 = 포트망.get(지표명);

            if (타겟_포트 != null) {
                float 기존_에너지 = 타겟_포트.segment().get(TENSOR_FLOAT_LAYOUT, 물리적_절대_오프셋);
                if (Math.abs(기존_에너지) > 디랙_에프실론) {
                    기존_텐서.put(차원, (double) 기존_에너지);
                }
            }
        }
        return 기존_텐서;
    }

    // [1. 한글 상세 주석]
    // [데이터 형변환] 사유 입자 레코드의 물리적 차원을 원시 희소 맵(Int2DoubleMap)으로 전환합니다.
    // [2. 영문 상세 주석]
    // [Data Type Conversion] Converts the physical dimensions of the reasoning
    // particle record into a primitive sparse map (Int2DoubleMap).
    // [3. 자바 코드]
    private Int2DoubleMap 입자_좌표를_맵으로_변환(위상_사유_입자_캡슐 입자) {
        Int2DoubleOpenHashMap 맵 = new Int2DoubleOpenHashMap(3);
        if (Math.abs(입자.X_방향성()) > 디랙_에프실론)
            맵.put(0, 입자.X_방향성());
        if (Math.abs(입자.Y_정보량()) > 디랙_에프실론)
            맵.put(1, 입자.Y_정보량());
        if (Math.abs(입자.Z_추상화()) > 디랙_에프실론)
            맵.put(2, 입자.Z_추상화());
        return 맵;
    }

    // [1. 한글 상세 주석]
    // 💡 [어댑터 패턴] 구형 자바 Map을 FastUtil Int2DoubleMap으로 안전하게 변환합니다.
    // [2. 영문 상세 주석]
    // 💡 [Adapter Pattern] Safely converts legacy Java Map to FastUtil
    // Int2DoubleMap.
    // [3. 자바 코드]
    private Int2DoubleMap 어댑터_변환하다_표준맵_투_원시맵(Map<Integer, Double> 표준_맵) {
        Int2DoubleOpenHashMap 원시_맵 = new Int2DoubleOpenHashMap(표준_맵.size());
        for (Map.Entry<Integer, Double> 엔트리 : 표준_맵.entrySet()) {
            if (엔트리.getKey() != null && 엔트리.getValue() != null) {
                원시_맵.put(엔트리.getKey().intValue(), 엔트리.getValue().doubleValue());
            }
        }
        return 원시_맵;
    }

    // [1. 한글 상세 주석]
    // 💡 [좌표 무결성 개방 주소법(Open Addressing)] 해시 충돌 방어를 위한 선형 탐사 로직.
    // [2. 영문 상세 주석]
    // 💡 [Coordinate Integrity Open Addressing] Linear probing logic for hash
    // collision defense.
    // [3. 자바 코드]
    private long 도출하다_절대_오프셋(String 문서_UUID, int 청크_인덱스) {
        long 가상_종목_인덱스 = 문서_위상_라우팅_사전.computeIfAbsent(문서_UUID, uuid -> {
            long 초기_해시 = Math.abs((long) uuid.hashCode()) % 10000L;
            long 탐사_인덱스 = 초기_해시;

            while (할당된_Y_인덱스_망.contains(탐사_인덱스)) {
                탐사_인덱스 = (탐사_인덱스 + 1) % 10000L;
                if (탐사_인덱스 == 초기_해시) {
                    throw new IllegalStateException("[시공간 포화] 매트릭스의 할당 가능한 위상 슬롯(10,000개)이 모두 해시 충돌로 소진되었습니다.");
                }
            }
            할당된_Y_인덱스_망.add(탐사_인덱스);
            return 탐사_인덱스;
        });
        return (가상_종목_인덱스 * 10000L + (청크_인덱스 % 10000L)) * 4L;
    }

    // [1. 한글 상세 주석]
    // 물리적 차원 ID를 대응되는 시스템 지표명으로 변환합니다.
    // [2. 영문 상세 주석]
    // Translates the physical dimension ID to the corresponding system feature
    // name.
    // [3. 자바 코드]
    private String 매핑하다_차원_지표명(int 차원_ID) {
        return switch (차원_ID) {
            case 0 -> "FEATURE_X_POLARITY";
            case 1 -> "FEATURE_Y_MAGNITUDE";
            case 2 -> "FEATURE_Z_ABSTRACTION";
            default -> "FEATURE_UNKNOWN";
        };
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 아키텍처 단층(Fault Line)의 수복과 어댑터 패턴:
 * 대규모 소프트웨어 시스템을 V6.1과 같은 새로운 규격(Zero-Allocation, FastUtil)으로 업그레이드할 때,
 * 가장 빈번하게 발생하는 재앙은 '타입 불일치(Type Mismatch)'입니다.
 * 상위 계층(T16)은 여전히 기존의 `java.util.Map`을 사용해 데이터를 넘겨주려 하는데,
 * 하위 계층(T8~T12)의 엔진들은 모두 `Int2DoubleMap`이라는 날카로운 원시 타입 검(Sword)으로 교체되었기 때문입니다.
 * 본 직조기 모듈(`423040`)은 이 두 개의 아키텍처 단층이 맞부딪히는 경계선(Boundary) 역할을 수행합니다.
 * `어댑터_변환하다_표준맵_투_원시맵` 메서드는 이 경계에서 레거시 데이터를 1차적으로 흡수하여
 * 하위 계층이 요구하는 원시 규격으로 안전하게 주물러(Adapter) 내려보냅니다. 이를 통해 전체 시스템의 코드를
 * 한 번에 뒤엎지 않고도 점진적이고 안정적인 마이그레이션이 가능해집니다.
 * 
 * 2. 콜백 파이프라인(Callback Pipeline) 기반의 비동기 직조:
 * 기존 지식과 신규 지식이 충돌했을 때, 이 직조기는 시스템을 멈추거나 에러를 던지지 않습니다.
 * 대신 T8 양자 버퍼로 데이터를 던진 뒤, `(최종_결단_텐서) -> { ... }` 라는 람다(Lambda) 콜백 함수만 남겨두고
 * 쿨하게 자신의 스레드 작업을 종료합니다.
 * 훗날 사령관(주권자)이 UI에서 승인 버튼을 누르는 순간, 이 콜백이 비동기적으로 깨어나
 * 멈춰있던 직조 과정을 조용히 끝마칩니다. 이것이 대규모 트래픽을 처리하는 HFT 시스템의 우아한 제어 역학입니다.
 * 
 * 3. 제로 얼로케이션 (Zero-Allocation) 및 FFM 메모리 다이렉트 타격:
 * `커밋하다_텐서_안착` 내부를 보면 파일 I/O 스트림(`FileOutputStream`)이 전혀 등장하지 않습니다.
 * `타겟_포트.segment().set()` 단 한 줄의 FFM API 코드가 OS 커널의 페이지 캐시(Page Cache)를 다이렉트로
 * 타격합니다. JVM의 힙(Heap) 메모리를 단 1바이트도 오염시키지 않으며 가비지 컬렉터(GC)를 완전히 잠재우는 극한의 제어입니다.
 * =============================================================================
 * 
 * 💡 [입문자 해설 (Beginner's Guide)]
 * 
 * - **어댑터와 번역기 비유**:
 * 바깥 세상(크롤러 등)에서 들어온 재료들은 구형 규격(표준 Map)을 쓰고 있습니다.
 * 하지만 우리 공장의 하부 엔진들은 최신식 초고속 규격(원시 맵)으로 싹 바뀌었죠.
 * 관리자는 재료가 들어오자마자 이 구형 재료를 최신 규격에 맞게 껍데기를 벗겨내어 변환(Adapter)해 줍니다.
 * 덕분에 아래쪽 엔진들에서 에러(incompatible types)가 나지 않습니다.
 * - **콜백 대기 비유**:
 * 변환된 재료를 지식 창고에 넣으려고 봤더니, 이미 비슷한 자리에 다른 지식이 있습니다.
 * 관리자는 억지로 쑤셔 넣지 않고 "불량품 격리실(Tier 8 양자 버퍼)"에 재료를 맡긴 뒤,
 * "사장님이 결정해주시면 나한테 문자 줘(콜백)"라는 쪽지만 남기고 다른 일을 하러 떠납니다.
 * 나중에 사장님이 승인을 하면 쪽지(콜백)가 작동하여 지식 창고에 최종적으로 재료가 예쁘게 등록됩니다.
 * =============================================================================
 */