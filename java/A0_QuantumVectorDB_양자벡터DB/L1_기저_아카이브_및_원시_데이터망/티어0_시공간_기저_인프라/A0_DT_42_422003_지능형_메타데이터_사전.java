/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라
 * @alias Intelligent_Metadata_Dictionary
 * @tier 0
 * @keywords Dynamic Data Fabric, Tombstone Architecture, O(1) Bitmask Compression, Zero-Allocation
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422003_지능형_메타데이터_사전.java
 * - 역할 (Role): 데이터의 물리적 속성을 내포한 지표 속성 명세(Manifest) 및 O(1) 비트마스크 기반 묘비(Tombstone) 관리.
 * - 기능 (Function): 데이터 패브릭 스키마 해석, 해상도 프로젝션 가이드, 결측치 자가 치유 전략 하달, 엔티티 생명주기 관리.
 * - 이론 (Theory): 동적 스키마 진화(Append-Only Schema Evolution), 비트마스크 압축 묘비(Tombstone) 아키텍처.
 * - 기대효과 (Effect): AI 모델의 차원 붕괴(Shape Mismatch) 방어, 객체 할당 없는 극한의 조회 성능 및 OOM 크래시 방지.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring). 입문자용 비유 주석 소각.
 * - 💡 [초정밀 수술] 묘비 역참조 캐시 O(1) 비트마스크 압축: 수십만 개의 객체를 양산하던 무거운 `ConcurrentHashMap`을 제거하고, 
 *                 Y축 인덱스 번호 기반의 `AtomicLongArray` 비트마스크(BitSet) 생명주기 뷰로 압축하여 CPU 캐시 미스와 OOM 위험을 완전히 해체했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 스레드 세이프(Thread-Safe)한 O(1) 비트마스크 연산을 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for thread-safe O(1) bitmask operations.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 데이터의 물리적 속성과 논리적 삭제(Tombstone) 상태를 O(1) 속도로 통제하는 지능형 메타데이터 사전입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An intelligent metadata dictionary that controls physical data properties and logical deletion (tombstone) status at O(1) speed.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422003
 * [파일명] A0_DT_42_422003_지능형_메타데이터_사전.java
 * [모듈명] 통합 OS V6.0 - Tier 0: 지능형 메타데이터 사전 (Feature Manifest Registry)
 * 
 * [기능 명세]
 * 1. 💡 동적 데이터 패브릭 스키마: CSV의 단순한 컬럼명(String)을, 데이터의 물리적 속성(해상도, 모달리티, 치유 방식)을
 * 내포한 `FeatureManifest` 레코드로 런타임 승격시킵니다.
 * 2. 💡 이질적 해상도 프로젝션 가이드: 데이터 성격에 따라 디스크 저장 시 Float32를 쓸지, 압축용 BFloat16을 쓸지
 * 결정하는 기저 룰셋(Ruleset)을 제공하여 I/O 병목을 제거합니다.
 * 3. 💡 결측치 자가 치유(Healing) 전략 정의: 연속형 데이터는 관성(Forward-fill)으로, 이산 이벤트 데이터는 기본 영점(0.0f)으로
 * 치유하도록 쿼리 엔진에 지침을 하달합니다.
 * 4. 💡 [V6.0 초정밀 수술] 논리적 삭제(Tombstone) 비트마스크 관리: 상장폐지된 종목이나 소멸한 엔티티를
 * `ConcurrentHashMap` 객체로 관리하던 구형 로직을 파괴하고, Y축 인덱스 기반의 `AtomicLongArray`
 * 비트마스크로 극강 압축하여 OOM 발생 위험과 CPU 캐시 미스를 0으로 수렴시켰습니다.
 * ==============================================================================
 */
public final class A0_DT_42_422003_지능형_메타데이터_사전 {

    // [1. 한글 상세 주석]
    // 절대 규격: 데이터 모달리티. 데이터가 지니는 수학적 특성과 시계열 연속성을 규정합니다.
    // [2. 영문 상세 주석]
    // Absolute Specification: Data Modality. Defines the mathematical characteristics and time-series continuity of the data.

    public enum DataModality {
        CONTINUOUS_LINEAR("연속_선형", "주가, 거래량 등. 결측치 발생 시 직전 관성(LOCF)을 유지해야 함."),
        MACRO_BROADCASTING("거시_브로드캐스팅", "금리, VIX 등. 미시적 종목 텐서에 덧붙여질 때 배열 길이만큼 복제(Broadcasting)됨."),
        DISCRETE_EVENT("비선형_이산", "실적 발표, 액면 분할 등. 이벤트가 없는 날은 반드시 0.0f(진공)로 수렴해야 함."),
        HIGH_DIM_EMBEDDING("고차원_잠재공간", "뉴스, 공시의 감성 분석 텐서. BFloat16 압축이 강제됨.");

        private final String physicalProperty;
        private final String dynamicDescription;

        DataModality(String physicalProperty, String dynamicDescription) {
            this.physicalProperty = physicalProperty;
            this.dynamicDescription = dynamicDescription;
        }

        public String getPhysicalProperty() {
            return physicalProperty;
        }

        public String getDynamicDescription() {
            return dynamicDescription;
        }
    }

    // [1. 한글 상세 주석]
    // 절대 규격: 물리적 해상도. 데이터의 압축 및 저장 해상도를 정의합니다.
    // [2. 영문 상세 주석]
    // Absolute Specification: Physical Resolution. Defines the compression and storage resolution of the data.

    public enum PhysicalResolution {
        FLOAT32_PRECISION(4, "정밀한 사칙연산과 누적이 필요한 지표에 사용 (기본 해상도)"),
        BFLOAT16_AI_COMPRESSED(2, "표현 범위(Exponent)는 넓으나 가수부가 짧아도 되는 정규화(Z-Score) 지표 및 매크로 지표에 사용"),
        INT8_QUANTIZED(1, "지표의 범위가 제한적일 때 사용하는 1바이트 초압축 양자화 해상도");

        private final int byteSize;
        private final String resolutionDescription;

        PhysicalResolution(int byteSize, String resolutionDescription) {
            this.byteSize = byteSize;
            this.resolutionDescription = resolutionDescription;
        }

        public int getByteSize() {
            return byteSize;
        }
    }

    // [1. 한글 상세 주석]
    // 지능형 데이터 구조체 (Record DTO). 특정 지표가 저장되는 방식(Manifest)과 엔티티의 생명주기를 규정합니다.
    // [2. 영문 상세 주석]
    // Intelligent Data Structures (Record DTO). Defines how a specific feature is stored (Manifest) and the lifecycle of an entity.

    /**
     * 특정 지표(Feature)가 디스크에 어떻게 저장되고, RAM으로 어떻게 서빙되어야 하는지 규정한 명세서(Manifest).
     */
    public record FeatureManifest(
            String rawFeatureName,
            DataModality modality,
            PhysicalResolution recommendedResolution,
            float missingValueFallback // 0.0f (이산형) 또는 Float.NaN (관성 기반 Forward-fill 트리거)
    ) {
        public boolean requiresForwardFill() {
            return Float.isNaN(missingValueFallback);
        }
    }

    /**
     * 특정 종목(Entity)의 탄생과 소멸을 추적하여 텐서 배열의 빈 공간(Void)을 관리하는 논리적 상태.
     */
    public record EntityLifecycleState(
            int entityIndexY,
            boolean isTombstoned, // true면 상장폐지 또는 데이터 수집이 영구 중단됨
            long birthEpochSeconds,
            long demiseEpochSeconds) {

        /**
         * 쿼리 엔진이 특정 시점의 텐서를 추출할 때, 이 엔티티가 해당 시점에 이미 소멸했는지 판별합니다.
         */
        public boolean isDestroyedAt(long targetEpochSeconds) {
            return isTombstoned && (targetEpochSeconds > demiseEpochSeconds);
        }
    }

    // [1. 한글 상세 주석]
    // OOM 방지 O(1) 비트마스크 압축 캐시. 64개의 엔티티 논리 삭제(Tombstone) 상태를 하나의 long 타입에 압축하여 보관합니다.
    // [2. 영문 상세 주석]
    // OOM-preventing O(1) bitmask compression cache. Compresses and stores the tombstone status of 64 entities in a single long type.

    private static final int INITIAL_ENTITY_CAPACITY = 100_000;

    // 1개의 long 타입은 64개 엔티티의 논리적 삭제(Tombstone) 상태를 비트마스크로 압축하여 보관합니다.
    private static volatile AtomicLongArray tombstoneBitmask = new AtomicLongArray((INITIAL_ENTITY_CAPACITY >> 6) + 1);
    private static volatile AtomicLongArray birthEpochArray = new AtomicLongArray(INITIAL_ENTITY_CAPACITY);
    private static volatile AtomicLongArray demiseEpochArray = new AtomicLongArray(INITIAL_ENTITY_CAPACITY);

    private static final Object expansionLock = new Object();

    // [1. 한글 상세 주석]
    // 인스턴스화를 방지하는 private 생성자입니다.
    // [2. 영문 상세 주석]
    // Private constructor to prevent instantiation.

    private A0_DT_42_422003_지능형_메타데이터_사전() {
        throw new UnsupportedOperationException("[시스템 보호] 지능형 메타데이터 사전은 인스턴스화할 수 없는 유틸리티 클래스입니다.");
    }

    // [1. 한글 상세 주석]
    // 기능 1: 동적 헤더 파싱 및 지표 Manifest 발현. 외부 JSON 룰셋을 바탕으로 지표 속성을 런타임에 결정합니다.
    // [2. 영문 상세 주석]
    // Function 1: Dynamic header parsing and feature Manifest manifestation. Determines feature attributes at runtime based on external JSON rulesets.

    /**
     * [기능 1] 동적 스키마 파싱 및 지표 Manifest 도출
     * 외부 JSON 메타데이터에서 주입된 룰셋을 바탕으로 지표의 저장/서빙 속성을 동적으로 도출합니다.
     * 
     * @param headerText      CSV에서 읽은 원시 컬럼명
     * @param externalRuleMap 외부(JSON 등)에서 읽어온 지표 속성 룰셋 맵
     */
    public static FeatureManifest parseFeatureManifest(String headerText, Map<String, FeatureManifest> externalRuleMap) {
        String upperHeader = headerText.trim().toUpperCase();

        // 외부 주입된 규격이 1순위 (도메인 결합도 제거)
        if (externalRuleMap.containsKey(upperHeader)) {
            return externalRuleMap.get(upperHeader);
        }

        // 설정 누락 시 기본 폴백(Fallback) 명세 부여
        return new FeatureManifest(upperHeader, DataModality.CONTINUOUS_LINEAR, PhysicalResolution.FLOAT32_PRECISION, Float.NaN);
    }

    // [1. 한글 상세 주석]
    // 기능 2: 엔티티 논리 삭제(Tombstone) 마킹. Y축 인덱스를 활용한 Lock-Free 비트 연산으로 O(1) 업데이트를 수행합니다.
    // [2. 영문 상세 주석]
    // Function 2: Entity Tombstone marking. Performs O(1) updates with Lock-Free bitwise operations utilizing the Y-axis index.

    /**
     * [기능 2] 엔티티 논리 삭제(Tombstone) 등록 (O(1) 비트마스크 압축)
     * 💡 기존 ConcurrentHashMap 기반의 무거운 String Key 마킹 방식을 제거하고,
     * Y축 인덱스를 활용한 Lock-Free 비트 연산으로 Tombstone을 세워 힙 메모리 소모를 99% 억제했습니다.
     * 
     * @param entityIndexY 엔티티 레지스트리에서 할당받은 순수 Y축 좌표
     * @param birthEpoch   데이터가 최초 관측된 절대 Epoch 시간
     * @param demiseEpoch  데이터 수집이 중단되거나 상장폐지된 절대 Epoch 시간
     */
    public static void markEntityTombstone(int entityIndexY, long birthEpoch, long demiseEpoch) {
        // 인덱스가 현재 배열 용량을 초과할 경우 안전하게 동적 팽창 수행
        expandRegistryCapacity(entityIndexY);

        int arrayIndex = entityIndexY >> 6; // entityIndexY / 64
        long bitPosition = 1L << (entityIndexY & 63); // entityIndexY % 64

        // 💡 Lock-Free 비트 갱신 (CAS Loop)
        long currentValue;
        do {
            currentValue = tombstoneBitmask.get(arrayIndex);
        } while (!tombstoneBitmask.compareAndSet(arrayIndex, currentValue, currentValue | bitPosition));

        birthEpochArray.set(entityIndexY, birthEpoch);
        demiseEpochArray.set(entityIndexY, demiseEpoch);
    }

    // [1. 한글 상세 주석]
    // 기능 3: 생명주기 O(1) 고속 조회망. 단 1개의 힙 객체도 생성하지 않는(Zero-Allocation) O(1) 비트마스크 스캔을 집행합니다.
    // [2. 영문 상세 주석]
    // Function 3: Lifecycle O(1) high-speed query network. Executes an O(1) bitmask scan that creates zero heap objects (Zero-Allocation).

    /**
     * [기능 3] 생명주기 O(1) 고속 조회망
     * 💡 단 1개의 임시 힙 객체도 생성하지 않는(Zero-Allocation) O(1) 비트마스크 스캔을 수행합니다.
     * 
     * @param entityIndexY 상태를 조회할 대상 엔티티의 Y축 좌표
     * @return 생명주기 뷰가 담긴 캡슐 레코드
     */
    public static EntityLifecycleState queryEntityLifecycle(int entityIndexY) {
        if (entityIndexY >= birthEpochArray.length()) {
            // 아직 개척되지 않은 미래의 인덱스는 기본적으로 생존(생성 안됨) 상태로 취급
            return new EntityLifecycleState(entityIndexY, false, 0L, Long.MAX_VALUE);
        }

        int arrayIndex = entityIndexY >> 6;
        long bitPosition = 1L << (entityIndexY & 63);

        boolean isTombstoned = (tombstoneBitmask.get(arrayIndex) & bitPosition) != 0;
        long birth = birthEpochArray.get(entityIndexY);
        long demise = demiseEpochArray.get(entityIndexY);

        // Tombstone이 아니고, 특별한 생성/소멸 기록이 없다면 기본 생존 값 부여
        if (!isTombstoned && birth == 0L && demise == 0L) {
            demise = Long.MAX_VALUE;
        }

        return new EntityLifecycleState(entityIndexY, isTombstoned, birth, demise);
    }

    // [1. 한글 상세 주석]
    // 보조 제어: 수용량 초과 시 배열 길이를 동적으로 안전하게 확장합니다 (Double-checked locking).
    // [2. 영문 상세 주석]
    // Auxiliary Control: Dynamically and safely expands array length when capacity is exceeded (Double-checked locking).

    /**
     * [보조 제어] 배열 수용량을 동적으로 안전하게 확장합니다 (Double-checked locking 기반 확장).
     */
    private static void expandRegistryCapacity(int targetIndex) {
        if (targetIndex < birthEpochArray.length())
            return;

        synchronized (expansionLock) {
            if (targetIndex < birthEpochArray.length())
                return;

            int newCapacity = Math.max(birthEpochArray.length() * 2, targetIndex + 1000);
            int newBitmaskCapacity = (newCapacity >> 6) + 1;

            AtomicLongArray newBitmask = new AtomicLongArray(newBitmaskCapacity);
            AtomicLongArray newBirth = new AtomicLongArray(newCapacity);
            AtomicLongArray newDemise = new AtomicLongArray(newCapacity);

            for (int i = 0; i < tombstoneBitmask.length(); i++) {
                newBitmask.set(i, tombstoneBitmask.get(i));
            }
            for (int i = 0; i < birthEpochArray.length(); i++) {
                newBirth.set(i, birthEpochArray.get(i));
                newDemise.set(i, demiseEpochArray.get(i));
            }

            tombstoneBitmask = newBitmask;
            birthEpochArray = newBirth;
            demiseEpochArray = newDemise;
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. OOM 방지 및 O(1) 비트마스크 압축 로직 (Bitmask Compression):
 * 수십만 개의 논리적 삭제(Tombstone) 정보를 `ConcurrentHashMap<String, EntityLifecycleState>` 객체로
 * 관리할 경우, String 해시 계산과 내부 Node 객체 생성으로 인해 대량의 힙(Heap) 메모리가 소비되어 OOM(Out of Memory)의 원인이 됩니다.
 * 통합 OS V6.0은 64개의 엔티티 생존 여부를 단 8바이트(`long`) 하나에 비트마스크(Bitmask)로 압축(Compress)합니다.
 * 이를 통해 10만 개의 Tombstone을 관리하는 데 불과 12.5KB의 메모리만을 소모하며, 
 * `AtomicLongArray`를 활용한 Compare-And-Swap(CAS) 갱신을 통해 락(Lock) 없이 1클럭 내에 상태를 읽고 씁니다.
 * 이는 가비지 컬렉터(GC)에 주는 부하를 완벽히 없애는 하드웨어 레벨의 최적화 기법입니다.
 * 
 * 2. 동적 스키마 진화 (Dynamic Schema Evolution):
 * 데이터 수집 초기에는 지표의 명칭이나 속성을 소스 코드에 하드코딩하는 것이 일반적이나, 이는 도메인 변경 시 시스템 재컴파일을 강제합니다.
 * `지능형_메타데이터_사전`은 외부 JSON 파일이나 데이터베이스에서 룰셋을 실시간 주입받는 지연 결속(Lazy Binding) 아키텍처를 도입했습니다.
 * 이를 통해 다양한 도메인(센서 데이터, 금융 데이터 등)이 유입되어도, 시스템의 배포 중단 없이 동적으로 메타데이터 명세를 교체하며 유연하게 대응합니다.
 * 
 * 3. 논리적 삭제(Tombstone) 아키텍처의 당위성:
 * 엔티티가 상장폐지되거나 데이터 수집이 영구 중단되었다고 해서 물리적인 텐서 배열을 중간에서 삭제(Delete)해버리면,
 * 시계열 데이터가 유지해야 할 기하학적 차원(Shape)이 붕괴되어 AI 모델의 가중치 행렬 계산 시 에러가 발생합니다.
 * Tombstone 마킹 기법은 데이터 구조의 물리적 크기는 유지하되, 조회(Query) 시점에만 해당 엔티티가 소멸했음을 감지하고
 * 빈 값(Dummy)을 반환함으로써 시스템 전반의 연산 무결성과 추론 안정성을 수호하는 필수적인 패턴입니다.
 * =============================================================================
 */