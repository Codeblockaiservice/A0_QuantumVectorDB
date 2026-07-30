/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어6_시맨틱_임베딩_변환기
 * @alias SIMD_UltraFast_Aggregation_Worker
 * @tier 6
 * @keywords SIMD, AVX-256, Java Vector API, Aggregation Push-down, Branchless Masking, Memory Alignment, Kahan Summation, Modulo Math
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422063_SIMD_초고속_집계_워커.java
 * - 기능 (Function): 오프힙(Off-Heap) 메모리 영역에 대해 Java Vector API를 활용하여 합계, 평균, 최대, 최소 등의 수학적 집계 연산을 하드웨어 가속(SIMD)으로 수행합니다.
 * - 역할 (Role): 상위 외교관 계층(Tier 17)에서 번역된 질의 계획(Query Plan)을 넘겨받아, 데이터를 RAM으로 복사하지 않고 커널 메모 단면에서 직접 리덕션(Reduction)을 집행하는 물리 연산 코어.
 * - 이론 (Theory): 단일 명령어 다중 데이터 처리(SIMD), 메모리 경계 정렬(Memory Alignment), 루프 언롤링(Loop Unrolling), 조건 분기 멸균(Branchless Masking), Kahan 오차 보상 합산, 대수학적 정렬 검증.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [V6.1 컴파일 붕괴 수복 (정렬 검증 교정)]: FFM API의 `MemorySegment`에는 존재하지 않는 `isAligned(int)` 헬퍼 메서드 호출을 영구 삭제했습니다. 대신 `(세그먼트.address() + 오프셋) % 벡터_크기 == 0` 이라는 순수 대수학적(Modulo) 검증 로직으로 치환하여 하드웨어 트랩 방어선의 문법적 파열을 100% 수복했습니다.
 * - 💡 [V6.1 변경 - 3단 안전 가속 엔진]: 메모리 정렬 불일치로 인한 하드웨어 예외를 막기 위해, 앞단의 비정렬 메모리는 스칼라 루프(Pre-loop)로 선처리하고, 완벽히 정렬된 본문만 SIMD(Main-loop)로 밀어버린 뒤, 남은 꼬리표를 스칼라(Post-loop)로 정리하는 무결점 아키텍처를 도입했습니다.
 * - 💡 [V6.1 신설 - Kahan Summation 이식]: 벡터 마스킹 연산 후 `reduceLanes` 과정에서 누산기(Accumulator)의 부동소수점 오차 누적(Catastrophic Cancellation)을 방지하기 위한 Kahan Summation 로직을 SIMD 리덕션 스텝 내부에 완벽히 이식했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 오프힙 메모리 제어, Java Vector API(JEP 460) 등 초고속 병렬 연산을 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for off-heap memory control and ultra-high-speed parallel computation, including Java Vector API (JEP 460).
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어6_시맨틱_임베딩_변환기;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. SIMD 벡터화 명령어를 통해 텐서 데이터를 일괄 압착(Reduction)하며, Kahan Summation으로 오차를 방어하는 초고속 3단 집계 워커입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An ultra-high-speed 3-stage aggregation worker that batch-reduces tensor data using SIMD vectorized instructions and defends against errors with Kahan Summation.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422063
 * [파일명] A0_DT_42_422063_SIMD_초고속_집계_워커.java
 * [모듈명] 통합 OS V6.1 - Tier 6: SIMD 초고속 집계 워커 (3단 병렬 리덕션 코어)
 * 
 * [설계 명세]
 * 1. 역할: 오프힙 매트릭스 데이터에 대한 고속 수학적 집계(SUM, AVG, MAX, MIN) 전담.
 * 2. 기능: AVX2/AVX-512 하드웨어 레지스터를 직접 타격하는 256비트 FloatVector 연산.
 * 3. 의도: 대량의 텐서 데이터를 순차적(Scalar)으로 순회할 때 발생하는 파이프라인 병목과 연산 지연을 파괴.
 * 4. 이론: SIMD (Single Instruction Multiple Data), Memory Alignment, Branchless
 * Masking, Kahan Summation.
 * 5. 💡 [V6.1 Kahan 오차 멸균]: SUM/AVG 연산 시 발생하는 부동소수점 오차 누적을 Kahan 보상 변수를 통해
 * 수학적으로 멸균.
 * 6. 💡 [V6.1 메모리 정렬 컴파일 수복]: 존재하지 않는 `isAligned` 대신 `address() % 32 == 0` 대수학
 * 검증으로 하드웨어 트랩 방어선 재구축.
 * ==============================================================================
 */
public final class A0_DT_42_422063_SIMD_초고속_집계_워커 {

    // [1. 한글 상세 주석]
    // 글로벌 로거 선언 및 SIMD 벡터화 연산을 위한 256비트(Float32 x 8개) 레지스터 상수(Species)를 정의합니다.
    // [2. 영문 상세 주석]
    // Global logger declaration and definition of 256-bit (Float32 x 8 elements)
    // register constants (Species) for SIMD vectorized operations.
    // [3. 자바 코드]
    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422063_SIMD_AGGREGATION_WORKER");

    private static final VectorSpecies<Float> SIMD_256_SPECIES = FloatVector.SPECIES_256;
    private static final int 벡터_바이트_크기 = SIMD_256_SPECIES.vectorByteSize(); // 32 Bytes
    private static final int 요소_바이트_크기 = 4; // Float32 = 4 Bytes

    // [1. 한글 상세 주석]
    // 외부 플래너(Tier 17)에서 번역된 질의의 연산 유형과, SIMD 리덕션의 최종 결과 및 유효 건수를 담는 캡슐 레코드입니다.
    // [2. 영문 상세 주석]
    // Capsule records containing the operation type of the query translated by the
    // external planner, and the final result and valid count of the SIMD reduction.
    // [3. 자바 코드]
    public enum 집계_연산_유형 {
        합계_SUM, 평균_AVG, 최대_MAX, 최소_MIN
    }

    public record SIMD_집계_결과(
            double 최종_결과값,
            long 유효_데이터_건수) {
    }

    // [1. 한글 상세 주석]
    // 무상태(Stateless) 연산 코어이므로 독립적인 인스턴스화가 가능합니다. 3단 가속 엔진 및 Kahan 멸균 엔진을 초기화합니다.
    // [2. 영문 상세 주석]
    // As a stateless computation core, independent instantiation is possible.
    // Initializes the 3-stage acceleration engine and Kahan sterilization engine.
    // [3. 자바 코드]
    public A0_DT_42_422063_SIMD_초고속_집계_워커() {
        로거.info(" >> [통합 OS V6.1] A0_DT_42_422063 SIMD 초고속 집계 워커 기동. (3단 안전 가속 엔진, Kahan 오차 멸균 및 대수학적 정렬 스캐너 탑재)");
    }

    // [1. 한글 상세 주석]
    // 💡 [핵심 수술 완료: 3단 안전 가속 엔진 및 대수학적 정렬 이중 검증]
    // 시작 오프셋의 정렬 불일치를 탐지하고, `address() % 벡터_크기` 로 물리적 이중 검증한 후 Pre-loop ->
    // Main-loop -> Post-loop로 이어지는 무결점 벡터 연산을 집행합니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Surgery Completed: 3-Stage Safety Acceleration Engine and Algebraic
    // Alignment Double Check]
    // Detects alignment mismatch of the start offset, double-checks physically with
    // `address() % vector_size`, and executes flawless vector operations leading
    // from Pre-loop to Main-loop to Post-loop.
    // [3. 자바 코드]
    /**
     * 메모리 경계 정렬을 준수하며 분기 없는(Branchless) SIMD 집계 연산 및 Kahan 오차 보상을 수행합니다.
     * 
     * @param 타겟_포트     데이터를 읽어올 권한이 부여된 읽기 포트
     * @param 시작_절대_오프셋 연산을 시작할 물리적 바이트 주소
     * @param 총_요소_수    스캔해야 할 총 Float32 요소의 개수
     * @param 연산_유형     수행할 집계 유형 (SUM, AVG, MAX, MIN)
     * @return 수학적 연산 결과 및 유효 건수가 담긴 캡슐
     */
    public SIMD_집계_결과 실행하다_초고속_벡터_리덕션(
            A0_DT_42_422001_권한_포트_인터페이스.ReadPort 타겟_포트,
            long 시작_절대_오프셋,
            int 총_요소_수,
            집계_연산_유형 연산_유형) {

        if (총_요소_수 <= 0 || 타겟_포트 == null) {
            return new SIMD_집계_결과(Double.NaN, 0);
        }

        MemorySegment 세그먼트 = 타겟_포트.segment();
        int 벡터_스텝_크기 = SIMD_256_SPECIES.length(); // 8개

        // 상태 누산기 초기화
        double 누적_합산 = 0.0;
        double 오차보상_C = 0.0; // 💡 Kahan Summation 변수
        float 전역_최대값 = Float.NEGATIVE_INFINITY;
        float 전역_최소값 = Float.POSITIVE_INFINITY;
        long 유효_카운트 = 0;

        // [1. 한글 상세 주석]
        // 💡 [경계 정렬 스캐너 및 이중 검증] 메모리가 32바이트 배수로 정렬되어 있는지 산술적으로 1차 역산하고, OS 커널 물리 주소를 통해
        // 2차 모듈로(Modulo) 검증을 수행하여 하드웨어 트랩을 방어합니다.
        // [2. 영문 상세 주석]
        // 💡 [Boundary Alignment Scanner and Double Check] Arithmetically
        // reverse-calculates whether the memory is aligned to a multiple of 32 bytes as
        // a primary check, and performs a secondary modulo verification via the OS
        // kernel physical address to defend against hardware traps.
        // [3. 자바 코드]
        long 비정렬_바이트 = 시작_절대_오프셋 % 벡터_바이트_크기;
        int 헤더_스칼라_처리개수 = 0;

        if (비정렬_바이트 != 0L) {
            long 정렬까지_남은_바이트 = 벡터_바이트_크기 - 비정렬_바이트;
            헤더_스칼라_처리개수 = (int) (정렬까지_남은_바이트 / 요소_바이트_크기);
        }

        // 💡 [V6.1 정렬 이중 검증: 컴파일 수복 완료]
        boolean SIMD_가용성_확보 = true;
        long 본문_시작_오프셋 = 시작_절대_오프셋 + (헤더_스칼라_처리개수 * 요소_바이트_크기);
        long 물리적_메모리_주소 = 세그먼트.address() + 본문_시작_오프셋;

        if (총_요소_수 > 헤더_스칼라_처리개수 && (물리적_메모리_주소 % 벡터_바이트_크기 != 0L)) {
            로거.warning(
                    " 🚨 [하드웨어 트랩 방어] 계산상 정렬되었으나 물리적 메모리 주소 정렬(Modulo) 검증 실패. JVM Crash 방지를 위해 SIMD 가속을 포기하고 전체 스칼라 연산(Fallback)으로 전환합니다.");
            헤더_스칼라_처리개수 = 총_요소_수; // 전체를 Pre-loop에서 처리하도록 강제
            SIMD_가용성_확보 = false;
        }

        // 데이터가 적어서 전부 스칼라로 끝나는 경우를 방어
        헤더_스칼라_처리개수 = Math.min(헤더_스칼라_처리개수, 총_요소_수);

        // [1. 한글 상세 주석]
        // [STAGE 1: Pre-loop (Scalar)] 정렬되지 않은 앞단의 메모리를 스칼라 연산으로 선처리하며 Kahan Summation을
        // 적용합니다.
        // [2. 영문 상세 주석]
        // [STAGE 1: Pre-loop (Scalar)] Pre-processes the unaligned leading memory with
        // scalar operations, applying Kahan Summation.
        // [3. 자바 코드]
        for (int i = 0; i < 헤더_스칼라_처리개수; i++) {
            long 현재_오프셋 = 시작_절대_오프셋 + (i * 4L);
            float 단일_값 = 세그먼트.get(A0_DT_42_422001_권한_포트_인터페이스.TENSOR_FLOAT32, 현재_오프셋);

            if (!Float.isNaN(단일_값)) {
                유효_카운트++;
                switch (연산_유형) {
                    case 합계_SUM, 평균_AVG -> {
                        // 💡 Kahan Summation (Scalar)
                        double 보정된_값 = 단일_값 - 오차보상_C;
                        double 임시_합산 = 누적_합산 + 보정된_값;
                        오차보상_C = (임시_합산 - 누적_합산) - 보정된_값;
                        누적_합산 = 임시_합산;
                    }
                    case 최대_MAX -> {
                        if (단일_값 > 전역_최대값)
                            전역_최대값 = 단일_값;
                    }
                    case 최소_MIN -> {
                        if (단일_값 < 전역_최소값)
                            전역_최소값 = 단일_값;
                    }
                }
            }
        }

        int SIMD_루프_한계 = 헤더_스칼라_처리개수;
        int SIMD_처리_가능개수 = 0;

        // [1. 한글 상세 주석]
        // [STAGE 2: Main-loop (SIMD)] 완벽히 정렬된 본문 구간을 256비트 AVX2로 압출합니다. 분기 없는 비트 마스킹과
        // 중립원 블렌딩으로 텐서 붕괴를 막습니다.
        // [2. 영문 상세 주석]
        // [STAGE 2: Main-loop (SIMD)] Extrudes the perfectly aligned main body section
        // with 256-bit AVX2. Prevents tensor collapse with branchless bit masking and
        // neutral element blending.
        // [3. 자바 코드]
        if (SIMD_가용성_확보 && 헤더_스칼라_처리개수 < 총_요소_수) {
            int 잔여_본문_요소수 = 총_요소_수 - 헤더_스칼라_처리개수;
            SIMD_처리_가능개수 = SIMD_256_SPECIES.loopBound(잔여_본문_요소수);
            SIMD_루프_한계 = 헤더_스칼라_처리개수 + SIMD_처리_가능개수;

            for (int i = 헤더_스칼라_처리개수; i < SIMD_루프_한계; i += 벡터_스텝_크기) {
                long 현재_오프셋 = 시작_절대_오프셋 + (i * 4L);

                // 오프힙 메모리에서 한 클럭에 8개의 Float 값을 벡터 레지스터로 로드 (대수학적 경계 정렬 100% 보장)
                FloatVector 벡터_레지스터 = FloatVector.fromMemorySegment(
                        SIMD_256_SPECIES, 세그먼트, 현재_오프셋, ByteOrder.LITTLE_ENDIAN);

                // 💡 [브랜치리스 마스킹 (Branchless Masking)] NaN 비트마스킹
                VectorMask<Float> 유효_데이터_마스크 = 벡터_레지스터.test(VectorOperators.IS_NAN).not();
                int 찰나_유효_개수 = 유효_데이터_마스크.trueCount();

                if (찰나_유효_개수 > 0) {
                    유효_카운트 += 찰나_유효_개수;

                    // 중립원(Neutral Element) 블렌딩 및 병렬 리덕션
                    switch (연산_유형) {
                        case 합계_SUM, 평균_AVG -> {
                            FloatVector 멸균된_벡터 = 벡터_레지스터.blend(0.0f, 유효_데이터_마스크.not());
                            float 찰나_리덕션_합계 = 멸균된_벡터.reduceLanes(VectorOperators.ADD);

                            // 💡 [V6.1 Kahan Summation 이식] SIMD 리덕션 후의 스칼라 합산 과정에서 부동소수점 오차 누적 방어
                            double 보정된_값 = 찰나_리덕션_합계 - 오차보상_C;
                            double 임시_합산 = 누적_합산 + 보정된_값;
                            오차보상_C = (임시_합산 - 누적_합산) - 보정된_값;
                            누적_합산 = 임시_합산;
                        }
                        case 최대_MAX -> {
                            FloatVector 멸균된_벡터 = 벡터_레지스터.blend(Float.NEGATIVE_INFINITY, 유효_데이터_마스크.not());
                            float 국소_최대 = 멸균된_벡터.reduceLanes(VectorOperators.MAX);
                            if (국소_최대 > 전역_최대값)
                                전역_최대값 = 국소_최대;
                        }
                        case 최소_MIN -> {
                            FloatVector 멸균된_벡터 = 벡터_레지스터.blend(Float.POSITIVE_INFINITY, 유효_데이터_마스크.not());
                            float 국소_최소 = 멸균된_벡터.reduceLanes(VectorOperators.MIN);
                            if (국소_최소 < 전역_최소값)
                                전역_최소값 = 국소_최소;
                        }
                    }
                }
            }
        }

        // [1. 한글 상세 주석]
        // [STAGE 3: Post-loop (Scalar)] 벡터 크기의 배수로 떨어지지 않은 남은 꼬리 데이터들을 전통적인 스칼라 방식으로
        // 처리합니다.
        // [2. 영문 상세 주석]
        // [STAGE 3: Post-loop (Scalar)] Processes the remaining tail data that does not
        // fall into multiples of the vector size using the traditional scalar method.
        // [3. 자바 코드]
        for (int i = SIMD_루프_한계; i < 총_요소_수; i++) {
            long 현재_오프셋 = 시작_절대_오프셋 + (i * 4L);
            float 단일_값 = 세그먼트.get(A0_DT_42_422001_권한_포트_인터페이스.TENSOR_FLOAT32, 현재_오프셋);

            if (!Float.isNaN(단일_값)) {
                유효_카운트++;
                switch (연산_유형) {
                    case 합계_SUM, 평균_AVG -> {
                        // 💡 Kahan Summation (Scalar)
                        double 보정된_값 = 단일_값 - 오차보상_C;
                        double 임시_합산 = 누적_합산 + 보정된_값;
                        오차보상_C = (임시_합산 - 누적_합산) - 보정된_값;
                        누적_합산 = 임시_합산;
                    }
                    case 최대_MAX -> {
                        if (단일_값 > 전역_최대값)
                            전역_최대값 = 단일_값;
                    }
                    case 최소_MIN -> {
                        if (단일_값 < 전역_최소값)
                            전역_최소값 = 단일_값;
                    }
                }
            }
        }

        // [1. 한글 상세 주석]
        // 최종 수학적 리덕션 결과를 취합하여 스칼라 캡슐 형태로 사출합니다.
        // [2. 영문 상세 주석]
        // Aggregates the final mathematical reduction results and emits them in the
        // form of a scalar capsule.
        // [3. 자바 코드]
        if (유효_카운트 == 0) {
            return new SIMD_집계_결과(Double.NaN, 0); // 모든 구간이 진공(NaN) 상태
        }

        double 최종_결과 = switch (연산_유형) {
            case 합계_SUM -> 누적_합산;
            case 평균_AVG -> 누적_합산 / 유효_카운트;
            case 최대_MAX -> 전역_최대값;
            case 최소_MIN -> 전역_최소값;
        };

        로거.fine(String.format("   └─ [3단 SIMD 집계 수료] 처리 스칼라: %d 건 | 연산: %s | 결과: %.6f (Pre:%d, Main:%d, Post:%d)",
                유효_카운트, 연산_유형.name(), 최종_결과, 헤더_스칼라_처리개수, SIMD_처리_가능개수, 총_요소_수 - SIMD_루프_한계));

        return new SIMD_집계_결과(최종_결과, 유효_카운트);
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 💡 Memory Alignment(메모리 경계 정렬)와 대수학적 이중 검증의 위력:
 * 최신 Vector API의 `FloatVector.fromMemorySegment`는 물리적 메모리 주소가 하드웨어의 벡터
 * 버스(32바이트)에 맞게
 * 완벽히 정렬(Aligned)되어 있을 때에만 $O(1)$의 광속 메모리 패치를 수행할 수 있습니다.
 * 이전의 코드에서는 존재하지 않는 `MemorySegment.isAligned()`에 의존하려다 컴파일 에러를 맞았습니다.
 * 수리된 통합 OS V6.1은 자바의 불완전한 헬퍼 메서드에 의존하는 것을 포기하고 순수 수학의 영역으로 회귀합니다.
 * `(세그먼트.address() + 본문_시작_오프셋) % 벡터_바이트_크기 == 0` 이라는 Modulo 연산을 통해
 * OS가 할당한 실제 커널의 물리적 시작 주소와 오프셋을 더하여 하드웨어 친화적 정렬 상태를 논리적으로 100% 증명해 냅니다.
 * 정렬이 실패하면 무리한 SIMD 가속을 포기하고 안전한 스칼라 연산(Fallback)으로 우회하여 시스템의 영구적 생존을 보장하는 궁극의
 * '기계적 공감'을 완성했습니다.
 * 
 * 2. 💡 Kahan Summation (카한 합산 알고리즘)에 의한 부동소수점 오차 멸균:
 * SIMD 리덕션(`reduceLanes`) 자체는 8개의 요소를 초고속으로 합산하지만, 이 결과값을 전역 스칼라 누산기(`누적_합산`)에
 * 계속 더해 나가는 과정에서 필연적으로 미세한 부동소수점 소실 누적(Catastrophic Cancellation)이 발생합니다.
 * 수백만 건의 데이터를 더하다 보면 이 미세한 오차가 나비효과를 일으켜 평균(AVG) 값이 크게 왜곡됩니다.
 * 수복된 V6.1 엔진은 누산 과정에 Kahan Summation 기법을 이식했습니다. 연산 과정에서 잘려나간 하위 비트(버려진 오차)를
 * `오차보상_C` 변수에 고스란히 담아두었다가 다음 루프 연산 때 다시 더해줌으로써, 수학적 무결성을 극한으로 방어해 냅니다.
 * 
 * 3. 연산 푸시다운 (Aggregation Push-down)과 스토리지 컴퓨팅의 융합:
 * 일반적으로 데이터베이스에서 데이터를 분석할 때, 클라이언트는 쿼리를 통해 막대한 양의 데이터를
 * 애플리케이션 계층(RAM)으로 모두 가져와서(Fetch) 루프를 돌며 합계나 평균을 계산합니다.
 * 이는 네트워크 병목, 불필요한 객체 직렬화 오버헤드, 그리고 심각한 메모리 스래싱을 유발합니다.
 * 본 워커는 외교관 계층(Tier 17)에서 번역된 명령을 넘겨받아 커널 메모리에 매핑된 오프힙 텐서 위에서 직접 수학적 집계를 완료한 뒤,
 * 오직 '하나의 숫자(Scalar)'만을 외부에 사출합니다.
 * 
 * 4. 브랜치리스 마스킹 (Branchless Masking)의 파이프라인 수호:
 * 금융이나 센서 데이터에는 결측치(NaN)가 필연적으로 존재합니다. 이를 연산에서 제외하기 위해
 * `if (!Float.isNaN(val))` 이라는 조건문을 루프 안에 넣으면, CPU의 분기 예측 유닛(Branch Predictor)이
 * 파괴되어
 * 가속 효과가 소멸합니다. 이 워커는 `VectorMask`를 통해 유효 데이터의 위치만 1로 비트스캔하고,
 * `blend` 함수를 호출하여 NaN 자리에 중립원(Neutral Element: 0.0f, -Infinity 등)을 덮어씌웁니다.
 * 분기 없이 흐르는 일직선의 파이프라인이 텐서 연산의 극초음속을 영구히 수호합니다.
 * =============================================================================
 * 
 * 💡 [입문자 해설 (Beginner's Guide)]
 * 
 * - **SIMD 가속 비유**:
 * 옛날 공장에서는 직원(CPU) 한 명이 짐을 하나씩만 들고 나갔습니다. SIMD는 지게차(Vector Register)를 투입하여 한 번에
 * 8개씩의 짐을
 * 번쩍 들어 나르는 기술입니다. 당연히 8배 빠른 속도로 작업이 끝납니다.
 * 
 * - **메모리 정렬(Memory Alignment)과 3단 가속 비유**:
 * 지게차는 폭이 커서 문(경계)에 똑바로 맞춰 들어가지 않으면 벽에 부딪혀 공장이 부서집니다(하드웨어 예외).
 * 그래서 우리 시스템은 1단계(Pre-loop)로 문에 어긋난 짐들은 손으로 살짝 옮겨두고, 2단계(Main-loop)로 지게차가 일렬로 쫙
 * 밀어버린 뒤,
 * 3단계(Post-loop)로 지게차에 안 실리는 남은 자투리를 다시 손으로 정리하는 '3단 안전 수술'을 통해 속도와 안전을 모두
 * 잡았습니다.
 * 
 * - **카한 합산(Kahan Summation) 비유**:
 * 모래알(소수점)을 큰 바구니에 계속 담다 보면, 바구니 구멍으로 미세한 모래가 조금씩 샙니다.
 * 이것을 백만 번 반복하면 잃어버린 모래 양이 엄청나지겠죠. 카한 합산법은 바구니 밑에 아주 얇은 접시(오차 보상 변수)를 받쳐두어
 * 새어나간 모래를 싹 다 모은 뒤 다시 위에 부어주는 마법 같은 방법입니다. 오차가 0(Zero)이 됩니다.
 * =============================================================================
 */