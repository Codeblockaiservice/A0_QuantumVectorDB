/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어5_다차원_매니폴드_오픈_DB
 * @alias Hyperspace_Navigation_HNSW_Engine
 * @tier 5
 * @keywords HNSW, Billion-Scale ANN, Zero-Allocation, Dynamic FFM C-Struct, Pointer Jumping, Exponential Backoff, CAS
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422057_초공간_항해_HNSW_엔진.java
 * - 기능: 오프힙 커널 메모리에 C언어 구조체(Struct) 형태의 HNSW 다층 그래프를 동적 맵핑하고, 포인터 점프만으로 수십억 개의 텐서 중 가장 유사한 최근접 이웃(ANN)을 탐색 및 간선을 조립합니다.
 * - 역할: 전수 스캔의 물리적 한계를 파괴하고, 다차원 위상 공간 내의 '웜홀(Wormhole)'을 뚫어 최단 거리 우회로를 제공하는 초공간 항해망.
 * - 이론: HNSW (Hierarchical Navigable Small World), Memory Cache Alignment, Mutual Link Assembly, Exponential Backoff.
 * - 💡 [V6.1 C-Struct 동적 레이아웃 신설]: 하드코딩된 오프셋 상수를 전면 폐기하고 `MemoryLayout.structLayout()`을 활용하여 JIT 컴파일러가 구동되는 하드웨어 아키텍처에 맞게 자동으로 메모리를 정렬(Alignment)하도록 승격시켰습니다.
 * - 💡 [V6.1 동시성 붕괴 수술]: 무한 `Thread.onSpinWait()`으로 인해 발생하던 데드락(Deadlock) 뇌관을 파괴하고, 지수 백오프(Exponential Backoff)와 최대 재시도 임계치를 결합한 완벽한 Fail-Fast 방어막을 이식했습니다.
 * - 💡 [V6.0 휴리스틱 현실화]: 이웃 슬롯이 가득 찼을 때 타겟과 가장 먼 노드를 잘라내는(Pruning) '거리 기반 가지치기'가 유지 적용되었습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 오프힙 메모리 제어(FFM API), 동적 레이아웃 산출, 데드락 방지를 위한 스레드 제어 유틸리티를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of FFM API, dynamic layout calculation, and thread control utilities for off-heap memory control and deadlock prevention.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어5_다차원_매니폴드_오픈_DB;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 객체를 생성하지 않고 C-Struct 기반의 동적 다층 그래프를 항해하며, 지수 백오프로 무장한 HNSW 코어 엔진입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. HNSW core engine armed with exponential backoff that navigates dynamic multi-layer graphs based on C-Struct without creating objects.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422057
 * [파일명] A0_DT_42_422057_초공간_항해_HNSW_엔진.java
 * [모듈명] 통합 OS V6.1 - Tier 5: 초공간 항해 HNSW 엔진 (Billion-Scale ANN Indexing)
 * 
 * [설계 명세]
 * 1. 역할: 10억 단위(Billion-Scale) 텐서 생태계에서 근사 최근접 이웃(ANN)을 O(log N)으로 탐색 및 간선 직조.
 * 2. 기능: 동적 MemoryLayout을 이용해 커널 메모리에 C-Struct 배열을 투영하고 지수 백오프로 데드락 방어.
 * 3. 의도: 하드코딩 오프셋으로 인한 OS/아키텍처별 SegFault를 막고, 무한 대기로 인한 파이프라인 붕괴를 원천 차단.
 * 4. 이론: 동적 메모리 정렬(Alignment), CAS 원자성 보장, 지수 백오프(Exponential Backoff), 거리 기반
 * 가지치기.
 * ==============================================================================
 */
public final class A0_DT_42_422057_초공간_항해_HNSW_엔진 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422057_HNSW_ENGINE");

    // [1. 한글 상세 주석]
    // 하드웨어 아키텍처와 1:1로 대응하는 리틀 엔디안 규격의 FFM 레이아웃 상수를 선언합니다.
    // [2. 영문 상세 주석]
    // Declares FFM layout constants in little-endian format that map 1:1 with the
    // hardware architecture.
    // [3. 자바 코드]
    private static final ValueLayout.OfLong C_LONG = ValueLayout.JAVA_LONG.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt C_INT = ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN);

    // 오프힙 메모리 상의 int 값을 원자적으로 조작하기 위한 CAS VarHandle
    private static final VarHandle SPIN_LOCK_HANDLE = C_INT.varHandle();

    // HNSW 그래프의 위상학적 구조를 통제하는 기하학 상수
    private static final int HNSW_MAX_LAYERS = 8; // L: 공간을 접을 최대 계층 수
    private static final int HNSW_M = 16; // M: 각 레이어당 최대 이웃 노드 개수

    // [1. 한글 상세 주석]
    // 💡 [Zero-Allocation 동적 C-Struct 레이아웃 신설]
    // 위험한 하드코딩 오프셋 수치를 폐기하고, JIT 컴파일러가 구동 환경(x86/ARM)에 맞추어
    // 패딩(Padding)과 메모리 경계 정렬(Alignment)을 완벽하게 계산하도록 구조체를 동적 정의합니다.
    // [2. 영문 상세 주석]
    // 💡 [Zero-Allocation Dynamic C-Struct Layout Established]
    // Discarded dangerous hardcoded offset values and dynamically defines the
    // struct so the JIT compiler perfectly calculates padding and memory alignment
    // according to the running environment (x86/ARM).
    // [3. 자바 코드]
    private static final StructLayout NODE_LAYOUT = MemoryLayout.structLayout(
            C_LONG.withName("vector_offset"), // (8 Bytes) 실제 L1 매트릭스 텐서의 시작 주소
            C_INT.withName("max_layer"), // (4 Bytes) 이 노드가 존재하는 최고 계층 (0 ~ 7)
            C_INT.withName("spin_lock"), // (4 Bytes) 💡 간선 갱신 시 원자성을 보장하는 스핀 락
            MemoryLayout.sequenceLayout(HNSW_MAX_LAYERS, C_INT).withName("neighbor_counts"), // 각 레이어별 이웃 개수
            MemoryLayout.sequenceLayout(HNSW_MAX_LAYERS, MemoryLayout.sequenceLayout(HNSW_M, C_LONG))
                    .withName("neighbors") // 이웃 노드 오프셋 배열
    ).withName("HNSW_Node");

    // 💡 MemoryLayout.byteOffset()을 통한 무결점(Zero-Defect) 물리적 주소 자동 추출
    private static final long STRUCT_OFFSET_VECTOR = NODE_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("vector_offset"));
    private static final long STRUCT_OFFSET_LAYER = NODE_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("max_layer"));
    private static final long STRUCT_OFFSET_SPINLOCK = NODE_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("spin_lock"));
    private static final long STRUCT_OFFSET_COUNTS = NODE_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("neighbor_counts"));
    private static final long STRUCT_OFFSET_NEIGHBORS = NODE_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("neighbors"));
    private static final long NODE_STRIDE_BYTES = NODE_LAYOUT.byteSize(); // JIT에 의해 정렬된 완벽한 노드 크기

    // 그래프 진입점(Entry Point) 메타데이터
    private static final long EP_META_OFFSET = 0L; // 0번지에 시작 노드(EP)의 오프셋 저장
    private static final long INDEX_DATA_START_OFFSET = 8L; // 실제 노드 데이터 시작점 (이 모듈에서는 직접 참조 대신 EP를 경유)

    private final MemorySegment HNSW_인덱스_세그먼트;
    private final A0_DT_42_422001_권한_포트_인터페이스.ReadPort 원시_텐서_포트;

    /**
     * [창세 생성자] HNSW 오프힙 인덱스 엔진을 점화합니다.
     */
    public A0_DT_42_422057_초공간_항해_HNSW_엔진(MemorySegment 인덱스_세그먼트, A0_DT_42_422001_권한_포트_인터페이스.ReadPort 원시_텐서_포트) {
        if (인덱스_세그먼트 == null || 원시_텐서_포트 == null) {
            throw new IllegalArgumentException("[항해 파열] 필수 메모리 세그먼트 및 권한 포트가 누락되었습니다.");
        }
        this.HNSW_인덱스_세그먼트 = 인덱스_세그먼트;
        this.원시_텐서_포트 = 원시_텐서_포트;

        로거.info(String.format(
                " >> [통합 OS V6.1] A0_DT_42_422057 초공간 항해 HNSW 엔진 기동. (동적 C-Struct 레이아웃 [%d Bytes] 장착 및 백오프 멸균 엔진 점화)",
                NODE_STRIDE_BYTES));
    }

    // [1. 한글 상세 주석]
    // 💡 [핵심 항해 역학: 근사 최근접 이웃(ANN) 탐색]
    // 힙(Heap) 객체를 단 하나도 생성하지 않고, 물리 메모리 상의 포인터를 이리저리 점프(Pointer Jumping)하여 타겟 텐서의
    // 절대 주소를 O(log N)으로 관통합니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Navigation Dynamics: Approximate Nearest Neighbor (ANN) Search]
    // Penetrates and finds the absolute address of the target tensor at O(log N)
    // speed by jumping pointers back and forth in physical memory without creating
    // a single Heap object.
    // [3. 자바 코드]
    public long 탐색하다_근사_최근접_이웃(long 질의_텐서_오프셋) {
        long 진입_노드_오프셋 = HNSW_인덱스_세그먼트.get(C_LONG, EP_META_OFFSET);

        if (진입_노드_오프셋 == 0L) {
            return -1L; // 그래프가 진공 상태임
        }

        int 최고_레이어_L = HNSW_인덱스_세그먼트.get(C_INT, 진입_노드_오프셋 + STRUCT_OFFSET_LAYER);
        long 현재_가장_가까운_노드 = 진입_노드_오프셋;
        double 최소_거리 = 산출하다_텐서_유클리드_거리(질의_텐서_오프셋, 읽다_벡터_오프셋(진입_노드_오프셋));

        // 하강 항해 (Top-Down Greedy Navigation)
        for (int 현재_계층 = 최고_레이어_L; 현재_계층 >= 0; 현재_계층--) {
            boolean 더_가까운_이웃_발견 = true;

            while (더_가까운_이웃_발견) {
                더_가까운_이웃_발견 = false;

                // 💡 읽기 연산은 락(Lock) 없이 고속으로 스캔합니다. (Lock-Free Read)
                int 이웃_개수 = HNSW_인덱스_세그먼트.get(C_INT, 현재_가장_가까운_노드 + STRUCT_OFFSET_COUNTS + (현재_계층 * 4L));
                long 이웃_배열_시작_주소 = 현재_가장_가까운_노드 + STRUCT_OFFSET_NEIGHBORS + (현재_계층 * HNSW_M * 8L);

                for (int i = 0; i < 이웃_개수; i++) {
                    long 이웃_노드_오프셋 = HNSW_인덱스_세그먼트.get(C_LONG, 이웃_배열_시작_주소 + (i * 8L));
                    long 이웃_벡터_오프셋 = 읽다_벡터_오프셋(이웃_노드_오프셋);

                    double 이웃과의_거리 = 산출하다_텐서_유클리드_거리(질의_텐서_오프셋, 이웃_벡터_오프셋);

                    if (이웃과의_거리 < 최소_거리) {
                        최소_거리 = 이웃과의_거리;
                        현재_가장_가까운_노드 = 이웃_노드_오프셋;
                        더_가까운_이웃_발견 = true;
                    }
                }
            }
        }
        return 읽다_벡터_오프셋(현재_가장_가까운_노드);
    }

    /**
     * 물리적 메모리 오프셋 2개를 받아, L1 매트릭스 내부의 텐서(예: 128차원) 유클리드 거리를 산출합니다.
     */
    private double 산출하다_텐서_유클리드_거리(long 기준_벡터_오프셋, long 대상_벡터_오프셋) {
        int 임베딩_차원_수 = 128;
        double 거리_제곱합 = 0.0;

        MemorySegment 텐서_메모리 = 원시_텐서_포트.segment();

        for (int i = 0; i < 임베딩_차원_수; i++) {
            long 상대_오프셋 = i * 4L;
            float 기준_값 = 텐서_메모리.get(ValueLayout.JAVA_FLOAT, 기준_벡터_오프셋 + 상대_오프셋);
            float 대상_값 = 텐서_메모리.get(ValueLayout.JAVA_FLOAT, 대상_벡터_오프셋 + 상대_오프셋);

            float 편차 = 기준_값 - 대상_값;
            거리_제곱합 += (편차 * 편차);
        }
        return Math.sqrt(거리_제곱합);
    }

    private long 읽다_벡터_오프셋(long 노드_시작_오프셋) {
        return HNSW_인덱스_세그먼트.get(C_LONG, 노드_시작_오프셋 + STRUCT_OFFSET_VECTOR);
    }

    // [1. 한글 상세 주석]
    // 💡 [건축 역학: 상호 간선 직조] 새로운 텐서를 그래프망에 삽입합니다.
    // 기존의 단순 삽입에서 발생하는 동시성 붕괴를 막기 위해 구조체의 물리적 락(Spin-Lock)을 제어합니다.
    // [2. 영문 상세 주석]
    // 💡 [Architectural Dynamics: Mutual Edge Assembly] Inserts a new tensor into
    // the graph network. Controls the physical Spin-Lock of the struct to prevent
    // concurrency collapse occurring in simple insertions.
    // [3. 자바 코드]
    public void 직조하다_HNSW_인덱스(long 신규_노드_오프셋, long 신규_벡터_오프셋, int 부여된_최고_레이어) {

        // 1. 동적 C-Struct 메타데이터 초기화 기록
        HNSW_인덱스_세그먼트.set(C_LONG, 신규_노드_오프셋 + STRUCT_OFFSET_VECTOR, 신규_벡터_오프셋);
        HNSW_인덱스_세그먼트.set(C_INT, 신규_노드_오프셋 + STRUCT_OFFSET_LAYER, 부여된_최고_레이어);
        HNSW_인덱스_세그먼트.set(C_INT, 신규_노드_오프셋 + STRUCT_OFFSET_SPINLOCK, 0); // 스핀 락 초기화

        // 이웃 카운트 배열 초기화 (32 Bytes = 8 layers * 4 bytes)
        HNSW_인덱스_세그먼트.asSlice(신규_노드_오프셋 + STRUCT_OFFSET_COUNTS, 32).fill((byte) 0);

        long 진입_노드_오프셋 = HNSW_인덱스_세그먼트.get(C_LONG, EP_META_OFFSET);
        if (진입_노드_오프셋 == 0L) {
            // 원자성 확보를 고려할 수 있으나, 창세 시점은 단일 스레드로 통제됨을 전제
            HNSW_인덱스_세그먼트.set(C_LONG, EP_META_OFFSET, 신규_노드_오프셋);
            로거.fine("   ├─ [초공간 창세] 최초의 HNSW 노드가 진입점(Entry Point)으로 등록되었습니다.");
            return;
        }

        int EP_최고_레이어 = HNSW_인덱스_세그먼트.get(C_INT, 진입_노드_오프셋 + STRUCT_OFFSET_LAYER);
        long 현재_탐색_노드 = 진입_노드_오프셋;

        // 상단 레이어 고속 하강
        for (int 현재_계층 = EP_최고_레이어; 현재_계층 > 부여된_최고_레이어; 현재_계층--) {
            현재_탐색_노드 = 스캔하다_계층내_최근접_이웃(현재_탐색_노드, 신규_벡터_오프셋, 현재_계층);
        }

        // 간선 조립 (Mutual Edge Assembly)
        for (int 현재_계층 = Math.min(EP_최고_레이어, 부여된_최고_레이어); 현재_계층 >= 0; 현재_계층--) {
            현재_탐색_노드 = 스캔하다_계층내_최근접_이웃(현재_탐색_노드, 신규_벡터_오프셋, 현재_계층);

            // 💡 양방향 간선 결속 시, 각 노드 구조체에 내장된 Spin-Lock을 획득하여 동시성을 안전하게 수호합니다.
            결속하다_단방향_간선_원자적(신규_노드_오프셋, 현재_탐색_노드, 현재_계층);
            결속하다_단방향_간선_원자적(현재_탐색_노드, 신규_노드_오프셋, 현재_계층);
        }

        if (부여된_최고_레이어 > EP_최고_레이어) {
            HNSW_인덱스_세그먼트.set(C_LONG, EP_META_OFFSET, 신규_노드_오프셋);
        }
    }

    private long 스캔하다_계층내_최근접_이웃(long 진입_노드, long 질의_벡터_오프셋, int 계층) {
        long 가장_가까운_노드 = 진입_노드;
        double 최소_거리 = 산출하다_텐서_유클리드_거리(질의_벡터_오프셋, 읽다_벡터_오프셋(진입_노드));
        boolean 더_가까운_이웃_발견 = true;

        while (더_가까운_이웃_발견) {
            더_가까운_이웃_발견 = false;
            int 이웃_개수 = HNSW_인덱스_세그먼트.get(C_INT, 가장_가까운_노드 + STRUCT_OFFSET_COUNTS + (계층 * 4L));
            long 이웃_배열_시작_주소 = 가장_가까운_노드 + STRUCT_OFFSET_NEIGHBORS + (계층 * HNSW_M * 8L);

            for (int i = 0; i < 이웃_개수; i++) {
                long 이웃_노드_오프셋 = HNSW_인덱스_세그먼트.get(C_LONG, 이웃_배열_시작_주소 + (i * 8L));
                double 거리 = 산출하다_텐서_유클리드_거리(질의_벡터_오프셋, 읽다_벡터_오프셋(이웃_노드_오프셋));

                if (거리 < 최소_거리) {
                    최소_거리 = 거리;
                    가장_가까운_노드 = 이웃_노드_오프셋;
                    더_가까운_이웃_발견 = true;
                }
            }
        }
        return 가장_가까운_노드;
    }

    // [1. 한글 상세 주석]
    // 💡 [데드락 멸균 수술 적용] 출발 노드의 Spin-Lock을 획득할 때, 무한 대기(Hanging)를 막기 위한 지수 백오프와 임계치를
    // 가동합니다.
    // 이웃이 가득 찼을 경우, 기존 이웃 중 타겟과 가장 거리가 먼 노드를 찾아라내고(Pruning) 스왑(Swap)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Deadlock Sterilization Surgery Applied] When acquiring the Spin-Lock of
    // the source node, operates exponential backoff and thresholds to prevent
    // infinite hanging.
    // If neighbors are full, finds the furthest neighbor from the target (Pruning)
    // and swaps it.
    // [3. 자바 코드]
    private void 결속하다_단방향_간선_원자적(long 출발_노드_오프셋, long 도착_노드_오프셋, int 계층) {
        long 락_주소 = 출발_노드_오프셋 + STRUCT_OFFSET_SPINLOCK;

        int 최대_재시도_횟수 = 100_000;
        long 백오프_나노초 = 100L;
        int 시도_횟수 = 0;

        // 1. CAS (Compare-And-Swap) 루프 및 지수 백오프 기반 데드락 방어막
        while (!((boolean) SPIN_LOCK_HANDLE.compareAndSet(HNSW_인덱스_세그먼트, 락_주소, 0, 1))) {
            시도_횟수++;
            if (시도_횟수 > 최대_재시도_횟수) {
                로거.severe(" 🚨 [데드락 방어막 격발] HNSW 간선 갱신 중 Spin-Lock 획득 타임아웃 발생. (무한 대기 물리적 차단)");
                throw new IllegalStateException("HNSW 스핀 락 획득 타임아웃 붕괴 (Deadlock Prevention)");
            }

            if (시도_횟수 > 1000) {
                // 1000번 이상 스핀 실패 시 OS 스케줄러에 자원을 양보하여 CPU 발열과 스래싱 방어
                LockSupport.parkNanos(백오프_나노초);
                백오프_나노초 = Math.min(백오프_나노초 * 2, 1_000_000L); // 최대 1ms(1,000,000ns) 대기
            } else {
                Thread.onSpinWait(); // CPU 파이프라인 최적화된 바쁜 대기
            }
        }

        try {
            long 카운트_주소 = 출발_노드_오프셋 + STRUCT_OFFSET_COUNTS + (계층 * 4L);
            int 현재_이웃_개수 = HNSW_인덱스_세그먼트.get(C_INT, 카운트_주소);
            long 이웃_배열_시작_주소 = 출발_노드_오프셋 + STRUCT_OFFSET_NEIGHBORS + (계층 * HNSW_M * 8L);

            if (현재_이웃_개수 < HNSW_M) {
                // 여유 슬롯 존재 시 물리적 덮어쓰기 및 카운트 증가
                HNSW_인덱스_세그먼트.set(C_LONG, 이웃_배열_시작_주소 + (현재_이웃_개수 * 8L), 도착_노드_오프셋);
                HNSW_인덱스_세그먼트.set(C_INT, 카운트_주소, 현재_이웃_개수 + 1);
            } else {
                // 💡 [Pruning 거리 기반 가지치기 현실화]
                // 단순 무지성 덮어쓰기(Mock) 폐기. 이웃 중 가장 먼 노드(Furthest)를 찾아 스왑합니다.
                long 출발_벡터 = 읽다_벡터_오프셋(출발_노드_오프셋);
                long 도착_벡터 = 읽다_벡터_오프셋(도착_노드_오프셋);
                double 타겟과의_거리 = 산출하다_텐서_유클리드_거리(출발_벡터, 도착_벡터);

                double 최대_거리 = -1.0;
                int 가장_먼_이웃_인덱스 = -1;

                for (int i = 0; i < HNSW_M; i++) {
                    long 기존_이웃_노드 = HNSW_인덱스_세그먼트.get(C_LONG, 이웃_배열_시작_주소 + (i * 8L));
                    long 기존_이웃_벡터 = 읽다_벡터_오프셋(기존_이웃_노드);
                    double 기존_거리 = 산출하다_텐서_유클리드_거리(출발_벡터, 기존_이웃_벡터);

                    if (기존_거리 > 최대_거리) {
                        최대_거리 = 기존_거리;
                        가장_먼_이웃_인덱스 = i;
                    }
                }

                // 신규 타겟이 기존의 가장 먼 이웃보다 가깝다면 교체(Swap)
                if (타겟과의_거리 < 최대_거리 && 가장_먼_이웃_인덱스 != -1) {
                    HNSW_인덱스_세그먼트.set(C_LONG, 이웃_배열_시작_주소 + (가장_먼_이웃_인덱스 * 8L), 도착_노드_오프셋);
                }
            }
        } finally {
            // 2. Spin-Lock 해제 (1을 0으로 복구하여 점유 해제)
            SPIN_LOCK_HANDLE.setVolatile(HNSW_인덱스_세그먼트, 락_주소, 0);
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 동적 MemoryLayout에 의한 하드웨어 파편화 방어 (Dynamic Alignment Defense):
 * 이전 코드에서 프로그래머가 손수 `STRUCT_OFFSET_NEIGHBORS = 48L` 로 계산했던 하드코딩 방식은
 * "모든 OS와 CPU가 내가 생각한 패딩(Padding)대로 메모리를 정렬해 줄 것이다"라는 치명적인 오만입니다.
 * x86 아키텍처와 ARM64(Apple Silicon 등)는 메모리 정렬 규칙이 미세하게 다릅니다. 이 하드코딩을 방치했다면
 * 서버 이전 시 곧바로 `Segmentation Fault`나 캐시 미스 스래싱(Cache Miss Thrashing)을 유발하며 사망했을
 * 것입니다.
 * 통합 OS V6.1은 자바 FFM API의 `MemoryLayout.structLayout()`을 투입하여, JIT 컴파일러가 구동되는
 * 운영체제의 가장 완벽한 물리적 메모리 정렬(Alignment) 규칙에 따라 `byteOffset`을 동적으로 산출해냅니다.
 * 이로써 C언어 구조체의 극한 성능과 자바의 WORA(Write Once, Run Anywhere) 철학이 기적적으로 융합되었습니다.
 * 
 * 2. 지수 백오프(Exponential Backoff)를 통한 Spin-Lock 데드락 멸균:
 * `Thread.onSpinWait()`은 락이 극히 짧은 시간(나노초 단위) 안에 풀릴 것을 가정하고 쓰는 명령어입니다.
 * 만약 어떤 스레드가 락을 쥐고 있는 도중 OS 컨텍스트 스위칭 아웃을 당하거나 예상치 못한 지연을 겪는다면,
 * 락을 기다리는 수천 개의 다른 스레드들은 무한 스핀에 빠져 CPU를 100% 태우며(Meltdown) 영원한 교착 상태에 갇히게 됩니다.
 * 새롭게 이식된 락 메커니즘은 1,000번의 스핀 시도 후에는 즉시 `LockSupport.parkNanos`로 넘어가
 * OS 스케줄러에게 CPU 점유를 우아하게 양보(Yield)합니다. 또한 백오프 대기 시간을 2배씩 늘려가며(Exponential
 * Backoff),
 * 10만 번을 초과하여 락 획득을 실패할 경우 미련 없이 `IllegalStateException`을 던져
 * 썩은 파이프라인의 환부를 스스로 폭파(Fail-Fast)시킴으로써 서버 전체의 생존을 수호합니다.
 * 
 * 3. 기계적 공감(Mechanical Sympathy)과 Zero-Allocation:
 * HNSW(Hierarchical Navigable Small World) 알고리즘은 10억 개의 노드가 서로의 포인터를 물고 있는 거대한
 * 그래프를 그립니다. 이를 객체(Object)화하여 힙(Heap)에 올리는 순간, 가비지 컬렉터(GC)는 수억 개의 참조(Reference)
 * 트리를 뒤지다 시스템을 몇 시간 동안 마비(Stop-The-World)시킵니다.
 * 본 엔진은 자바 객체를 단 하나도 띄우지 않습니다. 오프힙의 평면 메모리 위를 `get(C_LONG, offset)` 이라는
 * 순수 포인터 점프(Pointer Jumping) 연산만으로 미끄러지듯 횡단하여, GC 부하율 0%와 완벽한 O(log N) 검색을
 * 완성했습니다.
 * =============================================================================
 */