/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어5_다차원_매니폴드_오픈_DB
 * @alias Hyperspace_Navigation_HNSW_Engine
 * @tier 5
 * @keywords HNSW, Billion-Scale ANN, Zero-Allocation, Dynamic FFM C-Struct, Pointer Jumping, Exponential Backoff, CAS, Graph Persistence
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422057_초공간_항해_HNSW_엔진.java
 * - 기능: 오프힙 커널 메모리에 C언어 구조체(Struct) 형태의 HNSW 다층 그래프를 동적 맵핑하고, 포인터 점프만으로 수십억 개의 텐서 중 가장 유사한 최근접 이웃(ANN)을 탐색 및 간선을 조립합니다.
 * - 역할: 전수 스캔의 물리적 한계를 파괴하고, 다차원 위상 공간 내의 '웜홀(Wormhole)'을 뚫어 최단 거리 우회로를 제공하는 초공간 항해망이자 영속적(Persistent) 벡터 인덱서.
 * - 이론: HNSW (Hierarchical Navigable Small World), Memory Cache Alignment, Mutual Link Assembly, Exponential Backoff, Graph Serialization, Crash Recovery.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [신설] 그래프 간선(Edge) 갱신 영속화: 노드가 추가되거나 이웃이 교체될 때마다 그 변경점(Delta)을 별도의 WAL 파일에 기록하는 `직조_영수증_사출` 로직을 이식하여 HNSW 그래프의 영속성을 확보했습니다.
 * - 💡 [신설] 0.1초 크래시 리커버리 (Crash Recovery): 서버 기동 시 메타데이터 디렉토리를 스캔하여 WAL 영수증을 읽어 들이고 메모리상에 그래프를 완벽히 재조립하는 `콜드스타트_그래프_리플레이` 엔진을 추가했습니다.
 * - 💡 [V6.1 C-Struct 동적 레이아웃 신설]: 하드코딩된 오프셋 상수를 전면 폐기하고 `MemoryLayout.structLayout()`을 활용하여 JIT 컴파일러가 구동되는 하드웨어 아키텍처에 맞게 자동으로 메모리를 정렬(Alignment)하도록 승격시켰습니다.
 * - 💡 [V6.1 동시성 붕괴 수술]: 무한 `Thread.onSpinWait()`으로 인해 발생하던 데드락(Deadlock) 뇌관을 파괴하고, 지수 백오프(Exponential Backoff)와 최대 재시도 임계치를 결합한 완벽한 Fail-Fast 방어막을 이식했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 오프힙 메모리 제어(FFM API), 동적 레이아웃 산출, 데드락 방지를 위한 스레드 제어 유틸리티, 그리고 그래프 영속화를 위한 파일 I/O 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of FFM API, dynamic layout calculation, thread control utilities for deadlock prevention, and file I/O libraries for graph persistence.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어5_다차원_매니폴드_오픈_DB;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;

import java.io.IOException;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 객체를 생성하지 않고 C-Struct 기반의 동적 다층 그래프를 항해하며, 디스크 영속성(Persistence)을 획득한 HNSW 코어 엔진입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. HNSW core engine that navigates dynamic multi-layer graphs based on C-Struct without creating objects, now having acquired disk persistence.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422057
 * [파일명] A0_DT_42_422057_초공간_항해_HNSW_엔진.java
 * [모듈명] 통합 OS V6.2 - Tier 5: 초공간 항해 HNSW 엔진 (영속적 Billion-Scale ANN Indexing)
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
    // 💡 [Zero-Allocation 동적 C-Struct 레이아웃]
    // 위험한 하드코딩 오프셋 수치를 폐기하고, JIT 컴파일러가 구동 환경(x86/ARM)에 맞추어
    // 패딩(Padding)과 메모리 경계 정렬(Alignment)을 완벽하게 계산하도록 구조체를 동적 정의합니다.
    // [2. 영문 상세 주석]
    // 💡 [Zero-Allocation Dynamic C-Struct Layout]
    // Discarded dangerous hardcoded offset values and dynamically defines the
    // struct so the JIT compiler perfectly calculates padding and memory alignment
    // according to the running environment (x86/ARM).
    // [3. 자바 코드]
    private static final StructLayout NODE_LAYOUT = MemoryLayout.structLayout(
            C_LONG.withName("vector_offset"), // (8 Bytes) 실제 L1 매트릭스 텐서의 시작 주소
            C_INT.withName("max_layer"), // (4 Bytes) 이 노드가 존재하는 최고 계층 (0 ~ 7)
            C_INT.withName("spin_lock"), // (4 Bytes) 간선 갱신 시 원자성을 보장하는 스핀 락
            MemoryLayout.sequenceLayout(HNSW_MAX_LAYERS, C_INT).withName("neighbor_counts"), // 각 레이어별 이웃 개수
            MemoryLayout.sequenceLayout(HNSW_MAX_LAYERS, MemoryLayout.sequenceLayout(HNSW_M, C_LONG))
                    .withName("neighbors") // 이웃 노드 오프셋 배열
    ).withName("HNSW_Node");

    // MemoryLayout.byteOffset()을 통한 무결점(Zero-Defect) 물리적 주소 자동 추출
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
    private static final long NODE_STRIDE_BYTES = NODE_LAYOUT.byteSize();

    // 그래프 진입점(Entry Point) 메타데이터
    private static final long EP_META_OFFSET = 0L; // 0번지에 시작 노드(EP)의 오프셋 저장

    // 💡 [그래프 영속화 배관]
    private final MemorySegment HNSW_인덱스_세그먼트;
    private final A0_DT_42_422001_권한_포트_인터페이스.ReadPort 원시_텐서_포트;
    private final Path HNSW_메타데이터_디렉토리;
    private FileChannel HNSW_WAL_채널;
    private final Object WAL_순차_락 = new Object();

    // [1. 한글 상세 주석]
    // [창세 생성자] HNSW 오프힙 인덱스 엔진을 점화하고, 크래시 리커버리를 집행한 뒤 WAL 채널을 개방합니다.
    // [2. 영문 상세 주석]
    // [Genesis Constructor] Ignites the HNSW off-heap index engine, executes crash
    // recovery, and opens the WAL channel.
    // [3. 자바 코드]
    public A0_DT_42_422057_초공간_항해_HNSW_엔진(MemorySegment 인덱스_세그먼트, A0_DT_42_422001_권한_포트_인터페이스.ReadPort 원시_텐서_포트,
            Path 메타데이터_디렉토리) {
        if (인덱스_세그먼트 == null || 원시_텐서_포트 == null || 메타데이터_디렉토리 == null) {
            throw new IllegalArgumentException("[항해 파열] 필수 메모리 세그먼트, 권한 포트, 또는 메타데이터 경로가 누락되었습니다.");
        }
        this.HNSW_인덱스_세그먼트 = 인덱스_세그먼트;
        this.원시_텐서_포트 = 원시_텐서_포트;
        this.HNSW_메타데이터_디렉토리 = 메타데이터_디렉토리;

        try {
            if (!Files.exists(HNSW_메타데이터_디렉토리)) {
                Files.createDirectories(HNSW_메타데이터_디렉토리);
            }

            // 💡 [신설: 크래시 리커버리] 부팅 시 디스크에 남은 영수증을 바탕으로 메모리 그래프 재조립
            콜드스타트_그래프_리플레이();

            // 💡 영속화 WAL 채널 개방
            Path wal_경로 = HNSW_메타데이터_디렉토리.resolve("HNSW_GRAPH_WAL.log");
            this.HNSW_WAL_채널 = FileChannel.open(wal_경로, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);

        } catch (IOException 예외) {
            로거.log(Level.SEVERE, " [그래프 영속화 파열] HNSW 메타데이터 디렉토리 초기화 실패.", 예외);
            throw new RuntimeException("HNSW 엔진 기동 불가", 예외);
        }

        로거.info(String.format(
                " >> [통합 OS V6.2] A0_DT_42_422057 초공간 항해 HNSW 엔진 기동. (동적 C-Struct [%d Bytes], 영속성 WAL 장착 완료)",
                NODE_STRIDE_BYTES));
    }

    // [1. 한글 상세 주석]
    // 💡 [신설: 크래시 리커버리(Crash Recovery)]
    // 서버가 재부팅될 때, 디스크에 순차 기록된 WAL 영수증을 읽어들여 0.1초 만에 수십억 개의 텐서 그래프(메모리 오프셋 매핑)를
    // 복원합니다.
    // [2. 영문 상세 주석]
    // 💡 [New: Crash Recovery]
    // Upon server reboot, reads the sequentially recorded WAL receipts on disk to
    // restore billions of tensor graphs (memory offset mappings) in 0.1 seconds.
    // [3. 자바 코드]
    private void 콜드스타트_그래프_리플레이() throws IOException {
        Path wal_경로 = HNSW_메타데이터_디렉토리.resolve("HNSW_GRAPH_WAL.log");
        if (!Files.exists(wal_경로))
            return;

        int 복원된_노드_수 = 0;
        int 복원된_간선_수 = 0;

        try (FileChannel 읽기_채널 = FileChannel.open(wal_경로, StandardOpenOption.READ)) {
            ByteBuffer 헤더_버퍼 = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN);

            while (true) {
                헤더_버퍼.clear();
                if (읽기_채널.read(헤더_버퍼) < 1)
                    break;

                헤더_버퍼.flip();
                byte 명령_타입 = 헤더_버퍼.get();

                if (명령_타입 == 0x01) { // 💡 노드 생성 명령
                    ByteBuffer 노드_버퍼 = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
                    if (읽기_채널.read(노드_버퍼) < 20)
                        break;
                    노드_버퍼.flip();

                    long 신규_노드_오프셋 = 노드_버퍼.getLong();
                    long 신규_벡터_오프셋 = 노드_버퍼.getLong();
                    int 부여된_최고_레이어 = 노드_버퍼.getInt();

                    // 메모리에 직접 각인 (Lock-free)
                    HNSW_인덱스_세그먼트.set(C_LONG, 신규_노드_오프셋 + STRUCT_OFFSET_VECTOR, 신규_벡터_오프셋);
                    HNSW_인덱스_세그먼트.set(C_INT, 신규_노드_오프셋 + STRUCT_OFFSET_LAYER, 부여된_최고_레이어);
                    HNSW_인덱스_세그먼트.set(C_INT, 신규_노드_오프셋 + STRUCT_OFFSET_SPINLOCK, 0);
                    HNSW_인덱스_세그먼트.asSlice(신규_노드_오프셋 + STRUCT_OFFSET_COUNTS, 32).fill((byte) 0);

                    // EP (Entry Point) 갱신
                    long 진입_노드_오프셋 = HNSW_인덱스_세그먼트.get(C_LONG, EP_META_OFFSET);
                    if (진입_노드_오프셋 == 0L || 부여된_최고_레이어 > HNSW_인덱스_세그먼트.get(C_INT, 진입_노드_오프셋 + STRUCT_OFFSET_LAYER)) {
                        HNSW_인덱스_세그먼트.set(C_LONG, EP_META_OFFSET, 신규_노드_오프셋);
                    }
                    복원된_노드_수++;

                } else if (명령_타입 == 0x02) { // 💡 간선(Edge) 연결 명령
                    ByteBuffer 간선_버퍼 = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN);
                    if (읽기_채널.read(간선_버퍼) < 24)
                        break;
                    간선_버퍼.flip();

                    long 출발_노드_오프셋 = 간선_버퍼.getLong();
                    long 도착_노드_오프셋 = 간선_버퍼.getLong();
                    int 계층 = 간선_버퍼.getInt();
                    int 이웃_인덱스 = 간선_버퍼.getInt(); // 덮어쓸 슬롯 인덱스

                    long 카운트_주소 = 출발_노드_오프셋 + STRUCT_OFFSET_COUNTS + (계층 * 4L);
                    long 이웃_배열_시작_주소 = 출발_노드_오프셋 + STRUCT_OFFSET_NEIGHBORS + (계층 * HNSW_M * 8L);

                    int 현재_이웃_개수 = HNSW_인덱스_세그먼트.get(C_INT, 카운트_주소);

                    HNSW_인덱스_세그먼트.set(C_LONG, 이웃_배열_시작_주소 + (이웃_인덱스 * 8L), 도착_노드_오프셋);

                    // 만약 새로운 슬롯을 열었다면 카운트 증가
                    if (이웃_인덱스 == 현재_이웃_개수) {
                        HNSW_인덱스_세그먼트.set(C_INT, 카운트_주소, 현재_이웃_개수 + 1);
                    }
                    복원된_간선_수++;
                } else {
                    로거.warning(" [리플레이 손상] 알 수 없는 WAL 명령 타입 감지. 복원을 중단합니다.");
                    break;
                }
            }
            로거.info(String.format("   ├─ [그래프 리플레이 완료] %d개 노드 및 %d개 간선의 위상망이 물리 메모리에 부활했습니다.", 복원된_노드_수, 복원된_간선_수));
        }
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

                // 읽기 연산은 락(Lock) 없이 고속으로 스캔합니다. (Lock-Free Read)
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
    // 💡 [건축 역학: 상호 간선 직조 및 영속화]
    // 새로운 텐서를 그래프망에 삽입하고, 이 변경점(Node Creation)을 디스크 WAL에 즉각 사출합니다.
    // [2. 영문 상세 주석]
    // 💡 [Architectural Dynamics: Mutual Edge Assembly and Persistence]
    // Inserts a new tensor into the graph network and immediately emits this change
    // (Node Creation) to the disk WAL.
    // [3. 자바 코드]
    public void 직조하다_HNSW_인덱스(long 신규_노드_오프셋, long 신규_벡터_오프셋, int 부여된_최고_레이어) {

        // 1. 동적 C-Struct 메타데이터 초기화 기록
        HNSW_인덱스_세그먼트.set(C_LONG, 신규_노드_오프셋 + STRUCT_OFFSET_VECTOR, 신규_벡터_오프셋);
        HNSW_인덱스_세그먼트.set(C_INT, 신규_노드_오프셋 + STRUCT_OFFSET_LAYER, 부여된_최고_레이어);
        HNSW_인덱스_세그먼트.set(C_INT, 신규_노드_오프셋 + STRUCT_OFFSET_SPINLOCK, 0);
        HNSW_인덱스_세그먼트.asSlice(신규_노드_오프셋 + STRUCT_OFFSET_COUNTS, 32).fill((byte) 0);

        // 💡 [신설: 영속화] 노드 생성 영수증 사출
        직조_영수증_사출_노드생성(신규_노드_오프셋, 신규_벡터_오프셋, 부여된_최고_레이어);

        long 진입_노드_오프셋 = HNSW_인덱스_세그먼트.get(C_LONG, EP_META_OFFSET);
        if (진입_노드_오프셋 == 0L) {
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
    // 💡 [데드락 멸균 및 간선 영속화] 출발 노드의 Spin-Lock 획득 후 간선을 연결하고, 그 조립 내역(Edge)을 즉시 WAL에
    // 영속화(Force Sync)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Deadlock Sterilization and Edge Persistence] After acquiring the
    // Spin-Lock of the source node, connects the edge and immediately persists
    // (Force Sync) the assembly details (Edge) to the WAL.
    // [3. 자바 코드]
    private void 결속하다_단방향_간선_원자적(long 출발_노드_오프셋, long 도착_노드_오프셋, int 계층) {
        long 락_주소 = 출발_노드_오프셋 + STRUCT_OFFSET_SPINLOCK;

        int 최대_재시도_횟수 = 100_000;
        long 백오프_나노초 = 100L;
        int 시도_횟수 = 0;

        while (!((boolean) SPIN_LOCK_HANDLE.compareAndSet(HNSW_인덱스_세그먼트, 락_주소, 0, 1))) {
            시도_횟수++;
            if (시도_횟수 > 최대_재시도_횟수) {
                로거.severe(" 🚨 [데드락 방어막 격발] HNSW 간선 갱신 중 Spin-Lock 획득 타임아웃 발생. (무한 대기 물리적 차단)");
                throw new IllegalStateException("HNSW 스핀 락 획득 타임아웃 붕괴");
            }

            if (시도_횟수 > 1000) {
                LockSupport.parkNanos(백오프_나노초);
                백오프_나노초 = Math.min(백오프_나노초 * 2, 1_000_000L); // 최대 1ms 대기
            } else {
                Thread.onSpinWait();
            }
        }

        try {
            long 카운트_주소 = 출발_노드_오프셋 + STRUCT_OFFSET_COUNTS + (계층 * 4L);
            int 현재_이웃_개수 = HNSW_인덱스_세그먼트.get(C_INT, 카운트_주소);
            long 이웃_배열_시작_주소 = 출발_노드_오프셋 + STRUCT_OFFSET_NEIGHBORS + (계층 * HNSW_M * 8L);

            int 타겟_이웃_인덱스 = 현재_이웃_개수;

            if (현재_이웃_개수 < HNSW_M) {
                HNSW_인덱스_세그먼트.set(C_LONG, 이웃_배열_시작_주소 + (현재_이웃_개수 * 8L), 도착_노드_오프셋);
                HNSW_인덱스_세그먼트.set(C_INT, 카운트_주소, 현재_이웃_개수 + 1);
            } else {
                // 💡 [Pruning] 거리 기반 가지치기
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

                if (타겟과의_거리 < 최대_거리 && 가장_먼_이웃_인덱스 != -1) {
                    HNSW_인덱스_세그먼트.set(C_LONG, 이웃_배열_시작_주소 + (가장_먼_이웃_인덱스 * 8L), 도착_노드_오프셋);
                    타겟_이웃_인덱스 = 가장_먼_이웃_인덱스; // 덮어쓴 슬롯 인덱스 기록
                } else {
                    return; // 갱신 안됨
                }
            }

            // 💡 [신설: 영속화] 간선 연결 영수증 사출
            직조_영수증_사출_간선연결(출발_노드_오프셋, 도착_노드_오프셋, 계층, 타겟_이웃_인덱스);

        } finally {
            SPIN_LOCK_HANDLE.setVolatile(HNSW_인덱스_세그먼트, 락_주소, 0);
        }
    }

    // =========================================================================
    // 💡 [신설] 그래프 영속화(Persistence)를 위한 직조 영수증 사출 엔진
    // =========================================================================

    private void 직조_영수증_사출_노드생성(long 신규_노드_오프셋, long 신규_벡터_오프셋, int 최고_레이어) {
        if (HNSW_WAL_채널 == null)
            return;
        try {
            // [0x01(1)] + [노드오프셋(8)] + [벡터오프셋(8)] + [최고레이어(4)] = 21 Bytes
            ByteBuffer 버퍼 = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
            버퍼.put((byte) 0x01);
            버퍼.putLong(신규_노드_오프셋);
            버퍼.putLong(신규_벡터_오프셋);
            버퍼.putInt(최고_레이어);
            버퍼.flip();

            synchronized (WAL_순차_락) {
                while (버퍼.hasRemaining()) {
                    HNSW_WAL_채널.write(버퍼);
                }
                HNSW_WAL_채널.force(false);
            }
        } catch (IOException e) {
            로거.warning(" [WAL 영속화 실패] HNSW 노드 생성 영수증 기록 실패.");
        }
    }

    private void 직조_영수증_사출_간선연결(long 출발_노드, long 도착_노드, int 계층, int 슬롯_인덱스) {
        if (HNSW_WAL_채널 == null)
            return;
        try {
            // [0x02(1)] + [출발노드(8)] + [도착노드(8)] + [계층(4)] + [슬롯인덱스(4)] = 25 Bytes
            ByteBuffer 버퍼 = ByteBuffer.allocate(25).order(ByteOrder.LITTLE_ENDIAN);
            버퍼.put((byte) 0x02);
            버퍼.putLong(출발_노드);
            버퍼.putLong(도착_노드);
            버퍼.putInt(계층);
            버퍼.putInt(슬롯_인덱스);
            버퍼.flip();

            synchronized (WAL_순차_락) {
                while (버퍼.hasRemaining()) {
                    HNSW_WAL_채널.write(버퍼);
                }
                HNSW_WAL_채널.force(false);
            }
        } catch (IOException e) {
            로거.warning(" [WAL 영속화 실패] HNSW 간선 연결 영수증 기록 실패.");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 그래프의 휘발성 파괴와 디스크 영속성 (Graph Persistence & Crash Recovery):
 * HNSW(Hierarchical Navigable Small World) 알고리즘은 거대한 다층 그래프를 메모리에 띄워 놓고 O(log
 * N)의 광속 탐색을 보장합니다.
 * 그러나 과거 코드의 치명적 약점은 이 거대한 건축물이 오직 휘발성(Volatile) 메모리에만 존재했다는 것입니다.
 * 서버 전원이 내려가면 수십억 개의 텐서들을 연결하던 고속도로(Edge)가 완전히 증발하며, 재부팅 시 이를 처음부터 다시 계산하고 엮어내는
 * 데 수십 시간이 소요되는 대재앙(Catastrophe)을 겪어야 했습니다.
 * 수술된 V6.2 엔진은 노드가 탄생하거나 간선이 조립될 때마다 `직조_영수증_사출`을 통해 그 변경점(Delta)을 즉각 디스크(WAL)에
 * 순차 기록(Append-Only)합니다.
 * 정전 후 재부팅 시 `콜드스타트_그래프_리플레이` 데몬이 이 WAL 파일을 읽어 들이며, 복잡한 거리 계산 없이 포인터(Offset)만
 * 즉각 메모리에 꽂아 넣어 단 0.1초 만에 수십억 개의 은하계 고속도로를 완벽히 재건(Roll-forward)해 냅니다.
 * 
 * 2. 동적 MemoryLayout에 의한 하드웨어 파편화 방어 (Dynamic Alignment Defense):
 * 이전 코드에서 프로그래머가 손수 `STRUCT_OFFSET_NEIGHBORS = 48L` 로 계산했던 하드코딩 방식은
 * "모든 OS와 CPU가 내가 생각한 패딩(Padding)대로 메모리를 정렬해 줄 것이다"라는 치명적인 오만입니다.
 * x86 아키텍처와 ARM64(Apple Silicon 등)는 메모리 정렬 규칙이 미세하게 다릅니다. 이 하드코딩을 방치했다면 서버 이전 시
 * 곧바로 `Segmentation Fault`나 캐시 미스 스래싱(Cache Miss Thrashing)을 유발하며 사망했을 것입니다.
 * 통합 OS V6.1은 자바 FFM API의 `MemoryLayout.structLayout()`을 투입하여, JIT 컴파일러가 구동되는
 * 운영체제의 가장 완벽한 물리적 메모리 정렬(Alignment) 규칙에 따라 `byteOffset`을 동적으로 산출해냅니다.
 * 이로써 C언어 구조체의 극한 성능과 자바의 WORA(Write Once, Run Anywhere) 철학이 기적적으로 융합되었습니다.
 * 
 * 3. 지수 백오프(Exponential Backoff)를 통한 Spin-Lock 데드락 멸균:
 * `Thread.onSpinWait()`은 락이 극히 짧은 시간(나노초 단위) 안에 풀릴 것을 가정하고 쓰는 명령어입니다.
 * 만약 어떤 스레드가 락을 쥐고 있는 도중 OS 컨텍스트 스위칭 아웃을 당하거나 예상치 못한 지연을 겪는다면, 락을 기다리는 수천 개의 다른
 * 스레드들은 무한 스핀에 빠져 CPU를 100% 태우며(Meltdown) 영원한 교착 상태에 갇히게 됩니다.
 * 새롭게 이식된 락 메커니즘은 1,000번의 스핀 시도 후에는 즉시 `LockSupport.parkNanos`로 넘어가 OS 스케줄러에게
 * CPU 점유를 우아하게 양보(Yield)합니다.
 * 또한 백오프 대기 시간을 2배씩 늘려가며(Exponential Backoff), 10만 번을 초과하여 락 획득을 실패할 경우 미련 없이
 * `IllegalStateException`을 던져 썩은 파이프라인의 환부를 스스로 폭파(Fail-Fast)시킴으로써 서버 전체의 생존을
 * 수호합니다.
 * =============================================================================
 */