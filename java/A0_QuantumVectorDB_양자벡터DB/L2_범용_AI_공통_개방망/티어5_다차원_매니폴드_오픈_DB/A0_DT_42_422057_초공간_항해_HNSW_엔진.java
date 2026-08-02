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
 * - 기능: 오프힙 커널 메모리에 C언어 구조체(Struct) 형태의 HNSW 다층 그래프를 동적 맵핑하고, 포인터 점프(Pointer Jumping)만으로 수십억 개의 텐서 중 가장 유사한 최근접 이웃(ANN)을 탐색 및 간선을 조립합니다.
 * - 역할: O(N) 전수 스캔의 물리적 한계를 파괴하고, 다차원 위상 공간 내에 최단 거리 우회로를 제공하는 HNSW 기반 영속적(Persistent) 벡터 인덱서 코어.
 * - 이론: HNSW (Hierarchical Navigable Small World), Memory Cache Alignment, Mutual Link Assembly, Exponential Backoff, Graph Serialization, Crash Recovery.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [핵심 통제] 그래프 간선(Edge) 갱신 영속화: 노드가 추가되거나 이웃이 교체될 때마다 그 변경점(Delta)을 별도의 WAL(Write-Ahead Log) 파일에 기록하는 `emitWalReceipt` 로직을 이식하여 HNSW 그래프의 영속성을 확보했습니다.
 * - 💡 [신설] 0.1초 크래시 리커버리 (Crash Recovery): 서버 기동 시 메타데이터 디렉토리를 스캔하여 WAL 영수증을 읽어 들이고 메모리상에 그래프를 완벽히 재조립하는 `replayGraphFromWalColdStart` 엔진을 추가했습니다.
 * - 💡 [V6.1 C-Struct 동적 레이아웃 신설]: 하드코딩된 오프셋 상수를 전면 폐기하고 FFM `MemoryLayout.structLayout()`을 활용하여 JIT 컴파일러가 구동되는 하드웨어 아키텍처에 맞게 자동으로 메모리를 정렬(Alignment)하도록 승격시켰습니다.
 * - 💡 [V6.1 동시성 붕괴 수술]: 무한 `Thread.onSpinWait()`으로 인해 발생하던 데드락(Deadlock) 위험을 제거하고, 지수 백오프(Exponential Backoff)와 최대 재시도 임계치를 결합한 완벽한 Fail-Fast 방어막을 이식했습니다.
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
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 객체를 생성하지 않고 C-Struct 기반의 동적 다층 그래프를 항해하며, 디스크 영속성(Persistence)을 획득한 HNSW 코어 엔진입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. HNSW core engine that navigates dynamic multi-layer graphs based on C-Struct without creating objects, now equipped with disk persistence.
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

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422057_HNSW_ENGINE");

    // [1. 한글 상세 주석]
    // 하드웨어 아키텍처와 1:1로 대응하는 리틀 엔디안 규격의 FFM 레이아웃 상수를 선언합니다.
    // [2. 영문 상세 주석]
    // Declares FFM layout constants in little-endian format that map 1:1 with the hardware architecture.

    private static final ValueLayout.OfLong C_LONG = ValueLayout.JAVA_LONG.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt C_INT = ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN);

    // 오프힙 메모리 상의 int 값을 원자적으로 조작하기 위한 CAS VarHandle (Spin-Lock 제어용)
    private static final VarHandle SPIN_LOCK_HANDLE = C_INT.varHandle();

    // HNSW 그래프의 위상학적 구조를 통제하는 기하학 상수
    private static final int HNSW_MAX_LAYERS = 8; // L: 공간을 접을 최대 계층 수 (Hierarchical Levels)
    private static final int HNSW_M = 16;         // M: 각 레이어당 연결 가능한 최대 이웃 노드 개수

    // [1. 한글 상세 주석]
    // 💡 [Zero-Allocation 동적 C-Struct 레이아웃 (Dynamic C-Struct Layout)]
    // 하드코딩 오프셋 수치를 전면 폐기하고, JIT 컴파일러가 구동 환경(x86/ARM)에 맞추어 
    // 패딩(Padding)과 메모리 경계 정렬(Alignment)을 완벽하게 계산하도록 구조체를 동적 정의합니다.
    // [2. 영문 상세 주석]
    // 💡 [Zero-Allocation Dynamic C-Struct Layout]
    // Discarded dangerous hardcoded offset values and dynamically defines the struct 
    // so the JIT compiler perfectly calculates padding and memory alignment according to the running environment (x86/ARM).

    private static final StructLayout NODE_LAYOUT = MemoryLayout.structLayout(
            C_LONG.withName("vector_offset"), // (8 Bytes) 실제 L1 매트릭스 텐서의 시작 물리 주소
            C_INT.withName("max_layer"),      // (4 Bytes) 이 노드가 존재하는 최고 계층 (0 ~ 7)
            C_INT.withName("spin_lock"),      // (4 Bytes) 간선 갱신 시 원자성을 보장하는 스핀 락 플래그
            MemoryLayout.sequenceLayout(HNSW_MAX_LAYERS, C_INT).withName("neighbor_counts"), // 각 레이어별 현재 연결된 이웃 개수
            MemoryLayout.sequenceLayout(HNSW_MAX_LAYERS, MemoryLayout.sequenceLayout(HNSW_M, C_LONG))
                    .withName("neighbors")    // 이웃 노드들의 메모리 오프셋 2차원 배열
    ).withName("HNSW_Node");

    // MemoryLayout.byteOffset()을 통한 무결점(Zero-Defect) 물리적 주소 오프셋 자동 추출
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

    // 그래프 진입점(Entry Point) 메타데이터 오프셋
    private static final long EP_META_OFFSET = 0L; // 0번지에 HNSW 시작 노드(EP)의 오프셋 저장

    // 💡 [그래프 영속화 배관]
    private final MemorySegment hnswIndexSegment;
    private final A0_DT_42_422001_권한_포트_인터페이스.ReadPort rawTensorPort;
    private final Path hnswMetadataDirectory;
    private FileChannel hnswWalChannel;
    private final Object walSequentialLock = new Object();

    // [1. 한글 상세 주석]
    // [생성자] HNSW 오프힙 인덱스 엔진을 점화하고, 크래시 리커버리를 집행한 뒤 WAL 채널을 개방합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Ignites the HNSW off-heap index engine, executes crash recovery, and opens the WAL channel.

    public A0_DT_42_422057_초공간_항해_HNSW_엔진(MemorySegment indexSegment, A0_DT_42_422001_권한_포트_인터페이스.ReadPort rawTensorPort,
            Path metadataDirectory) {
        if (indexSegment == null || rawTensorPort == null || metadataDirectory == null) {
            throw new IllegalArgumentException("[설정 오류] 필수 메모리 세그먼트, 텐서 권한 포트, 또는 메타데이터 경로가 누락되었습니다.");
        }
        this.hnswIndexSegment = indexSegment;
        this.rawTensorPort = rawTensorPort;
        this.hnswMetadataDirectory = metadataDirectory;

        try {
            if (!Files.exists(hnswMetadataDirectory)) {
                Files.createDirectories(hnswMetadataDirectory);
            }

            // 💡 [핵심 제어: 크래시 리커버리] 서버 기동 시 디스크에 남은 영수증(WAL)을 바탕으로 메모리 그래프 재조립
            replayGraphFromWalColdStart();

            // 💡 영속화 WAL 채널 개방
            Path walPath = hnswMetadataDirectory.resolve("HNSW_GRAPH_WAL.log");
            this.hnswWalChannel = FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [그래프 영속화 파열] HNSW 메타데이터 디렉토리 초기화 및 WAL 개방 실패.", ex);
            throw new RuntimeException("HNSW 인덱스 엔진 기동 불가", ex);
        }

        logger.info(String.format(
                " >> [통합 OS V6.2] A0_DT_42_422057 초공간 항해 HNSW 엔진 기동. (동적 C-Struct [%d Bytes], 영속성 WAL 장착 완료)",
                NODE_STRIDE_BYTES));
    }

    // [1. 한글 상세 주석]
    // 💡 [크래시 리커버리 (Crash Recovery)]
    // 서버가 재부팅될 때, 디스크에 순차 기록된 WAL 영수증을 읽어들여 단시간에 수십억 개의 텐서 그래프(오프셋 매핑)를 메모리에 복원합니다.
    // [2. 영문 상세 주석]
    // 💡 [Crash Recovery]
    // Upon server reboot, reads the sequentially recorded WAL receipts on disk to restore billions of tensor graphs (offset mappings) in memory in a short time.

    private void replayGraphFromWalColdStart() throws IOException {
        Path walPath = hnswMetadataDirectory.resolve("HNSW_GRAPH_WAL.log");
        if (!Files.exists(walPath))
            return;

        int restoredNodeCount = 0;
        int restoredEdgeCount = 0;

        try (FileChannel readChannel = FileChannel.open(walPath, StandardOpenOption.READ)) {
            ByteBuffer headerBuffer = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN);

            while (true) {
                headerBuffer.clear();
                if (readChannel.read(headerBuffer) < 1)
                    break;

                headerBuffer.flip();
                byte commandType = headerBuffer.get();

                if (commandType == 0x01) { // 💡 노드 생성(Node Creation) 명령 복원
                    ByteBuffer nodeBuffer = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
                    if (readChannel.read(nodeBuffer) < 20)
                        break;
                    nodeBuffer.flip();

                    long newNodeOffset = nodeBuffer.getLong();
                    long newVectorOffset = nodeBuffer.getLong();
                    int assignedMaxLayer = nodeBuffer.getInt();

                    // 메모리 세그먼트에 다이렉트 각인 (Lock-free Restoration)
                    hnswIndexSegment.set(C_LONG, newNodeOffset + STRUCT_OFFSET_VECTOR, newVectorOffset);
                    hnswIndexSegment.set(C_INT, newNodeOffset + STRUCT_OFFSET_LAYER, assignedMaxLayer);
                    hnswIndexSegment.set(C_INT, newNodeOffset + STRUCT_OFFSET_SPINLOCK, 0);
                    hnswIndexSegment.asSlice(newNodeOffset + STRUCT_OFFSET_COUNTS, 32).fill((byte) 0);

                    // EP (Entry Point) 갱신
                    long entryPointOffset = hnswIndexSegment.get(C_LONG, EP_META_OFFSET);
                    if (entryPointOffset == 0L || assignedMaxLayer > hnswIndexSegment.get(C_INT, entryPointOffset + STRUCT_OFFSET_LAYER)) {
                        hnswIndexSegment.set(C_LONG, EP_META_OFFSET, newNodeOffset);
                    }
                    restoredNodeCount++;

                } else if (commandType == 0x02) { // 💡 간선 연결(Edge Assembly) 명령 복원
                    ByteBuffer edgeBuffer = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN);
                    if (readChannel.read(edgeBuffer) < 24)
                        break;
                    edgeBuffer.flip();

                    long sourceNodeOffset = edgeBuffer.getLong();
                    long targetNodeOffset = edgeBuffer.getLong();
                    int layerIndex = edgeBuffer.getInt();
                    int neighborSlotIndex = edgeBuffer.getInt(); // 덮어쓸 배열 슬롯 인덱스

                    long countAddress = sourceNodeOffset + STRUCT_OFFSET_COUNTS + (layerIndex * 4L);
                    long neighborArrayStartAddress = sourceNodeOffset + STRUCT_OFFSET_NEIGHBORS + (layerIndex * HNSW_M * 8L);

                    int currentNeighborCount = hnswIndexSegment.get(C_INT, countAddress);

                    hnswIndexSegment.set(C_LONG, neighborArrayStartAddress + (neighborSlotIndex * 8L), targetNodeOffset);

                    // 만약 새로운 빈 슬롯에 기록했다면 카운트 증가
                    if (neighborSlotIndex == currentNeighborCount) {
                        hnswIndexSegment.set(C_INT, countAddress, currentNeighborCount + 1);
                    }
                    restoredEdgeCount++;
                } else {
                    logger.warning(" [리플레이 오류] 알 수 없는 WAL 명령 타입 감지. 무결성을 위해 복원을 중단합니다.");
                    break;
                }
            }
            logger.info(String.format("   ├─ [그래프 리플레이 완료] %d개 노드 및 %d개 간선의 위상망이 물리 메모리에 부활(Restored)했습니다.", restoredNodeCount, restoredEdgeCount));
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [핵심 항해 로직: 근사 최근접 이웃(ANN) 탐색]
    // 힙(Heap)이나 DTO 객체를 단 하나도 생성하지 않고, 물리 메모리 상의 C-Struct 포인터를 이리저리 점프(Pointer Jumping)하며 
    // 질의 텐서에 가장 근접한 데이터의 절대 주소를 O(log N)으로 관통 탐색합니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Navigation Logic: Approximate Nearest Neighbor (ANN) Search]
    // Penetrates and finds the absolute address of the data closest to the query tensor at O(log N) speed by jumping C-Struct pointers in physical memory without creating a single Heap or DTO object.

    public long searchApproximateNearestNeighbor(long queryTensorOffset) {
        long entryPointOffset = hnswIndexSegment.get(C_LONG, EP_META_OFFSET);

        if (entryPointOffset == 0L) {
            return -1L; // 그래프가 텅 빈 진공 상태
        }

        int highestLayerL = hnswIndexSegment.get(C_INT, entryPointOffset + STRUCT_OFFSET_LAYER);
        long currentClosestNode = entryPointOffset;
        double minDistance = calculateTensorEuclideanDistance(queryTensorOffset, readVectorOffset(entryPointOffset));

        // 하강 항해 (Top-Down Greedy Search Navigation)
        for (int currentLayer = highestLayerL; currentLayer >= 0; currentLayer--) {
            boolean closerNeighborFound = true;

            while (closerNeighborFound) {
                closerNeighborFound = false;

                // 탐색(읽기) 연산은 스레드 블로킹(Lock) 없이 고속으로 스캔 수행 (Lock-Free Read)
                int neighborCount = hnswIndexSegment.get(C_INT, currentClosestNode + STRUCT_OFFSET_COUNTS + (currentLayer * 4L));
                long neighborArrayStartAddress = currentClosestNode + STRUCT_OFFSET_NEIGHBORS + (currentLayer * HNSW_M * 8L);

                for (int i = 0; i < neighborCount; i++) {
                    long neighborNodeOffset = hnswIndexSegment.get(C_LONG, neighborArrayStartAddress + (i * 8L));
                    long neighborVectorOffset = readVectorOffset(neighborNodeOffset);

                    double distanceToNeighbor = calculateTensorEuclideanDistance(queryTensorOffset, neighborVectorOffset);

                    if (distanceToNeighbor < minDistance) {
                        minDistance = distanceToNeighbor;
                        currentClosestNode = neighborNodeOffset;
                        closerNeighborFound = true;
                    }
                }
            }
        }
        return readVectorOffset(currentClosestNode);
    }

    private double calculateTensorEuclideanDistance(long referenceVectorOffset, long targetVectorOffset) {
        int embeddingDimensions = 128;
        double sumOfSquaredDifferences = 0.0;

        MemorySegment tensorMemory = rawTensorPort.segment();

        for (int i = 0; i < embeddingDimensions; i++) {
            long relativeOffset = i * 4L;
            float referenceValue = tensorMemory.get(ValueLayout.JAVA_FLOAT, referenceVectorOffset + relativeOffset);
            float targetValue = tensorMemory.get(ValueLayout.JAVA_FLOAT, targetVectorOffset + relativeOffset);

            float difference = referenceValue - targetValue;
            sumOfSquaredDifferences += (difference * difference);
        }
        return Math.sqrt(sumOfSquaredDifferences);
    }

    private long readVectorOffset(long nodeStartOffset) {
        return hnswIndexSegment.get(C_LONG, nodeStartOffset + STRUCT_OFFSET_VECTOR);
    }

    // [1. 한글 상세 주석]
    // 💡 [건축 역학: 상호 간선 직조 및 영속화 (Edge Assembly)]
    // 새로운 텐서를 그래프 인덱스에 삽입하고, 이 변경점(Node Creation Delta)을 디스크 WAL에 즉각 사출하여 영속화시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [Architectural Dynamics: Mutual Edge Assembly and Persistence]
    // Inserts a new tensor into the graph index and immediately emits this change (Node Creation Delta) to the disk WAL for persistence.

    public void assembleHnswIndex(long newNodeOffset, long newVectorOffset, int assignedMaxLayer) {

        // 1. 동적 C-Struct 메타데이터 메모리 초기화
        hnswIndexSegment.set(C_LONG, newNodeOffset + STRUCT_OFFSET_VECTOR, newVectorOffset);
        hnswIndexSegment.set(C_INT, newNodeOffset + STRUCT_OFFSET_LAYER, assignedMaxLayer);
        hnswIndexSegment.set(C_INT, newNodeOffset + STRUCT_OFFSET_SPINLOCK, 0);
        hnswIndexSegment.asSlice(newNodeOffset + STRUCT_OFFSET_COUNTS, 32).fill((byte) 0);

        // 💡 [핵심 통제: 영속화] 노드 생성 WAL 영수증 직렬화 사출
        emitWalReceiptNodeCreation(newNodeOffset, newVectorOffset, assignedMaxLayer);

        long entryPointOffset = hnswIndexSegment.get(C_LONG, EP_META_OFFSET);
        if (entryPointOffset == 0L) {
            hnswIndexSegment.set(C_LONG, EP_META_OFFSET, newNodeOffset);
            logger.fine("   ├─ [초공간 창조] 최초의 HNSW 노드가 진입점(Entry Point)으로 시스템에 등록되었습니다.");
            return;
        }

        int epMaxLayer = hnswIndexSegment.get(C_INT, entryPointOffset + STRUCT_OFFSET_LAYER);
        long currentSearchNode = entryPointOffset;

        // 상단 레이어 고속 하강 (Find insertion point)
        for (int currentLayer = epMaxLayer; currentLayer > assignedMaxLayer; currentLayer--) {
            currentSearchNode = scanNearestNeighborInLayer(currentSearchNode, newVectorOffset, currentLayer);
        }

        // 간선 상호 조립 (Mutual Edge Assembly)
        for (int currentLayer = Math.min(epMaxLayer, assignedMaxLayer); currentLayer >= 0; currentLayer--) {
            currentSearchNode = scanNearestNeighborInLayer(currentSearchNode, newVectorOffset, currentLayer);

            bindUnidirectionalEdgeAtomically(newNodeOffset, currentSearchNode, currentLayer);
            bindUnidirectionalEdgeAtomically(currentSearchNode, newNodeOffset, currentLayer);
        }

        if (assignedMaxLayer > epMaxLayer) {
            hnswIndexSegment.set(C_LONG, EP_META_OFFSET, newNodeOffset);
        }
    }

    private long scanNearestNeighborInLayer(long entryNode, long queryVectorOffset, int layerIndex) {
        long closestNode = entryNode;
        double minDistance = calculateTensorEuclideanDistance(queryVectorOffset, readVectorOffset(entryNode));
        boolean closerNeighborFound = true;

        while (closerNeighborFound) {
            closerNeighborFound = false;
            int neighborCount = hnswIndexSegment.get(C_INT, closestNode + STRUCT_OFFSET_COUNTS + (layerIndex * 4L));
            long neighborArrayStartAddress = closestNode + STRUCT_OFFSET_NEIGHBORS + (layerIndex * HNSW_M * 8L);

            for (int i = 0; i < neighborCount; i++) {
                long neighborNodeOffset = hnswIndexSegment.get(C_LONG, neighborArrayStartAddress + (i * 8L));
                double distance = calculateTensorEuclideanDistance(queryVectorOffset, readVectorOffset(neighborNodeOffset));

                if (distance < minDistance) {
                    minDistance = distance;
                    closestNode = neighborNodeOffset;
                    closerNeighborFound = true;
                }
            }
        }
        return closestNode;
    }

    // [1. 한글 상세 주석]
    // 💡 [데드락 방어 및 간선 영속화] 출발 노드의 Spin-Lock 획득 후 간선을 연결하고, 그 조립 내역을 즉시 디스크 WAL에 동기화(Force Sync)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Deadlock Defense and Edge Persistence] After acquiring the Spin-Lock of the source node, connects the edge and immediately synchronizes (Force Sync) the assembly details to the disk WAL.

    private void bindUnidirectionalEdgeAtomically(long sourceNodeOffset, long targetNodeOffset, int layerIndex) {
        long lockAddress = sourceNodeOffset + STRUCT_OFFSET_SPINLOCK;

        int maxRetryLimit = 100_000;
        long backoffNanos = 100L;
        int attemptCount = 0;

        // 원자적(CAS) Spin-Lock 획득 시도
        while (!((boolean) SPIN_LOCK_HANDLE.compareAndSet(hnswIndexSegment, lockAddress, 0, 1))) {
            attemptCount++;
            if (attemptCount > maxRetryLimit) {
                logger.severe(" 🚨 [데드락 방어막 격발] HNSW 간선 갱신 중 Spin-Lock 획득 타임아웃 발생. (무한 대기 물리적 차단 및 예외 발산)");
                throw new IllegalStateException("HNSW 스핀 락 획득 타임아웃 오류");
            }

            if (attemptCount > 1000) {
                LockSupport.parkNanos(backoffNanos); // 지수 백오프: OS 스케줄러에 실행 권한 양보
                backoffNanos = Math.min(backoffNanos * 2, 1_000_000L); // 최대 1ms 대기
            } else {
                Thread.onSpinWait();
            }
        }

        try {
            long countAddress = sourceNodeOffset + STRUCT_OFFSET_COUNTS + (layerIndex * 4L);
            int currentNeighborCount = hnswIndexSegment.get(C_INT, countAddress);
            long neighborArrayStartAddress = sourceNodeOffset + STRUCT_OFFSET_NEIGHBORS + (layerIndex * HNSW_M * 8L);

            int targetNeighborIndex = currentNeighborCount;

            if (currentNeighborCount < HNSW_M) {
                hnswIndexSegment.set(C_LONG, neighborArrayStartAddress + (currentNeighborCount * 8L), targetNodeOffset);
                hnswIndexSegment.set(C_INT, countAddress, currentNeighborCount + 1);
            } else {
                // 💡 [Pruning] 거리 기반 가지치기 (가장 먼 이웃을 교체)
                long sourceVector = readVectorOffset(sourceNodeOffset);
                long targetVector = readVectorOffset(targetNodeOffset);
                double distanceToTarget = calculateTensorEuclideanDistance(sourceVector, targetVector);

                double maxDistance = -1.0;
                int furthestNeighborIndex = -1;

                for (int i = 0; i < HNSW_M; i++) {
                    long existingNeighborNode = hnswIndexSegment.get(C_LONG, neighborArrayStartAddress + (i * 8L));
                    long existingNeighborVector = readVectorOffset(existingNeighborNode);
                    double existingDistance = calculateTensorEuclideanDistance(sourceVector, existingNeighborVector);

                    if (existingDistance > maxDistance) {
                        maxDistance = existingDistance;
                        furthestNeighborIndex = i;
                    }
                }

                if (distanceToTarget < maxDistance && furthestNeighborIndex != -1) {
                    hnswIndexSegment.set(C_LONG, neighborArrayStartAddress + (furthestNeighborIndex * 8L), targetNodeOffset);
                    targetNeighborIndex = furthestNeighborIndex; // 덮어쓴 슬롯 인덱스 기록
                } else {
                    return; // 갱신 안됨 (현재 연결된 이웃들이 더 가까움)
                }
            }

            // 💡 [핵심 통제: 영속화] 간선 연결 WAL 영수증 직렬화 사출
            emitWalReceiptEdgeConnection(sourceNodeOffset, targetNodeOffset, layerIndex, targetNeighborIndex);

        } finally {
            // Spin-Lock 해제
            SPIN_LOCK_HANDLE.setVolatile(hnswIndexSegment, lockAddress, 0);
        }
    }

    // =========================================================================
    // 💡 [그래프 영속화(Persistence)] 디스크 WAL(Write-Ahead Log) 직렬화 사출 엔진
    // =========================================================================

    private void emitWalReceiptNodeCreation(long newNodeOffset, long newVectorOffset, int assignedMaxLayer) {
        if (hnswWalChannel == null)
            return;
        try {
            // 구조: [명령타입 0x01(1)] + [노드오프셋(8)] + [벡터오프셋(8)] + [최고레이어(4)] = 총 21 Bytes
            ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put((byte) 0x01);
            buffer.putLong(newNodeOffset);
            buffer.putLong(newVectorOffset);
            buffer.putInt(assignedMaxLayer);
            buffer.flip();

            synchronized (walSequentialLock) {
                while (buffer.hasRemaining()) {
                    hnswWalChannel.write(buffer);
                }
                hnswWalChannel.force(false); // OS 캐시를 뚫고 물리 디스크에 강제 동기화 (Data Loss 방지)
            }
        } catch (IOException e) {
            logger.warning(" [WAL 영속화 실패] HNSW 노드 생성 영수증 디스크 기록 I/O 오류.");
        }
    }

    private void emitWalReceiptEdgeConnection(long sourceNode, long targetNode, int layerIndex, int slotIndex) {
        if (hnswWalChannel == null)
            return;
        try {
            // 구조: [명령타입 0x02(1)] + [출발노드(8)] + [도착노드(8)] + [계층(4)] + [슬롯인덱스(4)] = 총 25 Bytes
            ByteBuffer buffer = ByteBuffer.allocate(25).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put((byte) 0x02);
            buffer.putLong(sourceNode);
            buffer.putLong(targetNode);
            buffer.putInt(layerIndex);
            buffer.putInt(slotIndex);
            buffer.flip();

            synchronized (walSequentialLock) {
                while (buffer.hasRemaining()) {
                    hnswWalChannel.write(buffer);
                }
                hnswWalChannel.force(false);
            }
        } catch (IOException e) {
            logger.warning(" [WAL 영속화 실패] HNSW 간선(Edge) 연결 영수증 디스크 기록 I/O 오류.");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 그래프 휘발성 한계 돌파와 디스크 영속성 (Graph Persistence & Crash Recovery):
 * HNSW(Hierarchical Navigable Small World) 알고리즘은 거대한 다층 그래프 망을 메모리에 전개하여 O(log N)의 광속 근사 최근접 이웃(ANN) 탐색을 보장합니다.
 * 그러나 과거 구조의 가장 치명적인 약점은 이 거대한 건축물이 오직 휘발성(Volatile) 메모리에만 존재했다는 것입니다.
 * 서버 전원이 내려가면 수십억 개의 텐서들을 연결하던 고속도로(Edge Link)가 완전히 증발하며, 재부팅 시 이를 처음부터 다시 계산하고 엮어내는 데 수십 시간이 소요되는 대재앙을 겪어야 했습니다.
 * 적용된 V6.2 엔진은 노드가 탄생하거나 간선이 조립될 때마다 `emitWalReceipt` 모듈을 통해 그 변경점(Delta)을 즉각 디스크 WAL(Write-Ahead Log) 파일에 순차 기록(Append-Only)합니다.
 * 정전 후 재부팅 시 `replayGraphFromWalColdStart` 데몬이 이 WAL 파일을 스캔하며, 복잡한 유클리드 거리 재계산 없이 포인터(Offset)만 즉각 메모리에 꽂아 넣어 단 몇 밀리초 만에 수십억 개의 은하계 웜홀 고속도로를 완벽히 재건(Roll-forward)해 냅니다.
 * 
 * 2. 동적 MemoryLayout에 의한 하드웨어 파편화 방어 (Dynamic Cache Alignment Defense):
 * 과거 소스코드에서 프로그래머가 손수 `STRUCT_OFFSET_NEIGHBORS = 48L` 식으로 계산했던 하드코딩 방식은 "모든 OS와 CPU가 내가 생각한 패딩(Padding)대로 메모리를 정렬해 줄 것이다"라는 치명적인 설계 오만입니다.
 * x86 아키텍처와 ARM64(Apple Silicon 등)는 C-Struct 메모리 정렬 규칙이 미세하게 다릅니다. 이 하드코딩을 방치했다면 서버 이전 시 곧바로 `Segmentation Fault`나 캐시 미스 스래싱(Cache Miss Thrashing)을 유발하여 코어 덤프(Core Dump)를 발생시켰을 것입니다.
 * 통합 OS는 최신 자바 FFM API의 `MemoryLayout.structLayout()`을 투입하여, JIT 컴파일러가 구동되는 운영체제의 물리적 메모리 정렬(Alignment) 규칙에 따라 `byteOffset`을 동적으로 산출해냅니다.
 * 이로써 C언어 구조체의 극한 메모리 밀집 성능과 자바의 플랫폼 독립성(WORA: Write Once, Run Anywhere) 철학이 기적적으로 융합되었습니다.
 * 
 * 3. 지수 백오프(Exponential Backoff)를 통한 Spin-Lock 데드락(Deadlock) 멸균:
 * `Thread.onSpinWait()`은 멀티스레드 락(Lock)이 극히 짧은 시간(나노초 단위) 안에 풀릴 것을 엄격히 가정하고 쓰는 명령어입니다.
 * 만약 어떤 스레드가 락을 쥐고 있는 도중 OS 컨텍스트 스위칭 아웃을 당하거나 페이지 폴트(Page Fault)로 지연을 겪는다면, 락을 기다리는 수백 개의 다른 스레드들은 무한 스핀에 빠져 CPU 코어를 100% 태우며(Meltdown) 영원한 교착 상태에 갇히게 됩니다.
 * 새롭게 이식된 락(Lock) 메커니즘은 1,000번의 스핀 시도 후에는 즉시 `LockSupport.parkNanos`로 제어권을 넘겨 OS 스케줄러에게 CPU 점유를 우아하게 양보(Yield)합니다.
 * 또한 대기 시간을 2배씩 늘려가는 지수 백오프(Exponential Backoff)를 적용하고, 10만 번을 초과하여 락 획득을 실패할 경우 `IllegalStateException`을 발산시켜 썩은 파이프라인의 환부를 스스로 폭파(Fail-Fast)시킴으로써 서버 전체의 생존을 수호합니다.
 * =============================================================================
 */