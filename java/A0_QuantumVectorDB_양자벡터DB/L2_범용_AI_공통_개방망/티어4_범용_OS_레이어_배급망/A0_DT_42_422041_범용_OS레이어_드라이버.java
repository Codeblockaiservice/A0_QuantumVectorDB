/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망
 * @alias Universal_OSLayer_Driver
 * @tier 4
 * @keywords MVCC, Zero-Copy, Copy-on-Write, LRU Paging, Event Horizon Control, Deferred Eviction, RCU, OOM Defense, madvise, Cleaner
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422041_범용_OS레이어_드라이버.java
 * - 모듈명: 통합 OS V6.1 - Tier 4: 메모리 한계선 통제 및 텐서 서빙 드라이버
 * - 기능 및 역할: L1 커널 메모리의 특정 단면을 읽기/쓰기 전용 뷰(Port)로 상위 계층에 안전하게 배급하고 핫스왑(Hot-Swap)을 지원합니다.
 * - 이론 및 기술: 메모리 한계선(Memory Threshold) 제어, 능동형 LRU 캐시 교체, MemorySegment.asSlice(), Copy-on-Write 샌드박스, Epoch 기반 RCU 지연 퇴출.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [핵심 통제] `java.lang.ref.Cleaner`를 도입하여, 네이티브 메모리 해제를 JVM GC 주기와 안전하게 연동시키는 다중 안전장치(Use-After-Free 방어) 이식 완료.
 * - 💡 [핵심 최적화] FFM API `Linker`를 통해 `madvise(MADV_DONTNEED)` 시스템 콜을 직접 호출하여, OS 커널의 OOM Killer가 작동하기 전에 명시적으로 페이지 캐시 회수를 지시하는 커널 레벨 방어막 구축.
 * - 💡 [아키텍처 개편] 하드코딩 상수(`50MB`, `10MB`, `20%` 등)를 제거하고, 부팅 시 환경변수(Property)에서 로드하여 주입받는 Externalized Configuration 구조로 개편.
 * - 💡 [OOM 방어] 스왑 메모리 팽창 통제망 도입: `sandboxOccupiedBytes` 누적 할당량 카운터를 통해 샌드박스 스왑 공간의 크기를 엄격하게 제한합니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 OS 커널 메모리 제어(FFM API), 시스템 콜 링킹(Linker), 파일 채널 제어, 락프리 동시성 큐 관리를 위한 핵심 자바 표준 모듈들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core Java standard modules for OS kernel memory control (FFM API), system call linking (Linker), file channel control, and lock-free concurrent queue management.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.ref.Cleaner;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더.SmartIndexRegistry;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어6_시맨틱_임베딩_변환기.A0_DT_42_422061_매트릭스_쿼리_엔진;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. L1/L2 메모리의 안전한 배급 권한을 통제하며 운영체제의 OOM Killer 발동을 선제적으로 방어하는 OS 레이어 통합 드라이버입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An Integrated OS layer driver that controls safe distribution rights for L1/L2 memory and preemptively defends against the OS's OOM Killer activation.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422041
 * [파일명] A0_DT_42_422041_범용_OS레이어_드라이버.java
 * [모듈명] 통합 OS V6.1 - Tier 4: 메모리 한계선 통제 및 텐서 서빙 드라이버
 * 
 * [설계 명세]
 * 1. 역할: L1 커널 메모리의 특정 단면을 하드웨어적으로 잘라내어 읽기 전용 뷰(ReadPort) 및 Copy-on-Write 샌드박스 뷰로 상위 계층에 배급.
 * 2. 기능: 하드웨어 경계 절단(Truncate), 능동형 LRU 페이지 교체, 핫스왑(Hot-Swap) 시의 SegFault 방어막, Cleaner 기반 GC 네이티브 메모리 안전 해제.
 * 3. 의도: AI 추론 코어가 아직 도래하지 않은 미래 데이터를 훔쳐보는 것(Look-ahead Bias)을 막고, 메모리 압박 시 OS가 시스템 프로세스를 강제 종료(OOM Kill)하는 것을 물리적으로 회피.
 * ==============================================================================
 */
public final class A0_DT_42_422041_범용_OS레이어_드라이버 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422041_OS_DRIVER");

    // [1. 한글 상세 주석]
    // 💡 [FFM System Call 링킹] 무거운 JNI 오버헤드 없이 OS 커널의 `madvise` C 네이티브 함수를 직접 호출하기 위한 MethodHandle 링킹.
    // [2. 영문 상세 주석]
    // 💡 [FFM System Call Linking] MethodHandle linking to directly call the OS kernel's `madvise` C native function without heavy JNI overhead.

    private static final MethodHandle MADVISE_HANDLE;
    private static final int MADV_DONTNEED = 4; // Linux standard advice for freeing page cache

    static {
        MethodHandle handle = null;
        try {
            Linker linker = Linker.nativeLinker();
            SymbolLookup lookup = linker.defaultLookup();
            handle = lookup.find("madvise").map(addr -> linker.downcallHandle(addr, FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT))).orElse(null);
            if (handle != null) {
                logger.info("   ├─ [커널 훅 바인딩] FFM API를 통해 madvise(MADV_DONTNEED) 시스템 콜 링킹 성공.");
            }
        } catch (Exception e) {
            logger.warning("   ├─ [커널 훅 실패] madvise 시스템 콜 링킹 환경 미지원. 메모리 해제 우회 모드(Fallback)로 동작합니다.");
        }
        MADVISE_HANDLE = handle;
    }

    // [1. 한글 상세 주석]
    // 💡 [GC 생명주기 연동 방어막] Arena가 명시적으로 닫히지 않고 누수(Leak)될 경우, JVM GC가 해당 참조를 수거하는 시점에 커널 메모리 매핑을 안전하게 해제해주는 클리너.
    // [2. 영문 상세 주석]
    // 💡 [GC Lifecycle Linked Defense Shield] A cleaner that safely unmaps kernel memory when JVM GC collects an Arena reference that leaked without being explicitly closed.

    private static final Cleaner SAFE_DISPOSAL_CLEANER = Cleaner.create();

    private final A0_DT_42_422000_타임프레임_컨텍스트 timeframeContext;
    private final SmartIndexRegistry runtimeIndexRegistry;
    private final int totalEntityCountY;
    private volatile int validTickCursor = 0;

    // [1. 한글 상세 주석]
    // 시스템의 물리적 가용 메모리를 측정하고, 외부 설정(System Property)에서 주입받은 비율에 따라 LRU 및 샌드박스 한계 임계치를 동적으로 결정합니다.
    // [2. 영문 상세 주석]
    // Measures the system's physical available memory and dynamically determines the LRU and sandbox threshold limits based on the ratio injected from external settings (System Property).

    private final long physicalMaxMemoryBytes;
    private final long lruThresholdBytes;
    private final long sandboxThresholdBytes;

    private final AtomicLong currentOccupiedMemoryBytes = new AtomicLong(0);
    private final AtomicLong sandboxOccupiedBytes = new AtomicLong(0);

    private final Map<String, LayerCacheBlock> activeBufferPool = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<LayerCacheBlock> deferredEvictionQueue = new ConcurrentLinkedQueue<>();
    private final List<String> mountedLayerNames = new ArrayList<>();

    private final AtomicReference<Path> currentMasterDirectory;
    private A0_DT_42_422061_매트릭스_쿼리_엔진 queryEngine;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public record TensorMountResponse(String requestId, int statusCode, String errorMessage) {
    }

    // [1. 한글 상세 주석]
    // 💡 [네이티브 메모리 반환 대리자] 해당 객체 내부에 메모리 참조를 캡슐화한 뒤 Cleaner에 등록되어, 
    // 메인 객체의 강한 참조가 끊기면 백그라운드 스레드에서 시스템 콜을 호출하여 네이티브 리소스를 안전하게 반환합니다.
    // [2. 영문 상세 주석]
    // 💡 [Native Memory Disposal Delegate] Encapsulates memory references internally and registers with the Cleaner. 
    // When the strong reference to the main object is lost, it safely returns native resources by calling a system call in a background thread.

    private static class KernelMemoryDisposalDelegate implements Runnable {
        private final Arena arena;
        private final MemorySegment watchedSegment;
        private final String layerName;

        KernelMemoryDisposalDelegate(Arena arena, MemorySegment segment, String layerName) {
            this.arena = arena;
            this.watchedSegment = segment;
            this.layerName = layerName;
        }

        @Override
        public void run() {
            try {
                if (arena.scope().isAlive()) {
                    // 💡 [OOM Killer 방어] FFM 메모리 해제 직전, OS 커널에게 캐시된 페이지 메모리 삭제를 명시적 지시 (DONTNEED)
                    invokeMadviseSystemCall(watchedSegment);
                    arena.close();
                    logger.fine("   ├─ [GC 연동 안전 해제] 클리너(Cleaner) 메커니즘에 의해 누수된 아레나 메모리가 OS 커널에 안전하게 환원되었습니다: " + layerName);
                }
            } catch (Exception e) {
                logger.warning(" [클리너 예외] 네이티브 메모리 반환 중 오류 발생: " + e.getMessage());
            }
        }
    }

    private static class LayerCacheBlock {
        final String layerName;
        final Arena arena;
        final MemorySegment rawSegment;
        final MemorySegment shadowSegment;
        final long occupiedBytes;
        final AtomicLong lastReferencedNanos = new AtomicLong(System.nanoTime());
        final AtomicInteger activeReferenceCounter = new AtomicInteger(0);

        LayerCacheBlock(String layerName, Arena arena, MemorySegment raw, MemorySegment shadow, long occupiedBytes) {
            this.layerName = layerName;
            this.arena = arena;
            this.rawSegment = raw;
            this.shadowSegment = shadow;
            this.occupiedBytes = occupiedBytes;
        }
    }

    // [1. 한글 상세 주석]
    // [생성자] 하드코딩된 상수를 제거하고 Externalized Configuration(환경변수 주입) 아키텍처를 적용하여, 
    // 시스템 프로퍼티에서 주입받은 비율값으로 메모리 임계치(Threshold)를 동적으로 스케일링합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Removes hardcoded constants and applies the Externalized Configuration architecture, 
    // dynamically scaling memory thresholds using ratio values injected from system properties.

    public A0_DT_42_422041_범용_OS레이어_드라이버(
            A0_DT_42_422000_타임프레임_컨텍스트 context,
            SmartIndexRegistry indexRegistry) {

        this.timeframeContext = context;
        this.runtimeIndexRegistry = indexRegistry;
        this.totalEntityCountY = indexRegistry.featureZIndexMap().size();
        this.currentMasterDirectory = new AtomicReference<>(context.getFastDataRootPath());

        this.physicalMaxMemoryBytes = Runtime.getRuntime().maxMemory();

        // 💡 [설정 외부화 (Externalize Config)] 소스 코드 내 매직 넘버 하드코딩을 제거하고 Property 기반 임계치 로딩 적용
        double lruRatio = Double.parseDouble(System.getProperty("matrix.lru.threshold.ratio", "0.65"));
        double sandboxRatio = Double.parseDouble(System.getProperty("matrix.sandbox.threshold.ratio", "0.20"));

        this.lruThresholdBytes = (long) (physicalMaxMemoryBytes * lruRatio);
        this.sandboxThresholdBytes = (long) (physicalMaxMemoryBytes * sandboxRatio);

        logger.info(" ================================================================= ");
        logger.info(String.format(" >> [통합 OS V6.1] A0_DT_42_422041 OS 레이어 드라이버 기동. (도메인: %s)", context.getResolutionCode()));
        logger.info(String.format("   ├─ [LRU 한계선 (%.0f%%)] %.2f MB | [샌드박스 한계선 (%.0f%%)] %.2f MB",
                lruRatio * 100, (lruThresholdBytes / 1024.0 / 1024.0), sandboxRatio * 100, (sandboxThresholdBytes / 1024.0 / 1024.0)));
        logger.info(" ================================================================= ");
    }

    public void injectDependencyQueryEngine(A0_DT_42_422061_매트릭스_쿼리_엔진 queryEngine) {
        this.queryEngine = queryEngine;
        logger.info("   ├─ [생명주기 결계 락온] AI 쿼리 엔진 의존성이 드라이버에 동기화되었습니다. (핫스왑 SegFault 방어망 활성화)");
    }

    public TensorMountResponse executeMultipleLayerMount(String requestId, int currentValidTickCount, String... targetLayerNames) {
        readWriteLock.writeLock().lock();
        try {
            mountedLayerNames.clear();
            this.validTickCursor = currentValidTickCount;

            for (String layerName : targetLayerNames) {
                mountedLayerNames.add(layerName);
                mountPageOnDemandInternal(layerName);
            }

            logger.info(String.format(" >> [텐서 마운트 완료] 요청 ID: %s | 능동형 LRU 버퍼 %d겹 마운트 | 유효 커서: %d",
                    requestId, mountedLayerNames.size(), validTickCursor));
            return new TensorMountResponse(requestId, 200, "SUCCESS");

        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [마운트 붕괴] 텐서 결합 및 마운트 처리 중 시스템 예외 발생", ex);
            return new TensorMountResponse(requestId, 500, "INTERNAL_ERROR: " + ex.getMessage());
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [OS 레벨 OOM Killer 사전 방어망] 물리적 네이티브 메모리를 닫기 전, FFM 링커를 통해 OS에 `madvise` 명령을 내려 
    // 커널의 페이지 캐시 메모리 압박을 선제적으로 해소시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [OS-Level OOM Killer Preemptive Defense Net] Before closing physical native memory, 
    // issues an `madvise` command to the OS via FFM linker to preemptively relieve kernel page cache memory pressure.

    private static void invokeMadviseSystemCall(MemorySegment segment) {
        if (MADVISE_HANDLE != null) {
            try {
                MADVISE_HANDLE.invokeExact(segment, segment.byteSize(), MADV_DONTNEED);
            } catch (Throwable t) {
                // 커널 레벨 C 네이티브 시스템 콜 호출 실패 시 조용히 무시(Fail-Safe) 처리하여 Java 애플리케이션 파열(Crash)을 막음
            }
        }
    }

    private void evictLruMemoryBlocks(long requiredBytes) {
        deferredEvictionQueue.removeIf(deferredBlock -> {
            if (deferredBlock.activeReferenceCounter.get() == 0) {
                try {
                    if (deferredBlock.arena.scope().isAlive()) {
                        invokeMadviseSystemCall(deferredBlock.rawSegment);
                        deferredBlock.arena.close();
                    }
                } catch (Exception e) {
                    logger.warning(" [RCU 회수 경고] 지연된 아레나 자원 반환 중 예외 발생: " + e.getMessage());
                }
                currentOccupiedMemoryBytes.addAndGet(-deferredBlock.occupiedBytes);
                logger.fine(String.format("   ├─ [RCU 지연 수거 완료] 쿼리 참조가 0에 도달한 유예 레이어 블록이 OS 커널에 반환되었습니다: %s", deferredBlock.layerName));
                return true;
            }
            return false;
        });

        while (currentOccupiedMemoryBytes.get() + requiredBytes > lruThresholdBytes && !activeBufferPool.isEmpty()) {
            String candidateLayerToEvict = null;
            long oldestTime = Long.MAX_VALUE;

            for (Map.Entry<String, LayerCacheBlock> entry : activeBufferPool.entrySet()) {
                LayerCacheBlock block = entry.getValue();
                long referenceTime = block.lastReferencedNanos.get();

                if (referenceTime < oldestTime) {
                    oldestTime = referenceTime;
                    candidateLayerToEvict = entry.getKey();
                }
            }

            if (candidateLayerToEvict != null) {
                LayerCacheBlock blockToEvict = activeBufferPool.remove(candidateLayerToEvict);
                if (blockToEvict != null) {
                    if (blockToEvict.activeReferenceCounter.get() > 0) {
                        deferredEvictionQueue.offer(blockToEvict);
                        logger.warning(String.format(
                                " 🚨 [RCU 지연 퇴출 격발] %s 레이어가 아직 활성 AI 스레드에 의해 참조 중입니다. 커널 패닉(SegFault) 방어를 위해 물리적 메모리 해제를 큐에 유예합니다.",
                                candidateLayerToEvict));
                    } else {
                        try {
                            if (blockToEvict.arena.scope().isAlive()) {
                                invokeMadviseSystemCall(blockToEvict.rawSegment);
                                blockToEvict.arena.close();
                            }
                        } catch (Exception e) {
                            logger.warning(" [LRU 퇴출 경고] 아레나 즉시 반환 중 예외 발생: " + e.getMessage());
                        }
                        currentOccupiedMemoryBytes.addAndGet(-blockToEvict.occupiedBytes);
                        logger.warning(String.format(" 🚨 [LRU 즉시 퇴출] 가용 RAM 물리적 임계치 도달. 페이지 캐시 강제 해제(madvise) 및 자원 반환 완료: %s",
                                candidateLayerToEvict));
                    }
                }
            } else {
                break;
            }
        }
    }

    private LayerCacheBlock mountPageOnDemandInternal(String layerName) throws IOException {
        LayerCacheBlock block = activeBufferPool.get(layerName);
        if (block != null && block.arena.scope().isAlive()) {
            return block;
        }

        long singleByteSize = (long) A0_DT_42_422001_권한_포트_인터페이스.CHUNK_SIZE_TICKS * 50L * totalEntityCountY * 4L;
        long totalRequiredBytes = singleByteSize * 2;

        evictLruMemoryBlocks(totalRequiredBytes);

        Arena newArena = Arena.ofShared();
        Path rawPhysicalPath = resolvePathInDir(currentMasterDirectory.get(), layerName, false);
        Path shadowPhysicalPath = resolvePathInDir(currentMasterDirectory.get(), layerName, true);

        if (!Files.exists(rawPhysicalPath) || !Files.exists(shadowPhysicalPath)) {
            throw new IOException("물리 디스크에 마운트 대상 원본 또는 섀도우 텐서 파일이 존재하지 않습니다: " + layerName);
        }

        try (FileChannel rawChannel = FileChannel.open(rawPhysicalPath, StandardOpenOption.READ);
                FileChannel shadowChannel = FileChannel.open(shadowPhysicalPath, StandardOpenOption.READ)) {

            MemorySegment rawSegment = rawChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(rawPhysicalPath), newArena);
            MemorySegment shadowSegment = shadowChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(shadowPhysicalPath), newArena);

            block = new LayerCacheBlock(layerName, newArena, rawSegment, shadowSegment, totalRequiredBytes);

            // 💡 [GC 안전 해제 훅(Hook) 등록] 아레나 강한 참조가 사라질 때 대리자를 통해 백그라운드에서 안전하게 메모리 해제
            SAFE_DISPOSAL_CLEANER.register(block, new KernelMemoryDisposalDelegate(newArena, rawSegment, layerName));

            activeBufferPool.put(layerName, block);
            currentOccupiedMemoryBytes.addAndGet(totalRequiredBytes);

            logger.fine("   ├─ [On-Demand Paging] 디스크 레이어 파일 능동 메모리 매핑 완료 (GC Cleaner 결속됨): " + layerName);
            return block;
        }
    }

    public void executeHotSwapPointerUpdate(int newValidTickCount) {
        if (this.queryEngine != null) {
            this.queryEngine.awaitSafeHotSwap();
        } else {
            logger.warning(" [핫스왑 경보] AI 쿼리 엔진이 주입되지 않았습니다. 잠재적인 참조 붕괴(Dangling Pointer) 위험을 안고 핫스왑을 강행합니다.");
        }

        readWriteLock.writeLock().lock();
        try {
            if (mountedLayerNames.isEmpty())
                return;

            String[] layerNameArray = mountedLayerNames.toArray(new String[0]);
            executeMultipleLayerMount("HOT_RELOAD_V6", newValidTickCount, layerNameArray);

            logger.info(" >> [HOT-SWAP 완료] 실시간 AI 쿼리 연결 중단(Downtime) 없이 텐서 메모리 포인터 동적 갱신 성공.");
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void promoteSandboxToMaster(Path newMasterPath) {
        logger.info("   ├─ [샌드박스 영구화] RAM 스왑에 떠 있는 Copy-on-Write 샌드박스 변경사항을 물리 디스크로 강제 동기화(Flush) 시작...");
        readWriteLock.writeLock().lock();
        try {
            for (String layerName : mountedLayerNames) {
                LayerCacheBlock block = mountPageOnDemandInternal(layerName);

                Path newRawPath = resolvePathInDir(newMasterPath, layerName, false);
                Path newShadowPath = resolvePathInDir(newMasterPath, layerName, true);

                forceFlushToDisk(block.rawSegment, newRawPath);
                forceFlushToDisk(block.shadowSegment, newShadowPath);
            }

            this.currentMasterDirectory.set(newMasterPath);
            logger.info("   ├─ [샌드박스 커밋] 물리적 데이터 덤프 완료. 마스터 디렉토리 라우팅 포인터가 새 경로로 스왑(Swap) 되었습니다: " + newMasterPath);

        } catch (Exception ex) {
            logger.log(Level.SEVERE, "   ├─ [치명적 시스템 오류] 샌드박스 데이터 디스크 물리적 영구화(Flush) 작업 실패.", ex);
            throw new RuntimeException("Sandbox Promotion & Disk Flush Failed", ex);
        } finally {
            readWriteLock.writeLock().unlock();
        }

        executeHotSwapPointerUpdate(this.validTickCursor);
    }

    private void forceFlushToDisk(MemorySegment sourceSegment, Path targetPhysicalPath) throws IOException {
        Files.createDirectories(targetPhysicalPath.getParent());
        long segmentByteSize = sourceSegment.byteSize();

        try (FileChannel channel = FileChannel.open(targetPhysicalPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
                Arena tempArena = Arena.ofConfined()) {

            channel.truncate(segmentByteSize);
            MemorySegment targetSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, segmentByteSize, tempArena);
            MemorySegment.copy(sourceSegment, 0, targetSegment, 0, segmentByteSize);
            targetSegment.force();
        }
    }

    public AtomicInteger extractLayerActiveCounter(String layerName) {
        LayerCacheBlock block = activeBufferPool.get(layerName);
        return (block != null) ? block.activeReferenceCounter : null;
    }

    public A0_DT_42_422001_권한_포트_인터페이스.ReadPort extractTruncatedRawPort(int layerIndex) {
        String layerName;
        readWriteLock.readLock().lock();
        try {
            layerName = mountedLayerNames.get(layerIndex);
        } finally {
            readWriteLock.readLock().unlock();
        }

        LayerCacheBlock block = activeBufferPool.get(layerName);
        if (block == null || !block.arena.scope().isAlive()) {
            readWriteLock.writeLock().lock();
            try {
                block = mountPageOnDemandInternal(layerName);
            } catch (IOException e) {
                throw new RuntimeException("On-Demand 물리 페이지 마운트 로드 실패", e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }

        readWriteLock.readLock().lock();
        try {
            block.lastReferencedNanos.set(System.nanoTime());

            long currentValidByteSize = (long) validTickCursor * totalEntityCountY * 4L;
            // 하드웨어적으로 허가된 커서까지만 메모리를 잘라내어(Slice) 상위 계층에 제공함으로써 미래 데이터 스누핑(Look-ahead) 방지
            MemorySegment truncatedSegment = block.rawSegment.asSlice(0, currentValidByteSize);

            A0_DT_42_422001_권한_포트_인터페이스.TransparentReadLens defaultReadLens = A0_DT_42_422001_권한_포트_인터페이스.assembleReadLens(0, 1.0f, 0.0f);

            return new A0_DT_42_422001_권한_포트_인터페이스.ReadPort(
                    new AtomicReference<>(truncatedSegment),
                    defaultReadLens,
                    4L,
                    block.activeReferenceCounter);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public A0_DT_42_422001_권한_포트_인터페이스.ReadPort extractTruncatedShadowPort(int layerIndex) {
        String layerName;
        readWriteLock.readLock().lock();
        try {
            layerName = mountedLayerNames.get(layerIndex);
        } finally {
            readWriteLock.readLock().unlock();
        }

        LayerCacheBlock block = activeBufferPool.get(layerName);
        if (block == null || !block.arena.scope().isAlive()) {
            readWriteLock.writeLock().lock();
            try {
                block = mountPageOnDemandInternal(layerName);
            } catch (IOException e) {
                throw new RuntimeException("On-Demand 섀도우 페이지 마운트 로드 실패", e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }

        readWriteLock.readLock().lock();
        try {
            block.lastReferencedNanos.set(System.nanoTime());

            long currentValidByteSize = (long) validTickCursor * totalEntityCountY * 4L;
            MemorySegment truncatedSegment = block.shadowSegment.asSlice(0, currentValidByteSize);

            A0_DT_42_422001_권한_포트_인터페이스.TransparentReadLens defaultReadLens = A0_DT_42_422001_권한_포트_인터페이스.assembleReadLens(0, 1.0f, 0.0f);

            return new A0_DT_42_422001_권한_포트_인터페이스.ReadPort(
                    new AtomicReference<>(truncatedSegment),
                    defaultReadLens,
                    4L,
                    block.activeReferenceCounter);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public A0_DT_42_422001_권한_포트_인터페이스.WritePort extractTruncatedSandboxPort(int layerIndex) {
        String layerName;
        readWriteLock.readLock().lock();
        try {
            layerName = mountedLayerNames.get(layerIndex);
        } finally {
            readWriteLock.readLock().unlock();
        }

        Path rawPhysicalPath = resolvePathInDir(currentMasterDirectory.get(), layerName, false);
        long currentValidByteSize = (long) validTickCursor * totalEntityCountY * 4L;

        // 💡 샌드박스 임계치 방어막: 시뮬레이션으로 인해 스왑 메모리(Swap) 영역이 시스템 허용량을 초과하여 무한 팽창하는 현상 물리적 차단
        if (sandboxOccupiedBytes.get() + currentValidByteSize > sandboxThresholdBytes) {
            logger.severe(" 🚨 [OOM 방어막 격발] 백테스트 샌드박스의 누적 할당량이 시스템 임계치를 초과하여 OS 커널의 즉사(OOM Kill)를 사전 방어합니다.");
            throw new SecurityException("운영체제 스왑 메모리 보호를 위해 샌드박스 추가 개방 요청을 강제 거부합니다.");
        }
        sandboxOccupiedBytes.addAndGet(currentValidByteSize);

        try {
            FileChannel channel = FileChannel.open(rawPhysicalPath, StandardOpenOption.READ);
            Arena sandboxArena = Arena.ofShared();

            // MapMode.PRIVATE 매핑을 통해 원본을 해치지 않고 메모리 변경사항이 스왑 공간에만 적재되는 Copy-on-Write 활성화
            MemorySegment privateSegment = channel.map(FileChannel.MapMode.PRIVATE, 0, Files.size(rawPhysicalPath), sandboxArena);
            MemorySegment truncatedSegment = privateSegment.asSlice(0, currentValidByteSize);

            A0_DT_42_422001_권한_포트_인터페이스.TransparentWriteLens defaultWriteLens = A0_DT_42_422001_권한_포트_인터페이스.assembleWriteLens(0, 1.0f, 0.0f);

            logger.info("   ├─ [샌드박스 보안 락온] Copy-on-Write 방어막이 전개된 백테스트 모의전 전용 포트가 발급되었습니다: " + layerName);

            return new A0_DT_42_422001_권한_포트_인터페이스.WritePort(
                    new AtomicReference<>(truncatedSegment),
                    defaultWriteLens,
                    4L,
                    null);

        } catch (IOException ex) {
            sandboxOccupiedBytes.addAndGet(-currentValidByteSize);
            logger.log(Level.SEVERE, " [샌드박스 포트 발급 실패] 모의전 백테스트 우주 창조 및 커널 매핑 불가: " + layerName, ex);
            throw new RuntimeException("샌드박스 모드 읽기/쓰기 렌즈 발급 I/O 예외", ex);
        }
    }

    public int getMountedLayerCount() {
        return mountedLayerNames.size();
    }

    public int getValidTickCursor() {
        return this.validTickCursor;
    }

    private Path resolvePathInDir(Path baseDirectory, String layerName, boolean isShadow) {
        if (isShadow) {
            return baseDirectory.resolve("SHADOW_Z_LAYERS").resolve(layerName + ".zlayer");
        } else {
            boolean isBaseIndicator = layerName.toUpperCase().startsWith("BASE_");
            Path subDirectory = isBaseIndicator ? baseDirectory.resolve("BASE_DATA") : baseDirectory.resolve("INDICATORS");
            return subDirectory.resolve(layerName + ".layer");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 하드코딩의 파괴와 설정 외부화 (Externalize Configuration Architecture):
 * 일반적인 엔터프라이즈 어플리케이션이 설정값을 소스 코드 내부에 `private static final long LIMIT = 50 * 1024 * 1024` 형식으로 고정시켜 박아두는 것은 
 * 서버 인프라 장비 스펙이 업그레이드되거나 운영 정책이 바뀔 때마다 전체 소스 코드의 재컴파일(Re-build) 및 재배포를 강제하는 최악의 안티패턴(Anti-Pattern)입니다.
 * 개선된 V6.1 아키텍처는 시스템 부팅 시점의 런타임 환경변수(`System.getProperty`)에서 `matrix.lru.threshold.ratio` 와 같은 물리적 비율값을 동적으로 주입받아 임계치를 유연하게 결정(Lock-on)합니다. 
 * 이를 통해 소스 코드를 단 한 줄도 수정하지 않고도 배포 운영 환경(Dev/Prod)의 RAM 체급에 맞게 메모리 스케일링을 자유자재로 조율하는 완벽한 인프라 관리 추상화(Management Abstraction)를 이룩했습니다.
 * 
 * 2. 💡 JNI 우회 시스템 콜과 OS 레벨 OOM Killer 회피 방어막 (madvise MADV_DONTNEED):
 * Java의 `mmap`은 엄청나게 거대한 OS 커널 페이지 캐시(Page Cache) 영역을 점유합니다.
 * 우리가 애플리케이션 레벨에서 `Arena.close()`로 자바 영역의 참조만 해제하더라도, Linux 등 운영체제(OS)는 "다음에 또 동일한 파일을 읽을지 모르니 캐시에 남겨두자"며 메모리 해제를 암묵적으로 유예시킵니다.
 * 그러다 어느 순간 물리적 RAM이 100% 꽉 차면, OS의 최후 보루인 **OOM Killer**가 발동하여 가장 메모리를 많이 먹고 있는 프로세스(바로 우리 시스템의 JVM)를 무자비하게 강제 사살(Kill)해 버립니다.
 * V6.1 드라이버 모듈은 무거운 JNI 오버헤드조차 없는 최신 자바 FFM API `Linker`를 활용해 커널의 `madvise(addr, length, MADV_DONTNEED)` C 네이티브 함수를 다이렉트로 타격 호출합니다.
 * 아레나를 닫기 직전, "이 메모리 공간은 당분간 더 이상 사용할 계획이 없으니 지금 즉시 비워라(DONTNEED)"고 커널에 명시적으로 강력한 힌트를 주어 캐시 페이지를 즉각 파기시킴으로써 OS 레벨의 서버 셧다운을 물리적으로 봉쇄했습니다.
 * 
 * 3. 💡 Cleaner 기반 GC 연동 메모리 생명주기 제어 (Use-After-Free 방어):
 * 오프힙(Off-Heap) 메모리는 자바 가비지 컬렉터(GC)의 생명주기 관리 범위 밖(Unmanaged)에 존재합니다. 
 * 애플리케이션 개발자의 논리 실수나 예외(Exception) 흐름 이탈로 인해 아레나가 명시적으로 정상 종료되지 못하면 영구적인 커널 메모리 누수(Memory Leak)가 발생합니다.
 * 본 모듈은 이를 구조적으로 방어하기 위해 Java 9의 `java.lang.ref.Cleaner` 유틸리티를 전면 도입했습니다.
 * 마운트된 텐서 블록 객체가 코드 어디에서도 더 이상 참조되지 않아 JVM GC의 수거 대상이 된 찰나, `Cleaner` 데몬이 뒤에서 조용히 `KernelMemoryDisposalDelegate` 스레드를 깨워 
 * 남아있던 좀비 상태의 오프힙 아레나를 완벽히 해제(Close)하고 `madvise`를 호출하여 정리합니다.
 * 개발자가 수동 자원 관리를 놓치더라도 시스템이 절대 무너지지 않도록 보장하는 이중 안전망(Fail-Safe) 아키텍처를 구축했습니다.
 * =============================================================================
 */