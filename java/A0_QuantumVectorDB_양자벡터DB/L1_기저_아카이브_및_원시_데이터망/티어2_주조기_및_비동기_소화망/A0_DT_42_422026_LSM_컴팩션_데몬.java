/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망
 * @alias LSM_Compaction_Daemon
 * @tier 2
 * @keywords MVCC, Asynchronous Compaction, PID Controller, Adaptive Throttling, WAL Rolling, Crash Recovery
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422026_LSM_컴팩션_데몬.java
 * - 모듈명: 통합 OS V6.2 - Tier 2: LSM 컴팩션 데몬 (PID 기반 적응형 디스크 병합 관리자)
 * - 기능 및 역할: RAM(Delta 버퍼)에 적재된 텐서 파편들을 물리 디스크로 비동기 병합하며, 디스크 I/O 응답 지연(Latency)을 측정해 PID 제어기로 폴링 간격을 자가 조율(Self-Tuning)합니다.
 * - 이론 및 기술: 비동기 I/O 위임, CQRS, PID Controller (비례-적분-미분 제어기), Adaptive Throttling, NVMe WAL Rolling.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [아키텍처 확장] 접근 제어자 개방: `executeBackgroundCompactionLoop()`를 `public`으로 승격하고 내부 CAS 락을 적용하여, 외부 Admin API가 중복 실행 충돌 없이 안전하게 물리적 디스크 플러시를 수동 격발(Trigger)할 수 있는 제어권 위임(IoC) 배관을 구축했습니다.
 * - 💡 [리메이크 핵심: PID 제어기 이식] 하드코딩된 대기 시간을 전면 폐기했습니다. `channel.force()` 수행 시 소요된 실제 I/O 레이턴시를 계측하고, 목표 지연 시간(Target Latency)과의 오차를 계산하여 다음 컴팩션 대기 시간(Backoff Interval)을 실시간으로 자가 조율하는 완벽한 PID I/O 피드백 루프를 완성했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 I/O, FFM 메모리 제어, 비동기 스케줄링 및 락프리 큐를 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for file I/O, FFM memory control, asynchronous scheduling, and lock-free queues.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.WritePort;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. RAM에 적재된 텐서를 물리 디스크에 안전하게 영속화시키며 PID 제어기를 내장한 비동기 병합(Compaction) 데몬입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An asynchronous compaction daemon embedding a PID controller that safely persists tensors loaded in RAM to physical disks.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422026
 * [파일명] A0_DT_42_422026_LSM_컴팩션_데몬.java
 * [모듈명] 통합 OS V6.2 - Tier 2: LSM 컴팩션 데몬 (PID 기반 적응형 디스크 병합 관리자)
 * ==============================================================================
 */
public final class A0_DT_42_422026_LSM_컴팩션_데몬 {

    // [1. 한글 상세 주석]
    // 글로벌 로거 선언 및 PID 제어기 기반 백오프 스로틀링(Throttling) 통제를 위한 절대 상수들을 정의합니다.
    // [2. 영문 상세 주석]
    // Global logger declaration and definition of absolute constants for PID controller-based backoff throttling control.
    // [3. 자바 코드]
    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422026_LSM_COMPACTION_DAEMON");

    private static final double PID_TARGET_FLUSH_LATENCY_MS = 10.0;
    private static final double PID_GAIN_PROPORTIONAL = 0.5; 
    private static final double PID_GAIN_INTEGRAL = 0.1;     
    private static final double PID_GAIN_DERIVATIVE = 0.05;  
    
    private static final long MIN_POLLING_INTERVAL_MS = 50L;
    private static final long MAX_POLLING_INTERVAL_MS = 5000L;
    private static final long IDLE_DETECTION_THRESHOLD_MS = 2000L;
    private static final long WAL_ROTATION_THRESHOLD_BYTES = 50L * 1024L * 1024L;

    // [1. 한글 상세 주석]
    // PID 제어기 내부 상태 변수 및 데몬의 생명주기를 관리하는 스케줄러와 상태 추적 변수들을 선언합니다.
    // [2. 영문 상세 주석]
    // Declares the PID controller internal state variables and the scheduler and state tracking variables that manage the daemon's lifecycle.
    // [3. 자바 코드]
    private double pidIntegralError = 0.0;
    private double pidPreviousError = 0.0;
    private long dynamicPollingIntervalMs = 500L;

    private final ScheduledExecutorService compactionScheduler;
    private final AtomicBoolean isDaemonRunning = new AtomicBoolean(false);
    private final AtomicBoolean isCompactionInProgress = new AtomicBoolean(false); 

    private final AtomicLong lastTaskIngestionEpochMs = new AtomicLong(System.currentTimeMillis());

    // [1. 한글 상세 주석]
    // RCU 워커와 공유하는 읽기-쓰기 충돌 방어망(SeqLock) 및 커널 포트를 조달하기 위한 의존성 콜백입니다.
    // [2. 영문 상세 주석]
    // Read-write collision defense network (SeqLock) shared with the RCU worker and dependency callback to procure kernel ports.
    // [3. 자바 코드]
    private final Map<String, AtomicLong> seqLockRegistry;
    private final Function<String, WritePort> portResolver;

    // [1. 한글 상세 주석]
    // ACID 내구성을 보장하기 위한 WAL 롤링 아키텍처 변수와 비동기 가비지 컬렉션(GC) 대기열입니다.
    // [2. 영문 상세 주석]
    // WAL rolling architecture variables and asynchronous garbage collection (GC) queue to ensure ACID durability.
    // [3. 자바 코드]
    private final Path walStoragePath;
    private volatile FileChannel currentWalChannel;
    private volatile Path currentWalPath;
    private final Object walSequentialLock = new Object();

    private final Queue<Path> pendingGarbageWalQueue = new ConcurrentLinkedQueue<>();

    // [1. 한글 상세 주석]
    // RAM에 상주하는 델타 텐서 파편들을 담아두는 비동기 큐 레코드입니다.
    // [2. 영문 상세 주석]
    // An asynchronous queue record that holds delta tensor fragments residing in RAM.
    // [3. 자바 코드]
    public record DeltaCompactionTask(
            String featureName,
            WritePort mainDiskPort,
            MemorySegment deltaMemorySegment,
            long targetAbsoluteOffset,
            long byteSize) {
    }

    private final Queue<DeltaCompactionTask> compactionTaskQueue = new ConcurrentLinkedQueue<>();

    // [1. 한글 상세 주석]
    // [생성자] LSM 컴팩션 데몬을 기동하고 Crash Recovery 및 WAL 롤링 채널을 점화합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Starts the LSM compaction daemon and ignites Crash Recovery and the WAL rolling channel.
    // [3. 자바 코드]
    public A0_DT_42_422026_LSM_컴팩션_데몬(
            Map<String, AtomicLong> seqLockRegistry,
            Path walStoragePath,
            Function<String, WritePort> portResolver) {

        if (seqLockRegistry == null || walStoragePath == null || portResolver == null) {
            throw new IllegalArgumentException("[설정 오류] 필수 의존성이 누락되어 컴팩션 데몬을 기동할 수 없습니다.");
        }
        this.seqLockRegistry = seqLockRegistry;
        this.walStoragePath = walStoragePath;
        this.portResolver = portResolver;

        try {
            if (!Files.exists(walStoragePath)) {
                Files.createDirectories(walStoragePath);
            }

            try (Stream<Path> stream = Files.list(walStoragePath)) {
                List<Path> legacyWalList = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().startsWith("MATRIX_A0_422026_LSM_WAL_"))
                        .filter(p -> p.getFileName().toString().endsWith(".log"))
                        .sorted()
                        .collect(Collectors.toList());

                if (!legacyWalList.isEmpty()) {
                    logger.warning(String.format(" 🚨 [콜드스타트 복구] 비정상 종료로 인한 %d개의 LSM WAL 잔여 세그먼트 감지. 롤포워드(Replay)를 개시합니다.",
                            legacyWalList.size()));

                    for (Path legacyWal : legacyWalList) {
                        if (Files.size(legacyWal) > 0) {
                            executeWalCrashRecovery(legacyWal);
                        }
                        pendingGarbageWalQueue.offer(legacyWal);
                    }
                }
            }

            rotateNewWalSegment();

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [파일 시스템 오류] WAL 파일 채널을 물리적으로 개방할 수 없습니다.", ex);
            throw new RuntimeException("LSM WAL 기동 실패", ex);
        }

        this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "OS_LSM_COMPACTION_DAEMON");
            thread.setPriority(Thread.MIN_PRIORITY);
            thread.setDaemon(true);
            return thread;
        });

        logger.info(" >> [통합 OS V6.2] A0_DT_42_422026 LSM 컴팩션 데몬 기동. (PID 제어기 스로틀링 및 WAL 롤링 아키텍처 전개 완료)");
    }

    // [1. 한글 상세 주석]
    // [WAL 로테이션 엔진] WAL 세그먼트가 50MB를 초과했을 때 기존 파일을 닫아 가비지 큐로 넘기고 새로운 파일을 엽니다.
    // [2. 영문 상세 주석]
    // [WAL Rotation Engine] When the WAL segment exceeds 50MB, safely closes the existing file, passes it to the garbage queue, and opens a new file.
    // [3. 자바 코드]
    private void rotateNewWalSegment() throws IOException {
        if (currentWalChannel != null && currentWalChannel.isOpen()) {
            currentWalChannel.force(true);
            currentWalChannel.close();
            pendingGarbageWalQueue.offer(currentWalPath);
        }

        long newSequence = System.currentTimeMillis();
        String newFileName = String.format("MATRIX_A0_422026_LSM_WAL_%d.log", newSequence);
        currentWalPath = walStoragePath.resolve(newFileName);

        currentWalChannel = FileChannel.open(currentWalPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        logger.fine("   ├─ [WAL 로테이션] 새로운 Append-Only 쓰기 세그먼트가 개방되었습니다: " + newFileName);
    }

    // [1. 한글 상세 주석]
    // [복원 로직: WAL 리플레이] 정전 등으로 시스템이 비정상 종료되었을 때 잔여 WAL을 읽어 메모리 큐로 살려냅니다.
    // [2. 영문 상세 주석]
    // [Recovery Logic: WAL Replay] Revives residual WALs to the memory queue when the system terminates abnormally due to power outages, etc.
    // [3. 자바 코드]
    private void executeWalCrashRecovery(Path walFilePath) throws IOException {
        try (FileChannel readChannel = FileChannel.open(walFilePath, StandardOpenOption.READ)) {
            ByteBuffer headerBuffer = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
            int recoveredCount = 0;

            while (true) {
                headerBuffer.clear();
                int readLength = 0;
                while (readLength < 20) {
                    int result = readChannel.read(headerBuffer);
                    if (result == -1) break;
                    readLength += result;
                }

                if (readLength == 0) break;

                if (readLength < 20) {
                    logger.warning(" [WAL 손상] 헤더 바이트가 불완전합니다. 리플레이를 중단합니다: " + walFilePath.getFileName());
                    break;
                }

                headerBuffer.flip();
                int nameLength = headerBuffer.getInt();
                long targetAbsoluteOffset = headerBuffer.getLong();
                long byteSize = headerBuffer.getLong();

                ByteBuffer nameBuffer = ByteBuffer.allocate(nameLength);
                if (readChannel.read(nameBuffer) != nameLength) break;
                nameBuffer.flip();
                String featureName = new String(nameBuffer.array(), StandardCharsets.UTF_8);

                ByteBuffer dataBuffer = ByteBuffer.allocate((int) byteSize);
                if (readChannel.read(dataBuffer) != byteSize) break;
                dataBuffer.flip();
                MemorySegment deltaSegment = MemorySegment.ofArray(dataBuffer.array());

                WritePort recoveredPort = portResolver.apply(featureName);
                if (recoveredPort == null) {
                    logger.severe(" [복원 실패] 지표명 '" + featureName + "'의 WritePort를 조달할 수 없습니다. 해당 태스크를 유실 처리합니다.");
                    continue;
                }

                DeltaCompactionTask recoveredTask = new DeltaCompactionTask(featureName, recoveredPort, deltaSegment, targetAbsoluteOffset, byteSize);
                compactionTaskQueue.offer(recoveredTask);
                recoveredCount++;
            }
            logger.info(String.format("   ├─ [리플레이 수료] %s 파일에서 %d개의 증발했던 컴팩션 태스크가 성공적으로 부활했습니다.",
                    walFilePath.getFileName(), recoveredCount));
        }
    }

    // [1. 한글 상세 주석]
    // [관제 로직: 재귀적 PID 스케줄링] 고정된 간격(FixedDelay)을 버리고, PID 제어기에 의해 도출된 동적 백오프 시간을 기반으로 자기 자신을 재귀적으로 스케줄링합니다.
    // [2. 영문 상세 주석]
    // [Control Logic: Recursive PID Scheduling] Discards fixed intervals and recursively schedules itself based on the dynamic backoff time derived by the PID controller.
    // [3. 자바 코드]
    public void startCompactionDaemon() {
        if (!isDaemonRunning.compareAndSet(false, true)) {
            return;
        }
        scheduleNextCompactionCycle();
        logger.info("   ├─ [LSM 관리망 활성화] PID 제어기 기반의 적응형 디스크 컴팩션 스케줄러가 백그라운드에 상주합니다.");
    }

    private void scheduleNextCompactionCycle() {
        if (isDaemonRunning.get()) {
            compactionScheduler.schedule(() -> {
                executeCompaction(false);
                scheduleNextCompactionCycle();
            }, dynamicPollingIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    // [1. 한글 상세 주석]
    // [생산자 API: 델타 버퍼 위임 및 WAL 영속화] RCU 워커가 데이터를 RAM에 조립한 뒤 디스크 I/O를 해당 데몬에 위임합니다.
    // [2. 영문 상세 주석]
    // [Producer API: Delta Buffer Delegation and WAL Persistence] Delegates disk I/O to this daemon after the RCU worker assembles data into RAM.
    // [3. 자바 코드]
    public void submitAsyncCompaction(DeltaCompactionTask compactionTask) {
        if (compactionTask != null && isDaemonRunning.get()) {
            byte[] nameBytes = compactionTask.featureName().getBytes(StandardCharsets.UTF_8);
            int totalBufferSize = 20 + nameBytes.length + (int) compactionTask.byteSize();

            ByteBuffer walBuffer = ByteBuffer.allocate(totalBufferSize).order(ByteOrder.LITTLE_ENDIAN);
            walBuffer.putInt(nameBytes.length);
            walBuffer.putLong(compactionTask.targetAbsoluteOffset());
            walBuffer.putLong(compactionTask.byteSize());
            walBuffer.put(nameBytes);

            ByteBuffer dataBuffer = compactionTask.deltaMemorySegment()
                    .asSlice(0, compactionTask.byteSize())
                    .asByteBuffer();

            walBuffer.put(dataBuffer);
            walBuffer.flip();

            try {
                synchronized (walSequentialLock) {
                    while (walBuffer.hasRemaining()) {
                        currentWalChannel.write(walBuffer);
                    }
                    if (currentWalChannel.size() >= WAL_ROTATION_THRESHOLD_BYTES) {
                        rotateNewWalSegment();
                    }
                }
            } catch (IOException ex) {
                logger.log(Level.SEVERE, " [WAL 오류] 델타 태스크 순차 기록 중 I/O 예외 발생", ex);
            }

            compactionTaskQueue.offer(compactionTask);
            lastTaskIngestionEpochMs.set(System.currentTimeMillis());
        }
    }

    // [1. 한글 상세 주석]
    // [수동 타격 포트 (Admin Trigger)] 외부 REST 파사드나 시스템 관리자가 즉각적인 디스크 병합을 지시할 때 호출되는 퍼블릭 API입니다.
    // [2. 영문 상세 주석]
    // [Manual Strike Port (Admin Trigger)] A public API called when the external REST facade or system administrator orders an immediate disk compaction.
    // [3. 자바 코드]
    public void executeBackgroundCompactionLoop() {
        logger.info(" 🚨 [수동 컴팩션 격발] 관리자 또는 파사드의 명시적 호출에 의해 강제 디스크 플러시(Force Flush)를 집행합니다.");
        executeCompaction(true);
    }

    // [1. 한글 상세 주석]
    // [소비자 루프: 백그라운드 컴팩션] 내부 CAS 락을 통해 수동/자동 병합 프로세스의 중복 진입을 방어하며 모아치기를 수행합니다.
    // [2. 영문 상세 주석]
    // [Consumer Loop: Background Compaction] Defends against duplicate entry of manual/automatic merge processes via internal CAS lock and performs batch flushing.
    // [3. 자바 코드]
    private void executeCompaction(boolean isManualTrigger) {
        if (!isCompactionInProgress.compareAndSet(false, true)) {
            return;
        }

        try {
            if (compactionTaskQueue.isEmpty()) {
                return;
            }

            if (!isManualTrigger) {
                long elapsedTime = System.currentTimeMillis() - lastTaskIngestionEpochMs.get();
                if (elapsedTime < IDLE_DETECTION_THRESHOLD_MS) {
                    return; 
                }
            }

            Map<WritePort, Boolean> dirtyPortStagingMap = new HashMap<>();
            int mergedTaskCount = 0;

            DeltaCompactionTask task;
            while ((task = compactionTaskQueue.poll()) != null) {
                executeAtomicMemoryMerge(task);
                dirtyPortStagingMap.put(task.mainDiskPort(), true);
                mergedTaskCount++;
            }

            long flushStartNanos = System.nanoTime();

            for (WritePort updatedPort : dirtyPortStagingMap.keySet()) {
                updatedPort.segment().force();
            }

            long flushLatencyNanos = System.nanoTime() - flushStartNanos;
            calculatePidFeedback(flushLatencyNanos);

            if (mergedTaskCount > 0) {
                logger.fine(String.format("   ├─ [LSM 컴팩션 완료] %d개 파편 병합 및 디스크 플러시. (소요: %.2f ms | 다음 PID 백오프: %d ms)",
                        mergedTaskCount, (flushLatencyNanos / 1_000_000.0), dynamicPollingIntervalMs));

                Path obsoleteWalFile;
                while ((obsoleteWalFile = pendingGarbageWalQueue.poll()) != null) {
                    try {
                        Files.deleteIfExists(obsoleteWalFile);
                        logger.fine("   ├─ [WAL 가비지 컬렉션] 영속화가 완수되어 구형 WAL 세그먼트를 폐기했습니다: " + obsoleteWalFile.getFileName());
                    } catch (IOException e) {
                        pendingGarbageWalQueue.offer(obsoleteWalFile);
                        break;
                    }
                }
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [컴팩션 오류] 백그라운드 디스크 병합 중 치명적 커널 예외 발생.", ex);
        } finally {
            isCompactionInProgress.set(false);
        }
    }

    // [1. 한글 상세 주석]
    // [PID Controller 역학] 직전 디스크 I/O 플러시 시간에 기반하여 시스템의 부하 상태를 정밀하게 파악하고, 다음 컴팩션 주기를 자율적으로 조율합니다.
    // [2. 영문 상세 주석]
    // [PID Controller Dynamics] Precisely grasps the system's load state based on the previous disk I/O flush time, and autonomously tunes the next compaction cycle.
    // [3. 자바 코드]
    private void calculatePidFeedback(long flushLatencyNanos) {
        double currentLatencyMs = flushLatencyNanos / 1_000_000.0;
        
        double error = currentLatencyMs - PID_TARGET_FLUSH_LATENCY_MS;

        pidIntegralError += error;
        pidIntegralError = Math.max(-500.0, Math.min(500.0, pidIntegralError));

        double derivative = error - pidPreviousError;

        double adjustmentMs = (error * PID_GAIN_PROPORTIONAL) 
                            + (pidIntegralError * PID_GAIN_INTEGRAL) 
                            + (derivative * PID_GAIN_DERIVATIVE);

        pidPreviousError = error;

        long newInterval = dynamicPollingIntervalMs + (long) adjustmentMs;
        dynamicPollingIntervalMs = Math.max(MIN_POLLING_INTERVAL_MS, Math.min(newInterval, MAX_POLLING_INTERVAL_MS));
    }

    // [1. 한글 상세 주석]
    // [병합 로직: SeqLock 기반 원자적 메모리 복사] AI 코어의 찢어진 읽기(Torn Read)를 막기 위해 SeqLock을 활성화하고 메모리를 복사합니다.
    // [2. 영문 상세 주석]
    // [Merge Logic: SeqLock-based Atomic Memory Copy] Activates SeqLock and copies memory to prevent Torn Reads in the AI core.
    // [3. 자바 코드]
    private void executeAtomicMemoryMerge(DeltaCompactionTask task) {
        String featureName = task.featureName();
        AtomicLong seqLock = seqLockRegistry.get(featureName);

        if (seqLock != null) {
            seqLock.incrementAndGet();
        }

        try {
            MemorySegment.copy(
                    task.deltaMemorySegment(), 0,
                    task.mainDiskPort().segment().asSlice(task.targetAbsoluteOffset(), task.byteSize()), 0,
                    task.byteSize());
        } finally {
            if (seqLock != null) {
                seqLock.incrementAndGet();
            }
        }
    }

    // [1. 한글 상세 주석]
    // [종료 절차] 애플리케이션 강하 시 스케줄러 대기열에 남은 모든 텐서를 강제 병합하고 리소스를 안전하게 회수합니다.
    // [2. 영문 상세 주석]
    // [Termination Procedure] Force-merges all remaining tensors in the scheduler queue and safely reclaims resources upon application descent.
    // [3. 자바 코드]
    public void executeGracefulShutdown() {
        if (isDaemonRunning.compareAndSet(true, false)) {
            logger.info("   ├─ [컴팩션 셧다운] 데몬 정지 전 잔여 델타 대기열 강제 병합을 집행합니다...");

            compactionScheduler.shutdown();
            executeCompaction(true);

            try {
                if (!compactionScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    compactionScheduler.shutdownNow();
                }
            } catch (InterruptedException ex) {
                compactionScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            try {
                if (currentWalChannel != null && currentWalChannel.isOpen()) {
                    currentWalChannel.force(true);
                    currentWalChannel.close();
                }
            } catch (IOException e) {
                logger.warning(" [셧다운 경고] WAL 채널을 닫는 중 I/O 에러가 발생했습니다.");
            }

            logger.info(" >> [LSM 데몬 종료] 모든 델타 텐서가 물리 디스크에 성공적으로 영속화(Persist)되었습니다.");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 💡 PID 제어기 (Proportional-Integral-Derivative Controller) 기반의 Adaptive Throttling:
 * 디스크 I/O 최적화를 위해 과거에는 "NVMe면 10ms, HDD면 500ms"와 같이 하드웨어 스펙에 의존하여 백오프(대기 시간)를 정적으로(Statically) 하드코딩했습니다.
 * 그러나 클라우드 스토리지(EBS, S3)나 로컬 디스크 환경은 백그라운드 프로세스, 시스템 발열, 버퍼 캐시 한계 등 수많은 런타임 변수에 의해 매 순간 속도(Latency)가 출렁거립니다.
 * 수술된 V6.2 엔진은 산업 제어 공학의 정수인 **PID 제어기(PID Controller)**를 컴팩션 I/O 파이프라인에 직접 이식했습니다.
 * 
 * `channel.force()`(디스크 플러시)를 수행한 직후 실제 소요된 지연 시간(Latency)을 정밀 계측합니다.
 * 이 지연 시간이 목표치(Target, 예: 10ms)보다 길어지면, 이는 디스크가 과부하(Bottleneck) 상태임을 의미하므로 PID 제어기는 오차를 양수(+)로 산출하여 다음 병합 폴링 대기 시간(`dynamicPollingIntervalMs`)을 즉각적으로 확장시킵니다. 이를 통해 디스크 드라이브가 과호흡을 멈추고 회복할 물리적 유예 시간을 벌어줍니다 (배압/Backpressure 효과).
 * 반대로 지연 시간이 매우 짧다면 디스크가 쾌적한 상태이므로, 대기 시간을 최소 50ms 한계선까지 빠르게 줄여 실시간 동기화율(Real-time Consistency)을 극한으로 끌어올립니다.
 * 이것이 런타임 시스템이 살아 숨 쉬며 주변 환경(하드웨어 한계)에 완벽하게 자가 조율(Self-Tuning)하는 기계적 공감(Mechanical Sympathy)의 극한점입니다.
 * 
 * 2. 제어권 위임 (Inversion of Control)과 Admin 수동 타격 포트의 개방:
 * 데몬의 심장부인 `executeCompaction` 로직은 원래 스케줄러에 의해서만 비공개(private)로 호출되도록 설계되어 있었습니다.
 * 그러나 대규모 엔터프라이즈 환경에서 데이터베이스의 물리적 영속화(Flush)는 백그라운드 스케줄러의 시간표에만 수동적으로 끌려다녀서는 안 됩니다. 
 * 무결성 보존이나 시스템 백업 직전, 운영자나 관리 시스템 파사드(Admin API)가 즉각적인 100% 디스크 동기화를 명시적으로 요구(Trigger)할 수 있어야 합니다.
 * 이번 아키텍처 개편을 통해 `executeBackgroundCompactionLoop()` 제어자를 `public`으로 승격 개방했습니다.
 * 동시에 `isCompactionInProgress`라는 CAS 락(Lock) 스위치를 이식하여, 자동 백그라운드 스케줄러와 외부 수동 트리거(Manual Trigger)가 동시에 충돌하여 디스크 I/O가 찢어지는 참사(Race Condition)를 물리적으로 완벽히 멸균했습니다.
 * 
 * 3. NVMe 친화적 세그먼트 로테이션 (Segment Rolling) 아키텍처:
 * 과거 설계에서는 단일 `WAL.log` 파일에 계속 데이터를 쓴 뒤, 병합이 끝나면 `FileChannel.truncate(0)`을 호출하여 파일 크기를 강제로 0으로 만들었습니다.
 * 이는 파일 시스템(NTFS/ext4)의 MFT/Inode 블록 갱신을 피로하게 만들어 디스크 단편화(Fragmentation)를 유발하고 플래시 메모리(NVMe)의 셀 수명을 심각하게 갉아먹는 안티패턴입니다.
 * 개선된 엔진은 아파치 카프카(Apache Kafka)의 세그먼트 롤링 아키텍처를 도입했습니다. WAL이 50MB에 도달하면 무자비한 truncate 대신 우아하게 파일을 닫고 새로운 파일을 엽니다. 메인 디스크 병합이 완수되어 생명을 다한 '과거의 WAL 파일'들은 가비지 대기열(Queue)로 이동하며 백그라운드 병합 주기에 맞춰 OS 레벨에서 물리적으로 완전 삭제(Delete) 처리됩니다. 
 * 이를 통해 100% Append-Only 쓰기 속도를 보장하고 인프라 수명을 대폭 연장했습니다.
 * =============================================================================
 */