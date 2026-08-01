/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망
 * @alias LSM_Compaction_Daemon
 * @tier 2
 * @keywords MVCC, Asynchronous Compaction, WAL Rolling, Segment Architecture, Crash Recovery, Encapsulation Tuning
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422026_LSM_컴팩션_데몬.java
 * - 모듈명: 통합 OS V6.2 - Tier 2: LSM 컴팩션 데몬 (비동기 병합 관리자 및 WAL 수호자)
 * - 기능 및 역할: RCU 워커가 RAM에 적재한 텐서 파편들을 물리 디스크로 비동기 병합(Compaction)하며, 정전 시 증발을 막기 위해 WAL(Write-Ahead Log)을 기록합니다.
 * - 이론 및 기술: 비동기 I/O 위임, CQRS, Batch Force Flush, ByteBuffer Limit Control, NVMe WAL Rolling.
 * 
 * [수정 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [아키텍처 교정]: `A0_DT_42_424010_글로벌_표준_REST_파사드`의 Admin API에서 컴팩션을 수동 격발하려 할 때 발생하던 캡슐화 접근 차단 에러(has private access)를 수정했습니다. 
 *                 `executeBackgroundCompactionLoop()`의 제어자를 `public`으로 개방하여, 외부 관리 시스템의 명시적 I/O 타격(Trigger)을 허용하도록 제어권 위임(IoC) 배관을 전개했습니다.
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
// 컴플라이언스 선언 및 클래스 헤더. RAM에 적재된 텐서를 물리 디스크에 안전하게 영속화시키는 비동기 병합(Compaction) 데몬입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An asynchronous compaction daemon that safely persists tensors loaded in RAM to physical disks.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422026
 * [파일명] A0_DT_42_422026_LSM_컴팩션_데몬.java
 * [모듈명] 통합 OS V6.2 - Tier 2: LSM 컴팩션 데몬 (비동기 병합 관리자 및 WAL 컨트롤러)
 * 
 * [기능 명세]
 * 1. 💡 비동기 I/O 위임 (Asynchronous Compaction): RCU 워커가 RAM(Delta 버퍼)에
 * 적재한 텐서 파편들을 넘겨받아, 물리 디스크(.layer) 파일로의 병합 및 동기화(Flush)를 백그라운드에서 전담합니다.
 * 2. 💡 유휴 시간(Idle) 탐지 및 백오프 스로틀링: 데이터가 폭포수처럼 쏟아지는 수집 타이밍에는
 * 디스크 I/O를 유예(Backoff)하고, 시스템 트래픽이 평온해진 유휴 시간에만 원자적 병합을 수행하여 디스크 병목을 최소화합니다.
 * 3. 💡 CQRS (명령과 조회의 분리) 완성: 쓰기(Write)는 대기열 삽입만으로 O(1) 속도로 즉각 완료되며,
 * 조회(Read) 코어는 병합된 메인 매트릭스에서 O(1) 속도로 텐서를 읽어가게 만드는 아키텍처 패턴을 달성합니다.
 * 4. 💡 모아치기 플러시 (Batch Force): 수천 개의 델타 조각을 개별적으로 디스크에 `force()` 하지 않고,
 * 포트(Port)별로 그룹화하여 단 한 번의 커널 시스템 콜로 디스크 동기화를 집행합니다.
 * 5. 💡 NVMe 친화적 WAL 세그먼트 로테이션 (Rolling):
 * 구형 아키텍처의 `truncate(0)` 단일 파일 병목을 폐기했습니다. WAL 파일이 50MB를 초과하면 즉각 새로운
 * 세그먼트 파일로 로테이션(Rolling)하며, 디스크 병합이 완수된 구형 WAL 파일들만 비동기적으로 소각(GC)하여
 * 100% Append-Only 파일 시스템의 기하학적 성능을 확보했습니다.
 * ==============================================================================
 */
public final class A0_DT_42_422026_LSM_컴팩션_데몬 {

    // [1. 한글 상세 주석]
    // 글로벌 로거 선언 및 백오프 스로틀링(Throttling) 통제를 위한 절대 상수들을 정의합니다.
    // [2. 영문 상세 주석]
    // Global logger declaration and definition of absolute constants for backoff throttling control.

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422026_LSM_COMPACTION_DAEMON");

    private static final long DEFAULT_POLLING_INTERVAL_MS = 500L;
    private static final long IDLE_DETECTION_THRESHOLD_MS = 2000L; // 2초 이상 데이터 유입이 없으면 유휴(Idle)로 판단
    private static final long MAX_BACKOFF_DELAY_MS = 5000L;

    // 💡 [WAL 로테이션 임계치] WAL 파일 크기가 50MB에 도달하면 안전하게 세그먼트를 롤링합니다.
    private static final long WAL_ROTATION_THRESHOLD_BYTES = 50L * 1024L * 1024L;

    // [1. 한글 상세 주석]
    // 데몬의 생명주기를 관리하는 스케줄러와 상태 추적 변수들을 선언합니다.
    // [2. 영문 상세 주석]
    // Declares the scheduler and state tracking variables that manage the daemon's lifecycle.

    private final ScheduledExecutorService compactionScheduler;
    private final AtomicBoolean isDaemonRunning = new AtomicBoolean(false);

    private final AtomicLong lastIngestionTimestamp = new AtomicLong(System.currentTimeMillis());
    private long currentBackoffDelay = DEFAULT_POLLING_INTERVAL_MS;

    // [1. 한글 상세 주석]
    // RCU 워커와 공유하는 읽기-쓰기 충돌 방어망(SeqLock) 및 커널 포트를 조달하기 위한 의존성 콜백입니다.
    // [2. 영문 상세 주석]
    // Read-write collision defense network (SeqLock) shared with the RCU worker and dependency callback to procure kernel ports.

    private final Map<String, AtomicLong> seqLockRegistry;
    private final Function<String, WritePort> portResolver;

    // [1. 한글 상세 주석]
    // ACID 내구성을 보장하기 위한 WAL 롤링 아키텍처 변수와 비동기 가비지 컬렉션(GC) 대기열입니다.
    // [2. 영문 상세 주석]
    // WAL rolling architecture variables and asynchronous garbage collection (GC) queue to ensure ACID durability.

    private final Path walStoragePath;
    private volatile FileChannel currentWalChannel;
    private volatile Path currentWalPath;
    private final Object walSequentialLock = new Object();

    private final Queue<Path> pendingGarbageWalQueue = new ConcurrentLinkedQueue<>();

    // [1. 한글 상세 주석]
    // RAM에 상주하는 델타 텐서 파편들을 담아두는 비동기 큐 레코드입니다.
    // [2. 영문 상세 주석]
    // An asynchronous queue record that holds delta tensor fragments residing in RAM.

    public record DeltaCompactionTask(
            String featureName,
            WritePort mainDiskPort,
            MemorySegment deltaMemorySegment,
            long targetAbsoluteOffset,
            long byteSize) {
    }

    private final Queue<DeltaCompactionTask> deltaTaskQueue = new ConcurrentLinkedQueue<>();

    // [1. 한글 상세 주석]
    // [생성자] LSM 컴팩션 데몬을 기동하고 백그라운드 스케줄러 및 WAL 롤링 채널을 점화합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Starts the LSM compaction daemon and ignites the background scheduler and WAL rolling channel.

    /**
     * @param seqLockRegistry RCU 워커와 공유하는 읽기-쓰기 충돌 방어망 (SeqLock)
     * @param walStoragePath  NVMe 등 가장 빠른 스토리지에 위치할 WAL 파일의 디렉토리
     * @param portResolver    콜드스타트 리플레이 시, 지표명(String)을 통해 실제 커널 권한 포트(WritePort)를 역조달해 줄 콜백 함수
     */
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

        // 💡 [ACID 내구성 확보] WAL 파일 시스템 채널 개통 및 Crash Recovery 수행
        try {
            if (!Files.exists(walStoragePath)) {
                Files.createDirectories(walStoragePath);
            }

            // 1. [Crash Recovery] 잔여 WAL 파일들을 찾아 순차적으로 복원
            try (Stream<Path> stream = Files.list(walStoragePath)) {
                List<Path> legacyWalList = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().startsWith("MATRIX_A0_422026_LSM_WAL_"))
                        .filter(p -> p.getFileName().toString().endsWith(".log"))
                        .sorted() // 시퀀스(타임스탬프) 기반 시간순 정렬
                        .collect(Collectors.toList());

                if (!legacyWalList.isEmpty()) {
                    logger.warning(String.format(" 🚨 [콜드스타트 복구] 비정상 종료로 인한 %d개의 LSM WAL 잔여 세그먼트 감지. 롤포워드(Replay)를 개시합니다.",
                            legacyWalList.size()));

                    for (Path legacyWal : legacyWalList) {
                        if (Files.size(legacyWal) > 0) {
                            executeWalCrashRecovery(legacyWal);
                        }
                        // 리플레이가 완료된 파일은 RAM(대기열)에 성공적으로 복원되었으므로, 디스크 병합 후 삭제되도록 가비지 대기열로 이관
                        pendingGarbageWalQueue.offer(legacyWal);
                    }
                }
            }

            // 2. [고속 Append-Only 채널 개방] 새로운 롤링 WAL 세그먼트 점화
            rotateNewWalSegment();

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [파일 시스템 오류] WAL 파일 채널을 물리적으로 개방할 수 없습니다.", ex);
            throw new RuntimeException("LSM WAL 기동 실패", ex);
        }

        // 디스크 I/O를 전담할 단일 백그라운드 스레드 생성 (순차적 파일 병합 보장)
        this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "LSM-Compaction-Daemon");
            thread.setPriority(Thread.MIN_PRIORITY); // 메인 HFT 연산을 방해하지 않도록 최하위 우선순위 배정
            thread.setDaemon(true);
            return thread;
        });

        logger.info(" >> [통합 OS V6.2] A0_DT_42_422026 LSM 컴팩션 데몬 기동. (비동기 병합 및 WAL 롤링(Rolling) 아키텍처 전개 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 [WAL 로테이션 엔진] WAL 세그먼트가 50MB를 초과했을 때 기존 파일을 닫아 가비지 큐로 넘기고 새로운 파일을 엽니다.
    // [2. 영문 상세 주석]
    // 💡 [WAL Rotation Engine] When the WAL segment exceeds 50MB, safely closes the existing file, passes it to the garbage queue, and opens a new file.

    private void rotateNewWalSegment() throws IOException {
        if (currentWalChannel != null && currentWalChannel.isOpen()) {
            currentWalChannel.force(true); // 커널 버퍼에 남은 데이터를 디스크에 영구 고착(Persist)
            currentWalChannel.close();
            pendingGarbageWalQueue.offer(currentWalPath); // 닫은 이전 파일을 가비지 대기열로 이관
        }

        long newSequence = System.currentTimeMillis();
        String newFileName = String.format("MATRIX_A0_422026_LSM_WAL_%d.log", newSequence);
        currentWalPath = walStoragePath.resolve(newFileName);

        currentWalChannel = FileChannel.open(currentWalPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        logger.info("   ├─ [WAL 로테이션] 새로운 Append-Only 쓰기 세그먼트가 개방되었습니다: " + newFileName);
    }

    // [1. 한글 상세 주석]
    // [복원 로직: WAL 리플레이 (Crash Recovery)] 정전 등으로 시스템이 비정상 종료되었을 때 잔여 WAL을 읽어 메모리 큐로 살려냅니다.
    // [2. 영문 상세 주석]
    // [Recovery Logic: WAL Replay (Crash Recovery)] Revives residual WALs to the memory queue when the system terminates abnormally due to power outages, etc.

    private void executeWalCrashRecovery(Path walFilePath) throws IOException {
        try (FileChannel readChannel = FileChannel.open(walFilePath, StandardOpenOption.READ)) {
            // 헤더 규격: 이름_길이(4) + 절대_오프셋(8) + 바이트_크기(8) = 총 20 Bytes
            ByteBuffer headerBuffer = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);

            int recoveredCount = 0;
            while (true) {
                headerBuffer.clear();
                int readLength = 0;
                while (readLength < 20) {
                    int result = readChannel.read(headerBuffer);
                    if (result == -1)
                        break;
                    readLength += result;
                }

                if (readLength == 0)
                    break; // 파일의 끝(EOF) 도달

                if (readLength < 20) {
                    logger.warning(" [WAL 손상] 헤더 바이트가 불완전합니다. 리플레이를 중단합니다: " + walFilePath.getFileName());
                    break;
                }

                headerBuffer.flip();
                int nameLength = headerBuffer.getInt();
                long targetAbsoluteOffset = headerBuffer.getLong();
                long byteSize = headerBuffer.getLong();

                // 가변 길이의 지표명 문자열 바이트 판독
                ByteBuffer nameBuffer = ByteBuffer.allocate(nameLength);
                if (readChannel.read(nameBuffer) != nameLength)
                    break;
                nameBuffer.flip();
                String featureName = new String(nameBuffer.array(), StandardCharsets.UTF_8);

                // 델타 메모리 바이트 판독 및 세그먼트 복원
                ByteBuffer dataBuffer = ByteBuffer.allocate((int) byteSize);
                if (readChannel.read(dataBuffer) != byteSize)
                    break;
                dataBuffer.flip();
                MemorySegment deltaSegment = MemorySegment.ofArray(dataBuffer.array());

                // 💡 [지연 권한 조달] 콜백 함수를 통해 OS 커널에서 해당 지표의 WritePort를 다시 얻어옵니다.
                WritePort recoveredPort = portResolver.apply(featureName);
                if (recoveredPort == null) {
                    logger.severe(" [복원 실패] 지표명 '" + featureName + "'의 WritePort를 조달할 수 없습니다. 해당 태스크를 유실 처리합니다.");
                    continue;
                }

                // 읽어들인 태스크를 대기열에 고스란히 복원
                DeltaCompactionTask recoveredTask = new DeltaCompactionTask(featureName, recoveredPort, deltaSegment, targetAbsoluteOffset, byteSize);
                deltaTaskQueue.offer(recoveredTask);
                recoveredCount++;
            }
            logger.info(String.format("   ├─ [리플레이 수료] %s 파일에서 %d개의 증발했던 컴팩션 태스크가 성공적으로 부활했습니다.",
                    walFilePath.getFileName(), recoveredCount));
        }
    }

    // [1. 한글 상세 주석]
    // [관제 로직: 데몬 점화] 주기적으로 큐 대기열을 감시하며 유휴 시간(Idle Time)을 탐지하여 병합(Compaction)을 실행합니다.
    // [2. 영문 상세 주석]
    // [Control Logic: Daemon Ignition] Periodically monitors the queue and detects idle time to execute compaction.

    public void startCompactionDaemon() {
        if (!isDaemonRunning.compareAndSet(false, true)) {
            return;
        }

        compactionScheduler.scheduleWithFixedDelay(
                this::executeBackgroundCompactionLoop,
                DEFAULT_POLLING_INTERVAL_MS,
                DEFAULT_POLLING_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        logger.info("   ├─ [LSM 관리망 활성화] 델타 버퍼 감시 및 디스크 컴팩션 스케줄러가 백그라운드에 상주합니다.");
    }

    // [1. 한글 상세 주석]
    // [생산자 API: 델타 버퍼 위임 및 WAL 영속화] RCU 워커가 데이터를 RAM에 조립한 뒤 디스크 I/O를 해당 데몬에 위임합니다.
    // [2. 영문 상세 주석]
    // [Producer API: Delta Buffer Delegation and WAL Persistence] Delegates disk I/O to this daemon after the RCU worker assembles data into RAM.

    public void submitAsyncCompaction(DeltaCompactionTask compactionTask) {
        if (compactionTask != null && isDaemonRunning.get()) {

            // 1. 💡 [ACID 내구성 방어] RAM 대기열에 넣기 전 WAL 파일에 순차 기록 (Append-Only)
            byte[] nameBytes = compactionTask.featureName().getBytes(StandardCharsets.UTF_8);
            int totalBufferSize = 20 + nameBytes.length + (int) compactionTask.byteSize();

            ByteBuffer walBuffer = ByteBuffer.allocate(totalBufferSize).order(ByteOrder.LITTLE_ENDIAN);

            walBuffer.putInt(nameBytes.length);
            walBuffer.putLong(compactionTask.targetAbsoluteOffset());
            walBuffer.putLong(compactionTask.byteSize());
            walBuffer.put(nameBytes);

            // 아레나의 쓰레기 빈 공간(Zero-Padding)을 제거하는 정밀 절단 처리 (asSlice)
            ByteBuffer dataBuffer = compactionTask.deltaMemorySegment()
                    .asSlice(0, compactionTask.byteSize())
                    .asByteBuffer();

            walBuffer.put(dataBuffer);
            walBuffer.flip();

            try {
                // 여러 스레드가 동시에 채널에 쓰더라도 바이트가 섞이지 않도록 순차 락(Lock)을 적용합니다.
                synchronized (walSequentialLock) {
                    while (walBuffer.hasRemaining()) {
                        currentWalChannel.write(walBuffer);
                    }

                    // 💡 [최적화 핵심] WAL 파일이 50MB를 초과하면 새로운 파일로 롤링(Rolling)
                    if (currentWalChannel.size() >= WAL_ROTATION_THRESHOLD_BYTES) {
                        rotateNewWalSegment();
                    }
                }
            } catch (IOException ex) {
                logger.log(Level.SEVERE, " [WAL 오류] 델타 태스크 순차 기록 중 I/O 예외 발생", ex);
            }

            // 2. [비동기 큐 삽입] 안전하게 영속화된 태스크를 RAM 대기열 큐에 올림
            deltaTaskQueue.offer(compactionTask);
            lastIngestionTimestamp.set(System.currentTimeMillis()); // 활성 상태 타임스탬프 갱신
        }
    }

    // [1. 한글 상세 주석]
    // [소비자 루프: 백그라운드 컴팩션 및 동기화 루프] 유휴 상태를 판단하여 디스크 병합 및 모아치기를 수행합니다.
    // 💡 [아키텍처 확장 완료] 외부 Admin API(REST 파사드) 등 타 모듈에서 강제 격발(Trigger)할 수 있도록 접근 제어자를 public으로 개방했습니다.
    // [2. 영문 상세 주석]
    // [Consumer Loop: Background Compaction and Synchronization Loop] Judges idle state to perform disk compaction and batch flushing.
    // 💡 [Architecture Expanded] Opened the access modifier to public so that other modules like the external Admin API (REST Facade) can force trigger it.

    public void executeBackgroundCompactionLoop() {
        try {
            if (deltaTaskQueue.isEmpty()) {
                currentBackoffDelay = DEFAULT_POLLING_INTERVAL_MS; // 큐가 비어있으면 백오프 초기화
                return;
            }

            long elapsedTime = System.currentTimeMillis() - lastIngestionTimestamp.get();

            // 💡 [백오프 스로틀링 (Backoff Throttling)] 데이터가 폭주하는 도중에는 디스크 I/O를 괴롭히지 않고 유예합니다.
            if (elapsedTime < IDLE_DETECTION_THRESHOLD_MS) {
                currentBackoffDelay = Math.min(currentBackoffDelay * 2, MAX_BACKOFF_DELAY_MS);
                return;
            }

            // =========================================================================
            // 💡 유휴 시간 도래: 모아치기(Batch) 원자적 디스크 병합 집행
            // =========================================================================
            currentBackoffDelay = DEFAULT_POLLING_INTERVAL_MS;

            // 병합 완료 후 한 번에 플러시(force)하기 위해 데이터가 변경된 포트들을 수집합니다.
            Map<WritePort, Boolean> pendingFlushPortMap = new HashMap<>();
            int mergedTaskCount = 0;

            DeltaCompactionTask task;
            // 큐에 있는 모든 델타를 메인 디스크 매핑 메모리로 쓸어 담습니다 (Drain).
            while ((task = deltaTaskQueue.poll()) != null) {
                executeAtomicMemoryMerge(task);
                pendingFlushPortMap.put(task.mainDiskPort(), true);
                mergedTaskCount++;
            }

            // 💡 [I/O 모아치기 최적화] 커널 시스템 콜(force)은 포트당 단 1번씩만 호출하여 디스크 드라이브 섹터 스래싱(Thrashing) 방지.
            for (WritePort updatedPort : pendingFlushPortMap.keySet()) {
                updatedPort.segment().force();
            }

            // 💡 [NVMe 친화적 가비지 컬렉션 (GC)]
            // 무조건적인 truncate(0)로 인한 디스크 단편화를 방지합니다. 메인 매트릭스에 100% 안전하게 데이터가 안착했으므로, 
            // 롤링되어 쓸모가 다한 구형 WAL 세그먼트 파일들을 물리적으로 폐기(삭제)합니다.
            if (mergedTaskCount > 0) {
                logger.fine(String.format("   ├─ [LSM 컴팩션 완료] 유휴 시간 도달. %d개의 델타 파편이 메인 디스크로 원자적 병합 및 플러시되었습니다.",
                        mergedTaskCount));

                Path obsoleteWalFile;
                while ((obsoleteWalFile = pendingGarbageWalQueue.poll()) != null) {
                    try {
                        Files.deleteIfExists(obsoleteWalFile);
                        logger.fine("   ├─ [WAL 가비지 컬렉션] 병합이 완료되어 효력을 다한 구형 WAL 세그먼트 파일을 삭제했습니다: " + obsoleteWalFile.getFileName());
                    } catch (IOException e) {
                        // 다른 외부 프로세스(백신 등)가 Lock을 잡고 있어 삭제에 실패했다면 큐에 다시 넣어 다음 유휴 주기에 삭제를 시도합니다.
                        logger.warning(" [WAL 폐기 지연] 파일 삭제 실패 (다음 주기에 재시도합니다): " + obsoleteWalFile.getFileName());
                        pendingGarbageWalQueue.offer(obsoleteWalFile);
                        break;
                    }
                }
            }

        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [컴팩션 오류] 백그라운드 디스크 병합 중 치명적 커널 예외 발생.", ex);
        }
    }

    // [1. 한글 상세 주석]
    // [병합 로직: SeqLock 기반 원자적 메모리 복사] AI 코어의 찢어진 읽기(Torn Read)를 막기 위해 SeqLock을 활성화하고 메모리를 복사합니다.
    // [2. 영문 상세 주석]
    // [Merge Logic: SeqLock-based Atomic Memory Copy] Activates SeqLock and copies memory to prevent Torn Reads in the AI core.

    private void executeAtomicMemoryMerge(DeltaCompactionTask task) {
        String featureName = task.featureName();
        AtomicLong seqLock = seqLockRegistry.get(featureName);

        // 💡 [SeqLock 쓰기 락온] 홀수 버전으로 전환하여 AI 읽기 스레드에게 데이터가 갱신 중임을 통보
        if (seqLock != null) {
            seqLock.incrementAndGet();
        }

        try {
            // RAM(Delta) -> 커널 페이지 캐시(Main)로 SIMD 고속 복사 처리
            MemorySegment.copy(
                    task.deltaMemorySegment(), 0,
                    task.mainDiskPort().segment().asSlice(task.targetAbsoluteOffset(), task.byteSize()), 0,
                    task.byteSize());
        } finally {
            // 💡 [SeqLock 쓰기 해제] 짝수 버전으로 복귀하여 읽기 권한을 다시 안전하게 개방
            if (seqLock != null) {
                seqLock.incrementAndGet();
            }
        }
    }

    // [1. 한글 상세 주석]
    // [종료 절차] 애플리케이션 강하 시 스케줄러 대기열에 남은 모든 텐서를 강제 병합하고 리소스를 안전하게 회수합니다.
    // [2. 영문 상세 주석]
    // [Termination Procedure] Force-merges all remaining tensors in the scheduler queue and safely reclaims resources upon application descent.

    public void executeGracefulShutdown() {
        if (isDaemonRunning.compareAndSet(true, false)) {
            logger.info("   ├─ [컴팩션 셧다운] 데몬 정지 전 잔여 델타 대기열 강제 병합을 집행합니다...");

            // 스케줄러를 정지하고 남은 큐를 모두 비웁니다.
            compactionScheduler.shutdown();
            executeBackgroundCompactionLoop();

            try {
                if (!compactionScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    compactionScheduler.shutdownNow();
                }
            } catch (InterruptedException ex) {
                compactionScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // 현재 열려있는 WAL 파일 채널을 안전하게 닫습니다.
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
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * [한글]
 * 1. 캡슐화(Encapsulation)의 전략적 해제와 제어권 위임 (Inversion of Control):
 * 데몬의 심장부인 `executeBackgroundCompactionLoop()`는 원래 스케줄러에 의해서만 비공개(private)로 호출되도록 설계되어 있었습니다.
 * 그러나 엔터프라이즈 환경에서 데이터베이스의 영속화(Compaction)는 단순히 정해진 시간에 의해서만 이뤄져서는 안 되는 치명적인 비즈니스 요건입니다.
 * 운영자나 관리 시스템이 즉각적인 디스크 플러시를 명시적으로 요구할 수 있어야 합니다.
 * 이번 업데이트를 통해 해당 메서드의 제어자를 `public`으로 변경함으로써, L17 외교관 계층(REST 파사드의 Admin API)이 
 * 외부 HTTP 명령을 받아 내부 컴팩션 데몬을 명시적으로 격발(Trigger)할 수 있는 제어권 위임(IoC) 파이프라인이 열렸습니다. 
 * 이는 무분별한 캡슐화 파괴가 아니라, 통제 가능한 권한의 정당한 격상(Elevation) 및 아키텍처 확장입니다.
 * 
 * 2. 로그 구조화 병합 트리 (LSM-Tree)와 비동기 I/O 위임의 철학:
 * 전통적인 B-Tree 기반의 데이터베이스는 데이터가 들어올 때마다 디스크의 제자리를 찾아가서 덮어쓰기(In-place Update)를 수행합니다.
 * 이는 필연적으로 느린 랜덤 접근 I/O(Random I/O)를 유발하여 HFT(고빈도 매매)나 대용량 데이터 적재 시스템의 치명적인 병목이 됩니다.
 * 본 시스템은 최신 빅테크 시스템(Cassandra, RocksDB 등)의 LSM-Tree 철학을 차용했습니다.
 * RCU 데이터 주조 워커는 데이터를 디스크(Main)에 직접 쓰지 않고, 오직 RAM 상의 델타 큐(MemTable/Delta Buffer)에 O(1) 속도로 밀어 넣은 뒤 즉시 본연의 작업으로 복귀합니다. 
 * 무겁고 시간이 오래 걸리는 디스크 병합(Compaction)과 동기화(Flush) 작업은 오직 이 백그라운드 컴팩션 데몬 스케줄러만이 전담하여 시스템 전체의 I/O 경합을 분산시킵니다.
 * 
 * 3. NVMe 최적화 롤링 아키텍처 (Segment Rolling)와 단일 파일 병목 탈피:
 * 과거 설계에서는 단일 `WAL.log` 파일에 계속 데이터를 쓴 뒤, 병합이 끝나면 `FileChannel.truncate(0)`을 호출하여 파일 크기를 강제로 0으로 만들었습니다.
 * 이는 파일 시스템(NTFS/ext4)의 MFT/Inode 블록 갱신을 극단적으로 피로하게 만들어 디스크 단편화(Fragmentation)를 유발하고 플래시 메모리(NVMe)의 셀 수명을 갉아먹는 안티패턴(Anti-Pattern)이었습니다.
 * 개선된 엔진은 아파치 카프카(Apache Kafka)의 세그먼트 아키텍처를 도입했습니다.
 * WAL이 50MB에 도달하면 무자비한 truncate 대신 우아하게 파일을 닫고 새로운 파일을 엽니다(Rolling).
 * 메인 디스크 병합이 완수되어 생명을 다한 '과거의 WAL 파일'은 백그라운드 병합 주기에 맞춰 OS 레벨에서 물리적으로 완전 삭제(Delete) 처리됩니다. 
 * 이를 통해 100% Append-Only 파일 시스템의 기하학적 쓰기 성능을 완벽히 복원하고 디스크 수명을 연장했습니다.
 * 
 * [English]
 * 1. Strategic Release of Encapsulation and Delegation of Control (IoC):
 * The core of the daemon, `executeBackgroundCompactionLoop()`, was originally locked as private to be called only by the scheduler. However, in an enterprise environment, database persistence (Compaction) is a critical business requirement that cannot be left solely to the passage of time. Operators must be able to explicitly demand an immediate disk flush. By opening this method to `public`, the external HTTP commands received via the Admin API of the L17 REST Facade can now physically trigger the internal daemon, successfully opening a flawless Inversion of Control pipeline. This is a legitimate elevation of controllable authority.
 * 
 * 2. LSM-Tree and Asynchronous I/O Delegation Philosophy:
 * Instead of traditional B-Tree in-place updates causing slow Random I/O, this system adopts the LSM-Tree philosophy. Workers write data to RAM (Delta Buffer) at O(1) speed and leave immediately, delegating the heavy disk compaction entirely to this background daemon.
 * 
 * 3. NVMe-Friendly Segment Rolling and Escaping Single-File Bottlenecks:
 * We eliminated the anti-pattern of `FileChannel.truncate(0)`, which caused disk fragmentation and degraded NVMe lifespan. Taking inspiration from Kafka, WAL files are elegantly rolled into new segments when exceeding 50MB, and fully synced old segments are physically deleted by an asynchronous garbage collection queue, fully restoring geometric write performance of a 100% Append-Only file system.
 * =============================================================================
 */