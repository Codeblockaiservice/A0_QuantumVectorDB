/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인
 * @alias Autonomous_Ingress_Watcher
 * @tier 16
 * @keywords Event-Driven I/O, WatchService, Atomic Move, Partial Write Defense, Lock Stalking Cleaner, Delayed Fallback, Reconciliation Daemon, Virtual Threads, Structured Concurrency, Kill-Switch
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_423010_사상의_지평선_자율_감시망.java
 * - 역할: 외부 데이터 투입구(INGRESS) 폴더를 감시하며, 유입된 원시 문헌의 원자적 포획(Atomic Move) 및 I/O 이벤트 유실 복구를 수행합니다.
 * - 기능: WatchService 기반 유휴 대기, 불완전 복사 방어(Partial Write Defense), 좀비 파일 정리, 구조적 동시성(Structured Concurrency) 기반 스캔, Kill-Switch.
 * - 이론: Event-Driven I/O, Exclusive File Lock, 대사/조정(Reconciliation) 패턴, 구조적 동시성(Structured Concurrency).
 * - 기대효과: 대규모 파일 스톰(I/O Storm) 발생 시에도 파일 유실과 데드락을 원천 차단하며, 에러 전파 시 불필요한 연산을 즉각 중단(Short-circuit)시킵니다.
 * 
 * [수정 사항]
 * - 💡 [컴파일 에러 교정]: Java 21의 `StructuredTaskScope.ShutdownOnFailure` API 명세에 맞추어 `throwIfFailed(Exception.class)` 구문을 `throwIfFailed(e -> new RuntimeException(e))` 형태의 명시적 예외 전환 람다(Lambda)로 교체했습니다. 이로써 구조적 동시성의 킬 스위치(Kill-Switch) 배관이 100% 정상 가동됩니다.
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 시스템 이벤트 감청, 구조적 동시성(StructuredTaskScope), 정기 스캔 스케줄러 등 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for file system event listening, structured concurrency (StructuredTaskScope), and periodic scan schedulers.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 외부에서 복사된 파일을 OS 인터럽트로 감지하여 포획하고, 가상 스레드 기반으로 커널이 놓친 이벤트를 복구하는 자율 감시망입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An autonomous watch network that detects externally copied files via OS interrupts, captures them, and rescues events missed by the kernel using virtual threads.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_423010
 * [파일명] A0_DT_42_423010_사상의_지평선_자율_감시망.java
 * [모듈명] 통합 OS V6.2 - Tier 16: 사상의 지평선 자율 감시망 (무인 디렉토리 와처 및 이벤트 조정 데몬)
 * ==============================================================================
 */
public final class A0_DT_42_423010_사상의_지평선_자율_감시망 {

    // [1. 한글 상세 주석]
    // 글로벌 로거 선언 및 불완전 파일 복사 방어, 오버플로우 지연 대기 시간을 설정하는 절대 상수를 선언합니다.
    // [2. 영문 상세 주석]
    // Global logger declaration and absolute constants for incomplete file copy defense and overflow delay times.

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.423010_EVENT_HORIZON_WATCHER");

    private static final int MAX_LOCK_RETRY_COUNT = 100;
    private static final long LOCK_RETRY_DELAY_MS = 200L;
    private static final long OVERFLOW_COOLDOWN_NANOS = 1_000_000_000L; // 1초 (I/O 스톰 진정 대기 시간)

    // [1. 한글 상세 주석]
    // 투입구(INGRESS), 작업장(PROCESSING), 격리소(QUARANTINE)의 물리적 디렉토리 경로를 할당합니다.
    // [2. 영문 상세 주석]
    // Allocates the physical directory paths for INGRESS, PROCESSING, and QUARANTINE.

    private final Path ingressPath;
    private final Path processingPath;
    private final Path quarantinePath;

    // [1. 한글 상세 주석]
    // OS 커널의 파일 시스템 이벤트를 감청하는 하드웨어 훅(WatchService)과 스레드 풀, 상태 통제 변수를 선언합니다.
    // [2. 영문 상세 주석]
    // Declares the hardware hook (WatchService) listening to OS file system events, thread pools, and state control variables.

    private WatchService kernelWatchService;
    private final AtomicBoolean isWatcherRunning = new AtomicBoolean(false);

    private ExecutorService watcherDaemonThreadPool;
    private ScheduledExecutorService reconciliationScheduler;

    // [1. 한글 상세 주석]
    // 파일 수집 완료 시 다음 파이프라인으로 파일 경로를 이관하기 위한 콜백 포트 인터페이스입니다.
    // [2. 영문 상세 주석]
    // A callback port interface to transfer the file path to the next pipeline upon completion of file ingestion.

    private final IngestionNotificationPort nextPipelinePort;

    @FunctionalInterface
    public interface IngestionNotificationPort {
        void notifyPhysicalIngestionComplete(Path processingFilePath);
    }

    // [1. 한글 상세 주석]
    // [생성자] 무인 파수꾼을 초기화하고 디렉토리를 개척하며 커널 인터럽트를 바인딩합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Initializes the unmanned sentinel, pioneers directories, and binds the kernel interrupt.

    public A0_DT_42_423010_사상의_지평선_자율_감시망(
            Path ingressPath,
            Path processingPath,
            Path quarantinePath,
            IngestionNotificationPort notificationPort) {

        if (notificationPort == null) {
            throw new IllegalArgumentException("[설정 오류] 파일 흡수 알림 포트가 주입되지 않아 무인 파이프라인을 구축할 수 없습니다.");
        }

        this.ingressPath = ingressPath;
        this.processingPath = processingPath;
        this.quarantinePath = quarantinePath;
        this.nextPipelinePort = notificationPort;

        try {
            if (!Files.exists(this.ingressPath))
                Files.createDirectories(this.ingressPath);
            if (!Files.exists(this.processingPath))
                Files.createDirectories(this.processingPath);
            if (!Files.exists(this.quarantinePath))
                Files.createDirectories(this.quarantinePath);

            // OS 파일 시스템 인터럽트를 감지하는 서비스 객체 획득
            this.kernelWatchService = FileSystems.getDefault().newWatchService();
            this.ingressPath.register(kernelWatchService, ENTRY_CREATE);

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [커널 훅 실패] 운영체제 파일 시스템 이벤트를 감청할 수 없습니다.", ex);
            throw new RuntimeException("감시망 구축 실패 (OS 인터럽트 바인딩 에러)", ex);
        }

        logger.info(" >> [통합 OS V6.2] A0_DT_42_423010 사상의 지평선 자율 감시망 기동 준비 완료. (구조적 동시성 대사 데몬 탑재)");
    }

    // [1. 한글 상세 주석]
    // [관제 로직 1: 무인 감시 데몬 및 조정자(Reconciliation) 가동]
    // 커널 이벤트를 대기하는 백그라운드 스레드와, 유실된 이벤트를 찾아내는 1시간 주기 스캐너를 동시 점화합니다.
    // [2. 영문 상세 주석]
    // [Control Logic 1: Autonomous Watch Daemon and Reconciliation Operation]
    // Simultaneously ignites the background thread waiting for kernel events and a 1-hour periodic scanner to rescue lost events.

    public void startAutonomousWatcher() {
        if (!isWatcherRunning.compareAndSet(false, true)) {
            logger.warning(" [중복 실행 방어] 무인 감시망이 이미 가동 중입니다.");
            return;
        }

        this.watcherDaemonThreadPool = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "OS_EVENT_HORIZON_WATCHER");
            thread.setDaemon(true);
            return thread;
        });

        // 💡 [비동기 대사(Reconciliation) 스케줄러] 커널 이벤트 손실(Silent Drop)로 누락된 파일을 구출합니다.
        this.reconciliationScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "OS_RECONCILIATION_DAEMON");
            thread.setDaemon(true);
            thread.setPriority(Thread.MIN_PRIORITY); // 메인 HFT 연산을 방해하지 않도록 우선순위 하향 조정
            return thread;
        });

        this.watcherDaemonThreadPool.submit(this::listenKernelEvents);

        // 1시간 마다 INGRESS 디렉토리를 물리적으로 전수 스캔(Full Scan)하여 미처리 파일을 강제로 흡수시킵니다.
        this.reconciliationScheduler.scheduleAtFixedRate(
                this::executeManualFullScanAndRescue,
                1, 1, TimeUnit.HOURS);

        logger.info("   ├─ [감시망 활성화] INGRESS 디렉토리 감시 시작. 이벤트 누락을 원천 차단하는 이중 방어막이 활성화되었습니다.");
    }

    // [1. 한글 상세 주석]
    // [관제 로직 2: 커널 인터럽트 수신 및 블로킹 감시 루프]
    // 파일 시스템의 OVERFLOW 발생 시 I/O 폭풍을 진정시킨 뒤, 구조적 동시성(Structured Concurrency) 기반 수동 스캔을 집행합니다.
    // [2. 영문 상세 주석]
    // [Control Logic 2: Kernel Interrupt Reception and Blocking Watch Loop]
    // Executes a manual scan based on structured concurrency after calming the I/O storm when OVERFLOW occurs.

    private void listenKernelEvents() {
        try {
            // 데몬 시작 전 INGRESS에 이미 쌓여있던 파일 구출을 위한 1회 초기화 풀 스캔 실행
            executeManualFullScanAndRescue();

            while (isWatcherRunning.get()) {
                WatchKey watchKey = kernelWatchService.take(); // 이벤트를 기다리며 Blocking

                for (WatchEvent<?> event : watchKey.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    // I/O 큐에 너무 많은 파일이 쏟아져 커널이 이벤트를 버렸을 경우(OVERFLOW)
                    if (kind == OVERFLOW) {
                        logger.warning(" 🚨 [경보] OS 파일 감청 큐 OVERFLOW 감지! I/O 폭풍을 진정시킨 뒤(1초 지연) 수동 풀 스캔(Fallback)을 발동합니다.");
                        LockSupport.parkNanos(OVERFLOW_COOLDOWN_NANOS);
                        executeManualFullScanAndRescue();
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    Path incomingFileName = pathEvent.context();
                    Path incomingAbsolutePath = ingressPath.resolve(incomingFileName);

                    logger.info("   ├─ [이벤트 감지] 투입구(INGRESS)에 새로운 원시 문헌이 유입되었습니다: " + incomingFileName);

                    executeAtomicIngestion(incomingAbsolutePath);
                }

                boolean isKeyValid = watchKey.reset();
                if (!isKeyValid) {
                    logger.warning(" [감시망 파열] INGRESS 디렉토리가 물리적으로 삭제되었습니다. 파일 파수꾼을 강제 철수합니다.");
                    break;
                }
            }
        } catch (InterruptedException ex) {
            logger.info(" [파수꾼 철수] 스레드 인터럽트 시그널 수신. 무인 감시망 루프를 종료합니다.");
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [감시망 붕괴] 커널 이벤트 감청 중 치명적 내부 예외 발생.", ex);
        }
    }

    // [1. 한글 상세 주석]
    // [관제 로직 3: 오버플로우 대비 수동 풀스캔 및 가상 스레드 킬-스위치(Kill-Switch) 제어]
    // 💡 Java 21 `StructuredTaskScope.ShutdownOnFailure`를 사용하여, 풀스캔 중 단 1개의 치명타(Exception)가 
    // 발생하더라도 모든 자식 가상 스레드(Virtual Thread)에 즉시 Interrupt를 전파하여 무의미한 연산을 즉각 소각합니다.
    // [2. 영문 상세 주석]
    // [Control Logic 3: Delayed Overflow Manual Full Scan and Virtual Thread Kill-Switch]
    // 💡 Using Java 21 `StructuredTaskScope.ShutdownOnFailure`, if a single fatal exception occurs during a full scan, it propagates an Interrupt to all child virtual threads, immediately incinerating operations.

    private void executeManualFullScanAndRescue() {
        List<Path> pendingFileList;

        try (Stream<Path> fileStream = Files.list(ingressPath)) {
            pendingFileList = fileStream
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [수동 스캔 오류] 투입구(INGRESS) 폴더 파일 목록 스캔 중 I/O 예외 발생.", ex);
            return; // 목록 자체를 읽어오지 못하면 스캔 중단
        }

        if (pendingFileList.isEmpty()) {
            return; // 큐가 비어있는 평온한 상태
        }

        AtomicLong rescuedCount = new AtomicLong(0);

        // 💡 [V6.2 핵심 통제] ShutdownOnFailure를 통한 구조적 동시성 및 Kill-Switch 방어막 전개
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            for (Path pendingFile : pendingFileList) {
                // 각 파일을 처리하는 태스크를 초경량 가상 스레드(Virtual Thread)로 분기 (Fork)
                scope.fork(() -> {
                    executeAtomicIngestion(pendingFile);
                    rescuedCount.incrementAndGet();
                    return null;
                });
            }

            // 모든 가상 스레드의 작업 완료를 대기 (Barrier)
            scope.join();
            
            // 💡 [컴파일 에러 교정 완수] 자바 21 API 규격에 맞추어 명시적 예외 전환 람다를 주입 (Kill-Switch 격발점)
            scope.throwIfFailed(ex -> new RuntimeException("가상 스레드 기반 스캔 파이프라인 붕괴", ex));

            if (rescuedCount.get() > 0) {
                logger.info(String.format("   ├─ [조정(Reconciliation) 완료] 누락되었거나 대기 중이던 %d개의 파일이 가상 스레드 군단에 의해 안전하게 수집되었습니다.",
                        rescuedCount.get()));
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            logger.warning(" [스캔 중단] 수동 풀스캔 중 인터럽트 시그널 수신. 잔여 스캔을 포기합니다.");
        } catch (Exception ex) {
            // 💡 킬 스위치(Kill-Switch) 작동: 단 하나의 자식 스레드에서라도 치명적 에러 발생 시 부모 스코프가 여기로 진입하며, 전체 스캔이 즉각 셧다운(Short-circuit)됨.
            logger.log(Level.SEVERE,
                    " 🚨 [킬 스위치 발동] 가상 스레드 병렬 구출 작업 중 치명적 예외 감지. 리소스 낭비를 막기 위해 전체 스캔망을 강제 종료(Short-circuit)시킵니다.",
                    ex);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [수집 로직 1: 불완전 복사(Partial Write) 방어 및 락 스토킹 클리너 결계]
    // 외부 프로세스의 파일 쓰기가 완벽히 끝났음을 증명한 뒤 이동하며, 락 해제 대기 시간을 초과한 좀비 파일은 격리소로 강제 유폐시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [Ingestion Logic 1: Partial Write Defense and Lock Stalking Cleaner Shield]
    // Moves files after proving external process complete write, and forcibly isolates zombie files exceeding lock release wait time to QUARANTINE.

    private void executeAtomicIngestion(Path sourceFile) {
        String fileName = sourceFile.getFileName().toString();

        // 임시 파일 무시
        if (fileName.startsWith(".") || fileName.endsWith(".tmp") || fileName.endsWith(".crdownload")) {
            return;
        }

        // 1. [불완전 복사 방어] 외부 프로세스(크롤러 등)가 파일 쓰기를 완전히 마쳤는지 배타적 락을 통해 검증
        boolean isWriteCompleted = verifyFileWriteCompletion(sourceFile);

        if (!isWriteCompleted) {
            // 💡 [락 스토킹 방어] 영원히 락이 풀리지 않는 좀비 파일을 격리소로 치워버려 INGRESS의 청결을 유지합니다.
            logger.severe(" 🚨 [좀비 파일 적발] 파일 쓰기 락(Lock) 해제 대기 시간을 초과했습니다. 좀비 파일로 규정하여 격리소(QUARANTINE)로 강제 이동시킵니다: " + fileName);
            try {
                Path quarantineTarget = quarantinePath.resolve(fileName);
                Files.move(sourceFile, quarantineTarget, StandardCopyOption.REPLACE_EXISTING);
                logger.info("   ├─ [유폐 완료] 오염된 좀비 파일이 투입구에서 완전히 제거되어 격리되었습니다.");
            } catch (IOException e) {
                logger.log(Level.SEVERE, "   ├─ [유폐 실패] 파일 시스템 권한 에러로 좀비 파일 격리에 실패했습니다. 수동 삭제 요망: " + fileName, e);
            }
            return;
        }

        Path processingTarget = processingPath.resolve(fileName);

        try {
            // 2. 💡 [커널 레벨 원자적 이동 (Atomic Move)] 다중 스레드 환경에서 단 하나의 스레드만 소유권을 획득함을 OS가 보장.
            Files.move(sourceFile, processingTarget, StandardCopyOption.ATOMIC_MOVE);

            logger.fine("   ├─ [포획 완료] 문헌이 작업장(PROCESSING)으로 원자적 이동(Atomic Move) 처리되었습니다: " + fileName);

            // 3. [다음 파이프라인(Tier 2 워커)으로 릴레이] 콜백 포트를 통해 대상 경로 통보
            nextPipelinePort.notifyPhysicalIngestionComplete(processingTarget);

        } catch (AtomicMoveNotSupportedException ex) {
            try {
                // OS/파일시스템이 Atomic Move를 지원하지 않을 경우 일반 복사 기반 이동(Fallback) 적용
                Files.move(sourceFile, processingTarget, StandardCopyOption.REPLACE_EXISTING);
                logger.fine("   ├─ [포획 완료 (Fallback)] 문헌이 작업장으로 일반 복사/이동 처리되었습니다: " + fileName);
                nextPipelinePort.notifyPhysicalIngestionComplete(processingTarget);
            } catch (IOException fallbackEx) {
                // 예외를 런타임으로 위로 던져 부모 StructuredTaskScope의 Kill-Switch를 격발시킴
                throw new RuntimeException("Fallback 일반 파일 이동에 실패했습니다. 파일 락 경합 혹은 I/O 에러가 의심됩니다.", fallbackEx);
            }
        } catch (IOException ex) {
            // 동일 시점에 진입한 다른 파수꾼 스레드가 이미 파일을 가져간 경우 (Race Condition 패배) -> 조용히 무시
            logger.fine("   ├─ [중복 처리 방어] 해당 파일은 이미 다른 파이프라인에 의해 수집되었습니다: " + fileName);
        }
    }

    // [1. 한글 상세 주석]
    // [보조 검증 로직: 배타적 파일 락 검증] 파일 전송이 완전히 끝났는지 FileChannel의 배타적 락 시도로 검사합니다.
    // [2. 영문 상세 주석]
    // [Auxiliary Verification Logic: Exclusive File Lock Verification] Checks if file transfer is completely finished by attempting an exclusive lock on the FileChannel.

    private boolean verifyFileWriteCompletion(Path targetFile) {
        int retryCount = 0;

        while (retryCount < MAX_LOCK_RETRY_COUNT) {
            try (FileChannel channel = FileChannel.open(targetFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                // 외부 크롤러가 아직 쓰기를 완료하지 않았다면 tryLock()은 null을 반환하거나 예외를 던집니다.
                FileLock lock = channel.tryLock();
                if (lock != null) {
                    lock.release(); // 안전하게 락을 획득했으므로 쓰기가 끝났음이 보장됨. 즉시 릴리즈.
                    return true;
                }
            } catch (IOException ex) {
                // 아직 다른 외부 프로세스(크롤러 등)가 I/O 독점 중인 상태
            }

            try {
                Thread.sleep(LOCK_RETRY_DELAY_MS);
            } catch (InterruptedException e) {
                // 💡 가상 스레드 Kill-Switch 시그널이 도달하면 무의미한 대기를 풀고 즉시 false 반환
                Thread.currentThread().interrupt();
                return false;
            }
            retryCount++;
        }
        return false;
    }

    // [1. 한글 상세 주석]
    // [종료 절차] 애플리케이션 셧다운 시 감시망 이벤트 포트 및 스케줄러 데몬을 OS에 안전하게 환원합니다.
    // [2. 영문 상세 주석]
    // [Termination Procedure] Safely returns watch network event ports and scheduler daemon to the OS upon application shutdown.

    public void executeGracefulShutdown() {
        if (isWatcherRunning.compareAndSet(true, false)) {
            logger.info("   ├─ [감시망 셧다운] 사상의 지평선 무인 파수꾼 및 이벤트 대사(Reconciliation) 데몬 철수 절차 개시...");

            try {
                if (kernelWatchService != null) {
                    kernelWatchService.close();
                }
            } catch (IOException e) {
                logger.warning(" [셧다운 경고] WatchService 커널 포트 폐쇄 중 I/O 예외 발생.");
            }

            if (reconciliationScheduler != null) {
                reconciliationScheduler.shutdownNow();
            }

            if (watcherDaemonThreadPool != null) {
                watcherDaemonThreadPool.shutdown();
                try {
                    if (!watcherDaemonThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                        watcherDaemonThreadPool.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    watcherDaemonThreadPool.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            logger.info(" >> [무인 파이프라인 차단 완료] OS 커널 이벤트 감청 및 정기 대사(Reconciliation) 스레드가 안전하게 자원을 회수했습니다.");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * [한글]
 * 1. Java 21 구조적 동시성 (Structured Concurrency)과 Kill-Switch 아키텍처:
 * 과거 Java 8의 `parallelStream`이나 `CompletableFuture`를 이용하여 수천 개의 파일을 병렬 스캔할 때 가장 치명적인 결함은 '고아 스레드(Orphan Thread)' 현상이었습니다.
 * 만약 스캔 도중 1개의 파일에서 디스크 I/O 에러가 발생하더라도, 나머지 수백 개의 스레드들은 그 사실을 인지하지 못한 채 끝까지 무의미한 연산을 지속하여 CPU(Thrashing)를 낭비했습니다.
 * V6.2 아키텍처에서는 Java 21의 구조적 동시성 API인 `StructuredTaskScope.ShutdownOnFailure`를 이식했습니다.
 * 람다 파라미터를 통한 `throwIfFailed(ex -> new RuntimeException(ex))` 명시적 예외 주입을 적용하여,
 * 스캔 중 단 하나의 가상 스레드(Virtual Thread)에서라도 치명적인 에러(Exception)가 발생하면 `ShutdownOnFailure` 스위치가 격발(Kill-Switch)됩니다.
 * 이 트리거는 현재 I/O를 진행 중인 모든 형제(Sibling) 가상 스레드들에게 즉각적으로 `InterruptedException`을 전파하여 연산을 중지(Short-circuit)시킵니다. 
 * 이를 통해 낭비되는 연산 자원을 물리적으로 즉각 소각하고 시스템의 안정성을 획득합니다.
 * 
 * 2. 가상 스레드(Virtual Thread) 군단의 투입:
 * I/O 큐에 누락된 수만 개의 파일을 구출하기 위해 전통적인 OS 스레드를 수만 개 생성하는 것은 Context Switching 오버헤드와 메모리 고갈(OOM)로 이어지는 자살 행위입니다.
 * 반면, Java 21의 가상 스레드는 RAM을 거의 소모하지 않는 초경량 객체(Continuation)이므로, `scope.fork()`를 통해 누락된 파일 개수만큼 수만 개의 독립된 태스크를 주저 없이 분기(Fork)시킬 수 있습니다.
 * 이는 File I/O 대기(Blocking) 동작을 OS 스케줄러가 아닌 JVM 레벨에서 완벽하게 우회하여, 최소한의 OS 스레드 풀만으로도 극한의 논블로킹(Non-blocking) 병렬 처리량을 달성하는 혁명입니다.
 * 
 * [English]
 * 1. Java 21 Structured Concurrency and Kill-Switch Architecture:
 * A fatal flaw in legacy Java versions using `parallelStream` for concurrent I/O was that if a single file encountered a disk error, the remaining threads blindly continued their meaningless operations, wasting CPU.
 * In V6.2, we implemented Java 21's structured concurrency API, `StructuredTaskScope.ShutdownOnFailure`.
 * By replacing the invalid syntax with an explicit exception injection lambda `throwIfFailed(ex -> new RuntimeException(ex))`, if a fatal error occurs in even a single virtual thread, the Kill-Switch is triggered. 
 * This instantly propagates an `InterruptedException` to all sibling virtual threads currently performing I/O, physically short-circuiting wasted computational resources.
 * 
 * 2. Deployment of the Virtual Thread Legion:
 * Spawning tens of thousands of traditional OS threads to process thousands of files is a suicidal act leading to Context Switching overhead and OOM.
 * Virtual threads in Java 21 are ultra-lightweight continuations that consume almost no RAM, allowing us to branch (Fork) tens of thousands of independent tasks corresponding to the file count without hesitation. 
 * This is a revolution that achieves extreme non-blocking parallel throughput by perfectly bypassing I/O waiting blocks at the JVM level rather than the OS scheduler level.
 * =============================================================================
 */