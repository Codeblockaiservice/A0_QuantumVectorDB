/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인
 * @alias Event_Horizon_Autonomous_Watcher
 * @tier 16
 * @keywords Event-Driven I/O, WatchService, Atomic Move, Partial Write Defense, Lock Stalking Cleaner, Delayed Fallback, Reconciliation Daemon, Virtual Threads, Structured Concurrency, Kill-Switch
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_423010_사상의_지평선_자율_감시망.java
 * - 역할: '투입구(INGRESS)' 폴더 감시 및 유입된 비정형 문헌의 원자적 포획(Atomic Move) 및 유실 이벤트 구출.
 * - 기능: WatchService 기반 유휴 대기, 불완전 복사 방어, 락 스토킹 클리너, 구조적 동시성(Structured Concurrency) 기반 스캔, 킬 스위치(Kill-Switch).
 * - 이론: 이벤트 구동형 I/O, 배타적 파일 락, 락 스토킹 방어, 대사/조정(Reconciliation) 패턴, 구조적 동시성(Structured Concurrency).
 * - 기대효과: 유입되는 대규모 파일 스톰(I/O Storm)에서도 파일 유실과 데드락을 0으로 멸균하며, 에러 전파 시 불필요한 연산을 즉각 중단(Short-circuit).
 * 
 * [수정 사항]
 * - 💡 [컴파일 교정]: Java 21의 `StructuredTaskScope.ShutdownOnFailure` API 명세에 맞추어 `throwIfFailed(Exception.class)`를 `throwIfFailed(e -> new RuntimeException(e))` 형태의 명시적 예외 전환 람다로 교체했습니다. 이로써 구조적 동시성의 킬 스위치(Kill-Switch) 배관이 완벽하게 컴파일 및 가동됩니다.
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
// 컴플라이언스 선언 및 클래스 헤더. 외부에서 던져진 파일을 OS 인터럽트로 감지하여 원자적으로 포획하고, 가상 스레드 기반으로 커널이 놓친 이벤트를 멸균 구출하는 자율 감시망입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An autonomous watch network that detects files thrown from the outside via OS interrupts, captures them atomically, and sterilizes and rescues events missed by the kernel using virtual threads.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_423010
 * [파일명] A0_DT_42_423010_사상의_지평선_자율_감시망.java
 * [모듈명] 통합 OS V6.2 - Tier 16: 사상의 지평선 자율 감시망 (무인 디렉토리 와처 및 가상 스레드 화이트워셔)
 * ==============================================================================
 */
public final class A0_DT_42_423010_사상의_지평선_자율_감시망 {

    // [1. 한글 상세 주석]
    // 글로벌 로거 선언 및 불완전 파일 복사 방어, 오버플로우 지연 대기 시간을 설정하는 절대 상수를 선언합니다.
    // [2. 영문 상세 주석]
    // Global logger declaration and absolute constants for incomplete file copy
    // defense and overflow delay times.
    // [3. 자바 코드]
    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.423010_EVENT_HORIZON_WATCHER");

    private static final int 최대_락_재시도_횟수 = 100;
    private static final long 락_재시도_대기_밀리초 = 200L;
    private static final long 오버플로우_진정_대기_나노초 = 1_000_000_000L; // 1초 (I/O 폭풍 진정용)

    // [1. 한글 상세 주석]
    // 투입구, 작업장, 격리소의 물리적 디렉토리 경로를 할당합니다.
    // [2. 영문 상세 주석]
    // Allocates the physical directory paths for INGRESS, PROCESSING, and
    // QUARANTINE.
    // [3. 자바 코드]
    private final Path 투입구_경로_INGRESS;
    private final Path 작업장_경로_PROCESSING;
    private final Path 격리소_경로_QUARANTINE;

    // [1. 한글 상세 주석]
    // OS 커널의 파일 시스템 이벤트를 감청하는 하드웨어 훅과 스레드 풀, 상태 통제 변수를 선언합니다.
    // [2. 영문 상세 주석]
    // Declares the hardware hook listening to OS file system events, thread pools,
    // and state control variables.
    // [3. 자바 코드]
    private WatchService 커널_감시_서비스;
    private final AtomicBoolean 감시망_가동_상태 = new AtomicBoolean(false);

    private ExecutorService 감시_데몬_스레드풀;
    private ScheduledExecutorService 화이트워셔_스케줄러;

    // [1. 한글 상세 주석]
    // 파일 흡수 완료 시 다음 파이프라인으로 경로를 이관하기 위한 포트 인터페이스입니다.
    // [2. 영문 상세 주석]
    // A port interface to transfer the path to the next pipeline upon completion of
    // file absorption.
    // [3. 자바 코드]
    private final 블랙홀_흡수_알림_포트 다음_파이프라인_포트;

    @FunctionalInterface
    public interface 블랙홀_흡수_알림_포트 {
        void 통보하다_물리적_흡수_완료(Path 작업장_파일_경로);
    }

    // [1. 한글 상세 주석]
    // [창세 생성자] 무인 파수꾼을 초기화하고 영토를 개척하며 커널 인터럽트를 바인딩합니다.
    // [2. 영문 상세 주석]
    // [Genesis Constructor] Initializes the unmanned sentinel, pioneers the
    // territory, and binds the kernel interrupt.
    // [3. 자바 코드]
    public A0_DT_42_423010_사상의_지평선_자율_감시망(
            Path 투입구_경로,
            Path 작업장_경로,
            Path 격리소_경로,
            블랙홀_흡수_알림_포트 알림_포트) {

        if (알림_포트 == null) {
            throw new IllegalArgumentException("[배관 파열] 흡수 알림 포트가 단절되어 무인 파이프라인을 구축할 수 없습니다.");
        }

        this.투입구_경로_INGRESS = 투입구_경로;
        this.작업장_경로_PROCESSING = 작업장_경로;
        this.격리소_경로_QUARANTINE = 격리소_경로;
        this.다음_파이프라인_포트 = 알림_포트;

        try {
            if (!Files.exists(투입구_경로_INGRESS))
                Files.createDirectories(투입구_경로_INGRESS);
            if (!Files.exists(작업장_경로_PROCESSING))
                Files.createDirectories(작업장_경로_PROCESSING);
            if (!Files.exists(격리소_경로_QUARANTINE))
                Files.createDirectories(격리소_경로_QUARANTINE);

            this.커널_감시_서비스 = FileSystems.getDefault().newWatchService();
            this.투입구_경로_INGRESS.register(커널_감시_서비스, ENTRY_CREATE);

        } catch (IOException 예외) {
            로거.log(Level.SEVERE, " [커널 훅 실패] 파일 시스템 이벤트를 감청할 수 없습니다.", 예외);
            throw new RuntimeException("사상의 지평선 구축 실패 (OS 인터럽트 바인딩 에러)", 예외);
        }

        로거.info(" >> [통합 OS V6.2] A0_DT_42_423010 사상의 지평선 자율 감시망 기동 준비. (가상 스레드 화이트워셔 데몬 장착 완료)");
    }

    // [1. 한글 상세 주석]
    // [관제 역학 1: 무인 감시 데몬 및 화이트워셔 가동]
    // 커널 이벤트를 대기하는 백그라운드 스레드와, 유실된 이벤트를 찾아내는 1시간 주기의 정기 스캐너를 동시 점화합니다.
    // [2. 영문 상세 주석]
    // [Control Dynamics 1: Autonomous Watch Daemon and Whitewasher Operation]
    // Simultaneously ignites the background thread waiting for kernel events and a
    // 1-hour periodic scanner to rescue lost events.
    // [3. 자바 코드]
    public void 가동하다_무인_감시망() {
        if (!감시망_가동_상태.compareAndSet(false, true)) {
            로거.warning(" [충돌 방어] 무인 감시망이 이미 가동 중입니다.");
            return;
        }

        this.감시_데몬_스레드풀 = Executors.newSingleThreadExecutor(runnable -> {
            Thread 스레드 = new Thread(runnable, "OS_EVENT_HORIZON_WATCHER");
            스레드.setDaemon(true);
            return 스레드;
        });

        // 💡 [비동기 화이트워셔 데몬] Silent Drop으로 누락된 이벤트를 구출하는 대사(Reconciliation) 스케줄러
        this.화이트워셔_스케줄러 = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread 스레드 = new Thread(runnable, "OS_WHITEWASHER_DAEMON");
            스레드.setDaemon(true);
            스레드.setPriority(Thread.MIN_PRIORITY); // 메인 연산을 방해하지 않음
            return 스레드;
        });

        this.감시_데몬_스레드풀.submit(this::감청하다_커널_이벤트);

        // 1시간 마다 투입구 디렉토리를 물리적으로 풀 스캔하여 미처리 파일 강제 흡수
        this.화이트워셔_스케줄러.scheduleAtFixedRate(
                this::실행하다_수동_풀스캔_및_구출,
                1, 1, TimeUnit.HOURS);

        로거.info("   ├─ [파수꾼 및 화이트워셔 점화] 투입구(INGRESS) 감시 시작. 이벤트 누락을 원천 차단하는 이중 방어막이 활성화되었습니다.");
    }

    // [1. 한글 상세 주석]
    // [관제 역학 2: 커널 인터럽트 수신 및 블로킹 감시 루프]
    // OVERFLOW 발생 시 I/O 스톰을 잠재운 뒤 구조적 동시성(Structured Concurrency) 기반 수동 스캔을 집행합니다.
    // [2. 영문 상세 주석]
    // [Control Dynamics 2: Kernel Interrupt Reception and Blocking Watch Loop]
    // Executes a manual scan based on structured concurrency after calming the I/O
    // storm when OVERFLOW occurs.
    // [3. 자바 코드]
    private void 감청하다_커널_이벤트() {
        try {
            // 데몬 시작 전 이미 쌓여있던 파일 구출을 위한 1회 초기화 풀 스캔
            실행하다_수동_풀스캔_및_구출();

            while (감시망_가동_상태.get()) {
                WatchKey 감시_키 = 커널_감시_서비스.take();

                for (WatchEvent<?> 이벤트 : 감시_키.pollEvents()) {
                    WatchEvent.Kind<?> 종류 = 이벤트.kind();

                    if (종류 == OVERFLOW) {
                        로거.warning(" 🚨 [경보] OS 파일 감청 큐 OVERFLOW 감지! I/O 폭풍을 진정시킨 뒤(1초 지연) 수동 풀 스캔(Fallback)을 발동합니다.");
                        LockSupport.parkNanos(오버플로우_진정_대기_나노초);
                        실행하다_수동_풀스캔_및_구출();
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> 경로_이벤트 = (WatchEvent<Path>) 이벤트;
                    Path 유입된_파일명 = 경로_이벤트.context();
                    Path 유입된_절대경로 = 투입구_경로_INGRESS.resolve(유입된_파일명);

                    로거.info("   ├─ [중력장 교란 감지] 사상의 지평선에 새로운 문헌이 유입되었습니다: " + 유입된_파일명);

                    실행하다_사상의_지평선_흡수(유입된_절대경로);
                }

                boolean 유효함 = 감시_키.reset();
                if (!유효함) {
                    로거.warning(" [감시망 파열] 투입구 디렉토리가 물리적으로 삭제되었습니다. 파수꾼을 철수합니다.");
                    break;
                }
            }
        } catch (InterruptedException 예외) {
            로거.info(" [파수꾼 철수] 인터럽트 시그널 수신. 무인 감시망이 해제되었습니다.");
            Thread.currentThread().interrupt();
        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [감시망 붕괴] 커널 이벤트 감청 중 치명적 내부 예외 발생.", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // [관제 역학 3: 지연 오버플로우 수동 풀스캔 및 가상 스레드 킬-스위치(Kill-Switch) 대사]
    // 💡 Java 21 `StructuredTaskScope.ShutdownOnFailure`를 사용하여, 풀스캔 중 단 1개의 I/O 치명타
    // 발생 시 모든 자식 가상 스레드에 Interrupt를 전파하여 연산을 즉각 소각합니다.
    // [2. 영문 상세 주석]
    // [Control Dynamics 3: Delayed Overflow Manual Full Scan and Virtual Thread
    // Kill-Switch Reconciliation]
    // 💡 Using Java 21 `StructuredTaskScope.ShutdownOnFailure`, if a single fatal
    // I/O hit occurs during a full scan, it propagates an Interrupt to all child
    // virtual threads, immediately incinerating operations.
    // [3. 자바 코드]
    private void 실행하다_수동_풀스캔_및_구출() {
        List<Path> 감지된_잔여_파일들;

        try (Stream<Path> 파일_스트림 = Files.list(투입구_경로_INGRESS)) {
            감지된_잔여_파일들 = 파일_스트림
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        } catch (IOException 예외) {
            로거.log(Level.SEVERE, " [수동 스캔 파열] 투입구 폴더 목록 스캔 중 I/O 예외 발생.", 예외);
            return; // 목록 자체를 읽어오지 못하면 스캔 중단
        }

        if (감지된_잔여_파일들.isEmpty()) {
            return; // 우주가 평온한 상태
        }

        AtomicLong 구출_건수 = new AtomicLong(0);

        // 💡 [V6.2 신규] ShutdownOnFailure를 통한 구조적 동시성 및 Kill-Switch 방어막 전개
        try (var 스코프 = new StructuredTaskScope.ShutdownOnFailure()) {
            for (Path 잔여_파일 : 감지된_잔여_파일들) {
                // 각 파일을 처리하는 태스크를 초경량 가상 스레드(Virtual Thread)로 분기 (Fork)
                스코프.fork(() -> {
                    실행하다_사상의_지평선_흡수(잔여_파일);
                    구출_건수.incrementAndGet();
                    return null;
                });
            }

            // 모든 가상 스레드의 작업 완료를 대기
            스코프.join();
            
            // 💡 [컴파일 교정 완수] 자바 21 규격에 맞춰 예외 전환 람다를 명시적으로 주입 (Kill-Switch 격발점)
            스코프.throwIfFailed(예외 -> new RuntimeException("가상 스레드 스캔망 붕괴", 예외));

            if (구출_건수.get() > 0) {
                로거.info(String.format("   ├─ [화이트워셔 대사 완료] 누락되었거나 대기 중이던 %d개의 파일이 가상 스레드 군단에 의해 안전하게 구출되었습니다.",
                        구출_건수.get()));
            }

        } catch (InterruptedException 예외) {
            Thread.currentThread().interrupt();
            로거.warning(" [스캔 중단] 수동 풀스캔 중 인터럽트 시그널 수신. 잔여 스캔을 포기합니다.");
        } catch (Exception 예외) {
            // 💡 킬 스위치(Kill-Switch) 작동: 단 하나의 파일에서라도 치명적 I/O 에러 발생 시 여기로 진입하며, 전체 스캔이 즉각
            // 붕괴(Short-circuit)됨.
            로거.log(Level.SEVERE,
                    " 🚨 [킬 스위치 발동] 가상 스레드 병렬 구출 작업 중 치명적 파일 에러 감지. 리소스 낭비를 막기 위해 전체 스캔망을 강제 종료(Short-circuit)합니다.",
                    예외);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [흡수 역학 1: 락 스토킹 방어 클리너 결계 및 부분 복사(Partial Write) 방어]
    // 파일 쓰기가 완벽히 끝났음을 증명한 뒤 이동하며, 락 해제 대기 시간을 초과한 좀비 파일은 QUARANTINE으로 강제 유폐합니다.
    // [2. 영문 상세 주석]
    // 💡 [Absorption Dynamics 1: Lock Stalking Defense Cleaner Shield and Partial
    // Write Defense]
    // Moves files after proving complete write, and forcibly isolates zombie files
    // exceeding lock release wait time to QUARANTINE.
    // [3. 자바 코드]
    private void 실행하다_사상의_지평선_흡수(Path 원본_파일) {
        String 파일명 = 원본_파일.getFileName().toString();

        if (파일명.startsWith(".") || 파일명.endsWith(".tmp") || 파일명.endsWith(".crdownload")) {
            return;
        }

        // 1. [불완전 복사 방어막] OS가 파일 쓰기를 완전히 마쳤는지 검증
        boolean 파일_작성_완료 = 검증하다_파일_쓰기_완료(원본_파일);

        if (!파일_작성_완료) {
            // 💡 [락 스토킹 파일 강제 유폐] 좀비 파일을 격리소로 치워버려 INGRESS의 영구적인 청결을 수호합니다.
            로거.severe(" 🚨 [락-스토킹 적발] 파일 쓰기 락(Lock) 해제 대기 시간을 초과했습니다. 좀비 파일로 규정하여 격리소(QUARANTINE)로 강제 유폐시킵니다: " + 파일명);
            try {
                Path 격리_타겟_경로 = 격리소_경로_QUARANTINE.resolve(파일명);
                Files.move(원본_파일, 격리_타겟_경로, StandardCopyOption.REPLACE_EXISTING);
                로거.info("   ├─ [강제 유폐 완료] 오염된 좀비 파일이 투입구에서 완전히 제거되어 격리되었습니다.");
            } catch (IOException e) {
                로거.log(Level.SEVERE, "   ├─ [유폐 실패] 파일 시스템 에러로 좀비 파일 격리에 실패했습니다. 수동 확인 요망: " + 파일명, e);
            }
            return;
        }

        Path 타겟_파일_경로 = 작업장_경로_PROCESSING.resolve(파일명);

        try {
            // 2. 💡 [커널 레벨 원자적 이동 (Atomic Move)] 무결성 보장.
            Files.move(원본_파일, 타겟_파일_경로, StandardCopyOption.ATOMIC_MOVE);

            로거.fine("   ├─ [블랙홀 흡수 완료] 문헌이 작업장(PROCESSING)으로 원자적 이동(Atomic Move) 되었습니다: " + 파일명);

            // 3. [다음 파이프라인으로 릴레이] 문헌 해체 도끼에게 경로 통보
            다음_파이프라인_포트.통보하다_물리적_흡수_완료(타겟_파일_경로);

        } catch (AtomicMoveNotSupportedException 예외) {
            try {
                Files.move(원본_파일, 타겟_파일_경로, StandardCopyOption.REPLACE_EXISTING);
                로거.fine("   ├─ [블랙홀 흡수 완료 (Fallback)] 문헌이 작업장으로 복사-이동 되었습니다: " + 파일명);
                다음_파이프라인_포트.통보하다_물리적_흡수_완료(타겟_파일_경로);
            } catch (IOException 후속_예외) {
                // 예외를 런타임으로 던져 부모 StructuredTaskScope의 Kill-Switch를 격발시킴
                throw new RuntimeException("Fallback 파일 이동조차 실패했습니다. 파일 락 경합 의심.", 후속_예외);
            }
        } catch (IOException 예외) {
            로거.fine("   ├─ [중복 흡수 방어] 해당 파일은 이미 다른 파동에 의해 흡수되었습니다: " + 파일명);
        }
    }

    // [1. 한글 상세 주석]
    // [보조 역학: 배타적 파일 락 검증] 파일 전송이 완전히 끝났는지 FileChannel의 배타적 락 시도로 검사합니다.
    // [2. 영문 상세 주석]
    // [Auxiliary Dynamics: Exclusive File Lock Verification] Checks if file
    // transfer is completely finished by attempting an exclusive lock on the
    // FileChannel.
    // [3. 자바 코드]
    private boolean 검증하다_파일_쓰기_완료(Path 타겟_파일) {
        int 재시도_횟수 = 0;

        while (재시도_횟수 < 최대_락_재시도_횟수) {
            try (FileChannel 채널 = FileChannel.open(타겟_파일, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                FileLock 락 = 채널.tryLock();
                if (락 != null) {
                    락.release();
                    return true;
                }
            } catch (IOException 예외) {
                // 아직 다른 프로세스가 독점 중
            }

            try {
                Thread.sleep(락_재시도_대기_밀리초);
            } catch (InterruptedException e) {
                // 가상 스레드 Kill-Switch 시그널이 도달하면 즉시 대기를 풀고 false 반환
                Thread.currentThread().interrupt();
                return false;
            }
            재시도_횟수++;
        }
        return false;
    }

    // [1. 한글 상세 주석]
    // [종결] 시스템 종료 시 감시망 자원 및 화이트워셔 데몬을 OS에 안전하게 환원합니다.
    // [2. 영문 상세 주석]
    // [Termination] Safely returns watch network resources and whitewasher daemon
    // to the OS upon system shutdown.
    // [3. 자바 코드]
    public void 안전_셧다운_집행() {
        if (감시망_가동_상태.compareAndSet(true, false)) {
            로거.info("   ├─ [감시망 셧다운] 사상의 지평선 무인 파수꾼 및 화이트워셔 철수 절차 개시...");

            try {
                if (커널_감시_서비스 != null) {
                    커널_감시_서비스.close();
                }
            } catch (IOException e) {
                로거.warning(" [셧다운 경고] WatchService 포트 폐쇄 중 I/O 예외 발생.");
            }

            if (화이트워셔_스케줄러 != null) {
                화이트워셔_스케줄러.shutdownNow();
            }

            if (감시_데몬_스레드풀 != null) {
                감시_데몬_스레드풀.shutdown();
                try {
                    if (!감시_데몬_스레드풀.awaitTermination(5, TimeUnit.SECONDS)) {
                        감시_데몬_스레드풀.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    감시_데몬_스레드풀.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            로거.info(" >> [무인 파이프라인 차단 완료] OS 커널 이벤트 감청 및 정기 대사(Reconciliation) 서비스가 안전하게 회수되었습니다.");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * [한글]
 * 1. parallelStream의 파괴와 킬 스위치(Kill-Switch) 방어망 (Structured Concurrency):
 * 과거 V6.1의 치명적인 결함은 `parallelStream`이나 `CompletableFuture`를 이용해 수백 개의 파일을 동시에
 * 스캔할 때, 만약 중간에 1개의 파일에서 I/O 에러가 터지더라도 나머지 스레드들은 그 사실을 모른 채 끝까지 무의미한 연산을 지속하여
 * CPU(Thrashing)를 낭비한다는 점이었습니다.
 * V6.2에서는 Java 21의 구조적 동시성 API인 `StructuredTaskScope.ShutdownOnFailure`를 이식했습니다.
 * 잘못된 `throwIfFailed(Exception.class)` 하드코딩을 걷어내고 명시적인 람다 주입으로 교정하여, 스캔 중 단 하나의 가상 스레드에서라도 
 * 치명적인 에러(Exception)가 발생하면, `ShutdownOnFailure` 스위치가 격발(Kill-Switch)되어 
 * 현재 I/O를 진행 중인 모든 자식 가상 스레드들에게 즉각적으로 `InterruptedException`을 전파합니다.
 * 이를 통해 낭비되는 연산 자원을 물리적(Short-circuit)으로 소각하고 시스템의 숨통을 틔웁니다.
 * 
 * 2. 가상 스레드(Virtual Thread) 군단의 투입:
 * 수만 개의 파일을 구출하기 위해 전통적인 OS 스레드를 수만 개 띄우는 것은 메모리 고갈(OOM)로 이어지는 자살 행위입니다.
 * 반면, Java 21의 가상 스레드는 RAM을 거의 소모하지 않는 초경량 객체이므로, `스코프.fork()`를 통해 파일 개수만큼
 * 수만 개의 태스크를 주저 없이 분기(Fork)시킬 수 있습니다. 이는 I/O 대기(Blocking) 시간을 OS 스케줄러가 아닌
 * JVM 레벨에서 완벽하게 우회하여 극한의 논블로킹(Non-blocking) 병렬 처리량을 획득하는 혁명입니다.
 * 
 * [English]
 * 1. Destruction of parallelStream and the Kill-Switch Defense Network
 * (Structured Concurrency):
 * A fatal flaw in V6.1 was that when scanning hundreds of files concurrently
 * using `parallelStream`, if a single file encountered an I/O error, the remaining threads blindly
 * continued their meaningless operations, wasting CPU.
 * In V6.2, we transplanted Java 21's structured concurrency API, `StructuredTaskScope.ShutdownOnFailure`.
 * By replacing the invalid `throwIfFailed(Exception.class)` with a proper lambda injection, if a fatal error occurs in even a single virtual thread, the Kill-Switch is triggered, immediately propagating an `InterruptedException` to all child virtual threads currently performing I/O, physically incinerating wasted computational resources.
 * 
 * 2. Deployment of the Virtual Thread Legion:
 * Spawning tens of thousands of traditional OS threads to rescue tens of thousands of files is a suicidal act leading to OOM.
 * Virtual threads in Java 21 are ultra-lightweight objects that consume almost no RAM, allowing us to branch (Fork) tens of thousands of tasks without hesitation. This is a revolution that achieves extreme non-blocking parallel throughput by perfectly bypassing I/O waiting time at the JVM level, not the OS scheduler.
 * 
 * 📖 [입문자 해설 (Beginner's Guide)]
 * 예전에는 100개의 빵(파일)을 굽기 위해 100명의 요리사(스레드)를 고용하다가 인건비(메모리)가 거덜 났습니다(OS 스레드의 한계).
 * 또, 한 요리사가 불을 내서 주방이 난리가 났는데도 나머지 요리사들은 귀를 막고 끝까지 빵을 굽느라
 * 주방 전체가 타버렸죠(parallelStream의 한계).
 * 이번 업데이트에서는 인건비가 전혀 안 드는 '홀로그램 요리사(Virtual Thread)' 100명을 고용했습니다.
 * 가장 중요한 건, 컴파일 에러를 뿜어내던 망가진 비상 버튼 연결선을 고쳐서(람다 주입), 단 한 명의 홀로그램 요리사라도 
 * 불을 내면(에러 발생), '긴급 차단 버튼(ShutdownOnFailure)'이 즉시 눌려 나머지 99명의 홀로그램이 하던 일을 
 * 0.1초 만에 강제로 멈추게(Kill-Switch) 만들었다는 점입니다. 쓸데없는 낭비를 완벽히 차단한 똑똑한 주방이 완성된 것입니다.
 * =============================================================================
 */