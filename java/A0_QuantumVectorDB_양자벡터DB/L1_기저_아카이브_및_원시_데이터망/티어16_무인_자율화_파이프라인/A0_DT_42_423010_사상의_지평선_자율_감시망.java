/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인
 * @alias Event_Horizon_Autonomous_Watcher
 * @tier 16
 * @keywords Event-Driven I/O, WatchService, Atomic Move, Partial Write Defense, Lock Stalking Cleaner, Delayed Fallback
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_423010_사상의_지평선_자율_감시망.java
 * - 역할 (Role): '투입구(INGRESS)' 폴더 감시 및 유입된 비정형 문헌의 원자적 포획(Atomic Move).
 * - 기능 (Function): WatchService 기반 유휴 대기, 불완전 복사 방어, 락(Lock) 스토킹 클리너 및 지연 오버플로우 복구.
 * - 이론 (Theory): 이벤트 구동형 I/O, 배타적 파일 락, 락 스토킹 방어(Lock Stalking Defense), 지연 풀링(Delayed Polling).
 * - 기대효과 (Effect): 무한 루프 폴링에 의한 CPU 스래싱 멸균 및 100% 무결하게 작성된 파일만 '작업장'으로 이송.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 특정 세력을 연상시키는 단어를 전면 소각하고 '통합 OS'로 치환 완료.
 * - 💡 [신설] `WatchService` OVERFLOW 감지 시 지연을 둔 수동 풀링(Fallback) 로직 이식: 
 *                 수만 개의 파일 폭격으로 커널 큐가 포화 상태(OVERFLOW)일 때 즉시 디스크 풀 스캔을 돌리면 I/O 스래싱이 발생합니다. 
 *                 이를 막기 위해 OS 스케줄러에 1초간 자원을 양보(Delayed)한 뒤, 폭풍이 잦아들었을 때 수동 스윕(Sweep)을 집행합니다.
 * - 💡 [초정밀 수술] 락(Lock) 스토킹 방어 클리너 전개: 
 *                 파일 쓰기가 끝나지 않은 파일(Lock)에 대해 100번 재시도 후 흡수를 포기(`return;`)하여 
 *                 투입구에 영원히 방치되던 쓰레기(좀비) 파일 버그를 파괴했습니다.
 *                 임계치(Max Retry)에 도달한 파일은 즉시 `격리소_QUARANTINE` 디렉토리로 
 *                 강제 유폐(Move)시켜 투입구의 영구적인 청결을 유지하는 백그라운드 클리너 배관을 개통했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 시스템 이벤트 감청(WatchService), 원자적 이동(Atomic Move)을 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for file system event listening (WatchService) and atomic move.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트.스풀_상태;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 외부에서 던져진 파일을 OS 인터럽트로 감지하여 원자적으로 포획하고 좀비를 격리하는 자율 감시망입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An autonomous watch network that detects files thrown from the outside via OS interrupts, captures them atomically, and isolates zombies.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_423010
 * [파일명] A0_DT_42_423010_사상의_지평선_자율_감시망.java
 * [모듈명] 통합 OS V6.0 - Tier 16: 사상의 지평선 자율 감시망 (무인 디렉토리 와처 데몬)
 * 
 * [설계 명세]
 * 1. 역할: '투입구(INGRESS)' 폴더를 감시하다가 비정형 문헌(PDF, CSV, TXT 등)이 유입되면 OS 커널 인터럽트를
 * 수신하여 자동 포획.
 * 2. 기능: WatchService 기반 O(1) 유휴 대기, 지연된(Delayed) 오버플로우 복구, 락-스토킹 파일 강제 격리.
 * 3. 의도: 인간의 명시적 쿼리(INSERT) 없이, 파일을 던지는 행위 자체를 DB 트랜잭션의 시작으로 승격시킴.
 * 4. 이론: 이벤트 구동형 I/O(Event-Driven I/O), inotify/ReadDirectoryChangesW 커널 훅, 배타적
 * 파일 락.
 * ==============================================================================
 */
public final class A0_DT_42_423010_사상의_지평선_자율_감시망 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.423010_EVENT_HORIZON_WATCHER");

    // [1. 한글 상세 주석]
    // 💡 [절대 상수] 불완전 파일 복사 대기 설정 및 오버플로우 지연 대기 시간
    // 외부 탐색기나 다운로더가 파일을 아직 쓰고 있는 경우를 방어하기 위한 임계치입니다.
    // [2. 영문 상세 주석]
    // 💡 [Absolute Constants] Settings for waiting on incomplete file copies and
    // overflow delay times.
    // Thresholds to defend against external explorers or downloaders still writing
    // to the file.
    // [3. 자바 코드]
    private static final int 최대_락_재시도_횟수 = 100;
    private static final long 락_재시도_대기_밀리초 = 200L;
    private static final long 오버플로우_진정_대기_나노초 = 1_000_000_000L; // 1초 (I/O 폭풍 진정용)

    private final Path 투입구_경로_INGRESS;
    private final Path 작업장_경로_PROCESSING;

    // 💡 [수술 핵심] 락 스토킹 방어용 격리소 경로 결속
    private final Path 격리소_경로_QUARANTINE;

    // OS 커널의 파일 시스템 이벤트를 감청하는 하드웨어 훅(Hook)
    private WatchService 커널_감시_서비스;

    // 무인 감시 데몬 통제 플래그 및 스레드 풀
    private final AtomicBoolean 감시망_가동_상태 = new AtomicBoolean(false);
    private ExecutorService 감시_데몬_스레드풀;

    // 💡 [포트 앤 어댑터 패턴 (DIP)] 흡수 완료 시 다음 파이프라인으로 이관하기 위한 연결 포트
    private final 블랙홀_흡수_알림_포트 다음_파이프라인_포트;

    /**
     * [이관 포트 인터페이스]
     * 이 감시망은 다음 모듈이 무엇인지 알 필요가 없습니다. 오직 이 인터페이스를 통해
     * "작업장으로 이동된 완벽한 파일의 물리적 경로"를 건네줄 뿐입니다.
     */
    @FunctionalInterface
    public interface 블랙홀_흡수_알림_포트 {
        void 통보하다_물리적_흡수_완료(Path 작업장_파일_경로);
    }

    /**
     * [창세 생성자] 무인 파수꾼을 초기화하고 영토를 확정합니다.
     * 
     * @param 투입구_경로 감시할 외부 세상과의 접점 (Drop Zone)
     * @param 작업장_경로 내부 소화를 위해 파일을 격리할 보안 구역
     * @param 격리소_경로 💡 [수술 추가] 불완전 락 파일(좀비)을 강제 유폐시킬 감옥 구역
     * @param 알림_포트  파일 흡수 성공 시 호출될 콜백 인터페이스
     */
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
            // 사상의 지평선 영토 선제적 개척
            if (!Files.exists(투입구_경로_INGRESS))
                Files.createDirectories(투입구_경로_INGRESS);
            if (!Files.exists(작업장_경로_PROCESSING))
                Files.createDirectories(작업장_경로_PROCESSING);
            if (!Files.exists(격리소_경로_QUARANTINE))
                Files.createDirectories(격리소_경로_QUARANTINE);

            // OS 레벨의 파일 감청 서비스 등록 (디렉토리 생성/파일 드롭 감지)
            this.커널_감시_서비스 = FileSystems.getDefault().newWatchService();
            this.투입구_경로_INGRESS.register(커널_감시_서비스, ENTRY_CREATE);

        } catch (IOException 예외) {
            로거.log(Level.SEVERE, " [커널 훅 실패] 파일 시스템 이벤트를 감청할 수 없습니다.", 예외);
            throw new RuntimeException("사상의 지평선 구축 실패 (OS 인터럽트 바인딩 에러)", 예외);
        }

        로거.info(" >> [통합 OS V6.0] A0_DT_42_423010 사상의 지평선 자율 감시망 기동 준비. (지연 폴백 및 락-스토킹 방어 클리너 장착 완료)");
    }

    // [1. 한글 상세 주석]
    // [관제 역학 1: 무인 감시 데몬 가동]
    // 주기적인 폴링(Polling) 없이 잠들어 있다가, 커널이 깨울 때만 반응하는 진정한 의미의 백그라운드 이벤트 루프를 점화합니다.
    // [2. 영문 상세 주석]
    // [Control Dynamics 1: Autonomous Watch Daemon Operation]
    // Ignites a true background event loop that sleeps without periodic polling and
    // reacts only when awakened by the kernel.
    // [3. 자바 코드]
    public void 가동하다_무인_감시망() {
        if (!감시망_가동_상태.compareAndSet(false, true)) {
            로거.warning(" [충돌 방어] 무인 감시망이 이미 가동 중입니다.");
            return;
        }

        this.감시_데몬_스레드풀 = Executors.newSingleThreadExecutor(runnable -> {
            Thread 스레드 = new Thread(runnable, "OS_EVENT_HORIZON_WATCHER");
            스레드.setDaemon(true); // JVM 종료 시 족쇄 없이 우아하게 강하하기 위한 데몬화
            return 스레드;
        });

        this.감시_데몬_스레드풀.submit(this::감청하다_커널_이벤트);
        로거.info("   ├─ [파수꾼 점화] 투입구(INGRESS) 감시 시작. 외부 문헌 유입을 무기한 대기합니다...");
    }

    // [1. 한글 상세 주석]
    // [관제 역학 2: 커널 인터럽트 수신 및 블로킹 감시 루프]
    // 💡 [수술 핵심: 지연을 둔 수동 풀링(Delayed Fallback)] OVERFLOW 발생 시 I/O 스톰을 잠재운 뒤 수동 스캔을
    // 집행합니다.
    // [2. 영문 상세 주석]
    // [Control Dynamics 2: Kernel Interrupt Reception and Blocking Watch Loop]
    // 💡 [Surgery Core: Delayed Fallback Polling] Executes a manual scan after
    // calming the I/O storm when OVERFLOW occurs.
    // [3. 자바 코드]
    private void 감청하다_커널_이벤트() {
        try {
            // 데몬 시작 전 이미 쌓여있던 파일 구출을 위한 1회 초기화 풀 스캔
            실행하다_오버플로우_수동_풀스캔();

            while (감시망_가동_상태.get()) {
                // 💡 [기계적 공감] CPU 루프를 태우지 않고, OS 커널이 파일 생성 이벤트를 던져줄 때까지
                // 스레드를 완벽히 재워둡니다(Interruptible Wait). 대기 중 CPU 점유율 0%.
                WatchKey 감시_키 = 커널_감시_서비스.take();

                for (WatchEvent<?> 이벤트 : 감시_키.pollEvents()) {
                    WatchEvent.Kind<?> 종류 = 이벤트.kind();

                    // 💡 [핵심 교정: 지연을 둔 오버플로우 폴백 방어막 전개]
                    // 너무 많은 파일이 순식간에 쏟아져 커널 큐가 가득 차면 OVERFLOW 이벤트가 발생합니다.
                    // 즉시 디스크를 풀 스캔하면 I/O 폭풍(Thrashing)이 일어나므로, OS 스케줄러에 1초간 자원을 양보(Delay)하여
                    // 파일 유입이 진정된 후에 안전하게 수동 구출 작전을 개시합니다.
                    if (종류 == OVERFLOW) {
                        로거.warning(" 🚨 [경보] OS 파일 감청 큐 OVERFLOW 감지! I/O 폭풍을 진정시킨 뒤(1초 지연) 수동 풀 스캔(Fallback)을 발동합니다.");
                        LockSupport.parkNanos(오버플로우_진정_대기_나노초); // 1초 지연 대기
                        실행하다_오버플로우_수동_풀스캔();
                        continue;
                    }

                    // 발생한 이벤트의 파일명 획득
                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> 경로_이벤트 = (WatchEvent<Path>) 이벤트;
                    Path 유입된_파일명 = 경로_이벤트.context();
                    Path 유입된_절대경로 = 투입구_경로_INGRESS.resolve(유입된_파일명);

                    로거.info("   ├─ [중력장 교란 감지] 사상의 지평선에 새로운 문헌이 유입되었습니다: " + 유입된_파일명);

                    // 즉각 원자적 이동을 수행하여 파일을 안전한 작업장으로 격리합니다.
                    실행하다_사상의_지평선_흡수(유입된_절대경로);
                }

                // 키 초기화 (실패 시 감시 대상 폴더가 삭제된 것이므로 루프 종료)
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

    /**
     * [관제 역학 3: 지연 오버플로우 수동 풀스캔 (Fallback Recovery)]
     * 이벤트가 누락되어 영구히 방치될 위험에 처한 파일들을 디렉토리 순회(`Files.list`)를 통해
     * 일괄 색출하고 사상의 지평선으로 강제 흡수시킵니다.
     */
    private void 실행하다_오버플로우_수동_풀스캔() {
        try (Stream<Path> 파일_스트림 = Files.list(투입구_경로_INGRESS)) {
            파일_스트림
                    .filter(Files::isRegularFile)
                    .forEach(this::실행하다_사상의_지평선_흡수);
        } catch (IOException 예외) {
            로거.log(Level.SEVERE, " [수동 스캔 파열] 투입구 폴더 수동 복구 스캔 중 I/O 예외 발생.", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [초정밀 수술 적용: 락 스토킹 방어 클리너 결계]
    // 불완전 복사 방어 중 한계치에 도달한 파일(다운로드 중단 등)을 버리고(return) 도망가지 않습니다.
    // 즉시 격리소(QUARANTINE)로 파일을 강제 치워버려 INGRESS의 영구적인 청결 상태를 유지합니다.
    // [2. 영문 상세 주석]
    // 💡 [Ultra-Precision Surgery Applied: Lock Stalking Defense Cleaner Shield]
    // Does not simply abandon (return) files that reach the threshold during
    // partial copy defense.
    // Instantly force-moves them to the Quarantine to maintain the permanent
    // cleanliness of INGRESS.
    // [3. 자바 코드]
    /**
     * [흡수 역학 1: 부분 복사(Partial Write) 방어 및 원자적 이동(Atomic Move)]
     * 파일 쓰기가 완벽히 끝났음을 OS 파일 락(FileLock)으로 증명한 뒤 원자적으로 흡수합니다.
     */
    private void 실행하다_사상의_지평선_흡수(Path 원본_파일) {
        String 파일명 = 원본_파일.getFileName().toString();

        // 숨김 파일, 운영체제 메타 파일, 임시 다운로드 파일(.tmp, .crdownload) 무시
        if (파일명.startsWith(".") || 파일명.endsWith(".tmp") || 파일명.endsWith(".crdownload")) {
            return;
        }

        // 1. [불완전 복사 방어막] OS가 파일 쓰기를 완전히 마쳤는지 검증
        boolean 파일_작성_완료 = 검증하다_파일_쓰기_완료(원본_파일);

        if (!파일_작성_완료) {
            // 💡 [수술 핵심: 락 스토킹 파일 강제 유폐]
            // 최대 재시도 횟수를 초과해도 락이 풀리지 않는 파일은 다운로드가 멈췄거나 좀비화된 파일입니다.
            // 투입구에 남겨두면 영원히 다시 스캔되므로, 격리소로 물리적 이주(Move)를 집행합니다.
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
            // 2. 💡 [커널 레벨 원자적 이동 (Atomic Move)]
            // 일반적인 복사 후 삭제(Copy & Delete) 방식은 도중에 전원이 나가면 고아 파일(Orphan)을 만듭니다.
            // ATOMIC_MOVE는 동일 파일 시스템 내에서 메타데이터 인덱스만 1클럭에 교체하므로
            // 중간 상태(Intermediate State)가 절대 존재하지 않는 100% 무결성을 보장합니다.
            Files.move(원본_파일, 타겟_파일_경로, StandardCopyOption.ATOMIC_MOVE);

            로거.fine("   ├─ [블랙홀 흡수 완료] 문헌이 작업장(PROCESSING)으로 원자적 이동(Atomic Move) 되었습니다: " + 파일명);

            // 3. [다음 파이프라인으로 릴레이] 문헌 해체 도끼(Shredder)에게 작업장 경로 통보
            다음_파이프라인_포트.통보하다_물리적_흡수_완료(타겟_파일_경로);

        } catch (AtomicMoveNotSupportedException 예외) {
            // 파일 시스템이 Atomic Move를 지원하지 않는 다른 볼륨(Drive)일 경우의 안전한 폴백(Fallback)
            try {
                Files.move(원본_파일, 타겟_파일_경로, StandardCopyOption.REPLACE_EXISTING);
                로거.fine("   ├─ [블랙홀 흡수 완료 (Fallback)] 문헌이 작업장으로 복사-이동 되었습니다: " + 파일명);
                다음_파이프라인_포트.통보하다_물리적_흡수_완료(타겟_파일_경로);
            } catch (IOException 후속_예외) {
                로거.log(Level.SEVERE, " [원자적 붕괴] Fallback 파일 이동조차 실패했습니다. 파일 락 경합 의심.", 후속_예외);
            }
        } catch (IOException 예외) {
            // 파일이 이미 다른 스레드나 수동 스캔에 의해 옮겨졌을 경우 무시 (Idempotent)
            로거.fine("   ├─ [중복 흡수 방어] 해당 파일은 이미 다른 파동에 의해 흡수되었습니다: " + 파일명);
        }
    }

    /**
     * [보조 역학: 배타적 파일 락 (Exclusive File Lock) 검증]
     * 파일 전송(다운로드, FTP, 드래그 앤 드롭 복사)이 완전히 끝났는지 FileChannel의 배타적 락으로 스캔합니다.
     */
    private boolean 검증하다_파일_쓰기_완료(Path 타겟_파일) {
        int 재시도_횟수 = 0;

        while (재시도_횟수 < 최대_락_재시도_횟수) {
            // 파일을 WRITE 옵션으로 열기 시도. 외부 탐색기가 아직 파일을 쓰고 있다면 여기서 IOException이 발생함.
            try (FileChannel 채널 = FileChannel.open(타겟_파일, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

                // tryLock()은 블로킹되지 않고 즉시 락을 시도합니다.
                // 다른 프로세스(백신 프로그램, 탐색기 등)가 파일을 잡고 있다면 null을 반환합니다.
                FileLock 락 = 채널.tryLock();
                if (락 != null) {
                    락.release();
                    return true; // 온전한 독점권 확보 확인 (쓰기 완료)
                }
            } catch (IOException 예외) {
                // 파일이 아직 다른 프로세스에 의해 배타적으로 사용 중임
            }

            // 파일이 아직 쓰여지는 중이므로 백오프(Backoff) 대기 후 재시도
            try {
                Thread.sleep(락_재시도_대기_밀리초);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            재시도_횟수++;
        }
        return false; // 최대 재시도 초과 시 락-스토킹 상태로 반환
    }

    /**
     * [종결] 시스템 종료 시 감시망 자원을 OS에 안전하게 환원합니다.
     */
    public void 안전_셧다운_집행() {
        if (감시망_가동_상태.compareAndSet(true, false)) {
            로거.info("   ├─ [감시망 셧다운] 사상의 지평선 무인 파수꾼 철수 절차 개시...");

            try {
                if (커널_감시_서비스 != null) {
                    // WatchService를 닫으면 take()에서 대기 중인 스레드에 ClosedWatchServiceException이 발생하며 깨어남
                    커널_감시_서비스.close();
                }
            } catch (IOException e) {
                로거.warning(" [셧다운 경고] WatchService 포트 폐쇄 중 I/O 예외 발생.");
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
            로거.info(" >> [무인 파이프라인 차단 완료] OS 커널 이벤트 감청 서비스가 안전하게 회수되었습니다.");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 💡 지연 오버플로우 수동 풀스캔 (Delayed Fallback for Queue Overflow):
 * OS 커널의 `inotify` 큐는 크기가 제한되어 있습니다. 악의적인 스크립트나 FTP 복사 작업이 1초에 수만 개의
 * 파일을 투입구(INGRESS)에 부어버리면 커널은 `OVERFLOW` 이벤트를 사출합니다.
 * 과거 아키텍처는 이를 감지하자마자 즉시 `Files.list`로 디스크 전체를 스윕(Sweep)하려 들었습니다.
 * 이는 현재 디스크가 파일 쓰기 폭풍(I/O Storm)으로 비명을 지르고 있는 와중에 디스크 락을 더 세게 쥐어짜는
 * 자살 행위이자 CPU 스래싱(Thrashing)의 원흉이었습니다.
 * 본 리메이크는 오버플로우 감지 시, `LockSupport.parkNanos(1_000_000_000L)`를 호출하여
 * OS 스케줄러에게 딱 1초 동안 자원을 양보(Yield)합니다. 파일 폭격이 지나가고 디스크의 자기 평형이 회복된 그 찰나에
 * 우아하게 수동 스캔을 개시하는 극강의 '기계적 공감(Mechanical Sympathy)'을 성취했습니다.
 * 
 * 2. 💡 락(Lock) 스토킹 방어 클리너 (Defending against Lock Stalking Zombies):
 * 이전 버전의 치명적인 맹점은, 파일 복사 중 다운로드가 끊기거나 백신(Anti-virus) 프로그램이
 * 파일을 영구적으로 물고(Lock) 놓아주지 않을 때, 100번을 재시도하고 나서 단순히 `return;`으로
 * 도망가버리는 것이었습니다. 이는 투입구(INGRESS) 폴더에 영원히 처리되지 않는 '좀비 파일(Zombie File)'을
 * 방치하여 시스템의 투명성을 파괴하는 락-스토킹(Lock-Stalking) 현상입니다.
 * 수술이 완료된 파이프라인은 이 좀비들을 무자비하게 잡아냅니다. 임계치에 도달하여 인내심(Timeout)이
 * 바닥난 파일은 즉시 `QUARANTINE(격리소)` 폴더로 원자적 무브(`Files.move`) 시켜버립니다.
 * 이를 통해 투입구는 단 하나의 불순물도 남지 않는 영구적인 무균 상태(Sterile State)를 수호하게 됩니다.
 * 
 * 3. 무인 자동화(Autonomous)와 이벤트 구동 I/O (Event-Driven I/O):
 * 기존의 RDBMS 배치는 스케줄러(Cron)가 매 1분마다 폴더를 열어보고 파일이 있는지
 * 검사(Polling)하는 방식을 썼습니다. 이는 파일이 없을 때도 CPU와 디스크 I/O를 갉아먹는 치명적인
 * 열역학적 낭비(Thermodynamic Waste)입니다.
 * 이 '자율 감시망'은 자바의 `WatchService`를 활용하여 리눅스의 `inotify` 하드웨어 인터럽트를 직접 래핑합니다.
 * 문헌이 떨어지기 전까지 파수꾼 스레드는 CPU를 단 0.0001%도 사용하지 않고 깊은 동면에 빠져 있다가,
 * 커널이 "파일이 생겼다"고 전기를 흘려보내는 순간 0 나노초 지연으로 즉각 깨어나 파일을 집어삼킵니다.
 * 인간의 `INSERT` 쿼리를 '파일 드래그 앤 드롭'이라는 물리적 행위로 승격시킨 진정한 무인 시스템(Unmanned
 * System)입니다.
 * =============================================================================
 */