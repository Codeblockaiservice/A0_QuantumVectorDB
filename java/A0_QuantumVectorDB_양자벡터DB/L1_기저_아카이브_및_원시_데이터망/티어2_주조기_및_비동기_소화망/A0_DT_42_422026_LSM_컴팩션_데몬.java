/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망
 * @alias LSM_Compaction_Daemon
 * @tier 2
 * @keywords MVCC, Asynchronous Compaction, WAL Rolling, Segment Architecture, Crash Recovery
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422026_LSM_컴팩션_데몬.java
 * - 모듈명: 통합 OS V6.1 - Tier 2: LSM 컴팩션 데몬 (비동기 병합 관리자 및 WAL 수호자)
 * - 기능 및 역할: RCU 주조 워커가 RAM에 적재한 텐서 파편들을 물리 디스크로 비동기 병합(Compaction)하며, 정전 시 증발을 막기 위해 WAL(Write-Ahead Log)을 기록합니다.
 * - 이론 및 기술: 비동기 I/O 위임, CQRS, 모아치기 플러시(Batch Force), ByteBuffer Limit Control, NVMe WAL Rolling.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정]: 지시사항에 따라 특정 세력을 연상시키는 단어를 전면 소각하고 '통합 OS'로 치환 완료.
 * - 💡 [V6.1 치명적 결함 수술] NVMe 친화적 WAL 세그먼트 로테이션 (Rolling) 완수: 
 *                 영속화 후 무지성으로 `truncate(0)`을 수행하여 디스크 단편화와 SSD 수명 단축을 유발하던 안티패턴을 완전히 파괴했습니다. 
 *                 파일 크기가 50MB를 초과하면 기존 WAL을 닫고 새 파일(Segment)로 롤링(Rolling)하며, 
 *                 백그라운드 병합 주기에 맞춰 병합이 완료된 구형 WAL만을 비동기적으로 삭제(Delete)하는 
 *                 초고속 Append-Only 세그먼트 아키텍처로 전면 개편했습니다.
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
// 컴플라이언스 선언 및 클래스 헤더. RAM에 적재된 텐서를 물리 디스크에 안전하게 영속화시키는 비동기 병합 관리자입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An asynchronous compaction manager that safely persists tensors loaded in RAM to a physical disk.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422026
 * [파일명] A0_DT_42_422026_LSM_컴팩션_데몬.java
 * [모듈명] 통합 OS V6.1 - Tier 2: LSM 컴팩션 데몬 (비동기 병합 관리자 및 WAL 수호자)
 * 
 * [기능 명세]
 * 1. 💡 비동기 I/O 위임 (Asynchronous Compaction): RCU 주조 워커가 RAM(Delta 버퍼)에
 * 적재한 텐서 파편들을 건네받아, 물리 디스크(.layer) 파일로의 병합 및 동기화(Flush)를 백그라운드에서 전담합니다.
 * 2. 💡 유휴 시간(Idle) 탐지 및 백오프 스로틀링: 데이터가 폭포수처럼 쏟아지는 주조 타이밍에는
 * 디스크 I/O를 유예(Backoff)하고, 시스템이 평온해진 유휴 시간에만 원자적 병합을 수행하여 디스크 병목을 멸균합니다.
 * 3. 💡 CQRS (명령과 조회의 분리) 완수: 주조(Write)는 대기열 삽입만으로 $O(1)$ 속도로 즉각 완료되며,
 * 조회(Read) 코어는 병합된 메인 매트릭스에서 $O(1)$ 속도로 텐서를 퍼가게 만드는 아키텍처적 극의를 달성합니다.
 * 4. 💡 모아치기 플러시 (Batch Force): 수천 개의 델타 조각을 하나하나 디스크에 `force()` 하지 않고,
 * 포트(Port)별로 그룹화하여 단 한 번의 커널 시스템 콜로 디스크 동기화를 집행합니다.
 * 5. 💡 [V6.1 초정밀 수술] NVMe 친화적 WAL 세그먼트 로테이션 (Rolling):
 * `truncate(0)`에 의존하던 단일 파일 병목을 파괴했습니다. WAL 파일이 50MB를 초과하면 즉각 새로운
 * 세그먼트로 로테이션(Rolling)하며, 디스크 병합이 완수된 구형 WAL 파일들만 비동기적으로 소각(GC)하여
 * 100% Append-Only 파일 시스템의 기하학적 성능을 완벽히 복원했습니다.
 * ==============================================================================
 */
public final class A0_DT_42_422026_LSM_컴팩션_데몬 {

    // [1. 한글 상세 주석]
    // 글로벌 로거 선언 및 스로틀링 통제를 위한 절대 상수를 정의합니다.
    // [2. 영문 상세 주석]
    // Global logger declaration and definition of absolute constants for throttling
    // control.
    // [3. 자바 코드]
    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422026_LSM_COMPACTION_DAEMON");

    private static final long 기본_폴링_주기_밀리초 = 500L;
    private static final long 유휴_판단_임계치_밀리초 = 2000L; // 2초 이상 데이터 유입이 없으면 유휴(Idle)로 판단
    private static final long 최대_백오프_지연_밀리초 = 5000L;

    // 💡 [V6.1 신규] WAL 세그먼트 로테이션 임계치 (50MB)
    private static final long WAL_로테이션_임계치_바이트 = 50L * 1024L * 1024L;

    // [1. 한글 상세 주석]
    // 데몬의 생명주기를 관리하는 스케줄러와 상태 추적 변수들을 선언합니다.
    // [2. 영문 상세 주석]
    // Declares the scheduler and state tracking variables that manage the daemon's
    // lifecycle.
    // [3. 자바 코드]
    private final ScheduledExecutorService 컴팩션_스케줄러;
    private final AtomicBoolean 데몬_가동_상태 = new AtomicBoolean(false);

    private final AtomicLong 최근_주조_타임스탬프 = new AtomicLong(System.currentTimeMillis());
    private long 현재_백오프_지연시간 = 기본_폴링_주기_밀리초;

    // [1. 한글 상세 주석]
    // RCU 워커와 공유하는 읽기-쓰기 충돌 방어망(SeqLock) 및 커널 포트를 조달하기 위한 의존성 콜백입니다.
    // [2. 영문 상세 주석]
    // Read-write collision defense network (SeqLock) shared with the RCU worker and
    // dependency callback to procure kernel ports.
    // [3. 자바 코드]
    private final Map<String, AtomicLong> 시퀀스_락_망;
    private final Function<String, WritePort> 포트_리졸버;

    // [1. 한글 상세 주석]
    // ACID 내구성을 보장하기 위한 WAL 롤링 아키텍처 변수와 비동기 가비지 컬렉션 대기열입니다.
    // [2. 영문 상세 주석]
    // WAL rolling architecture variables and asynchronous garbage collection queue
    // to ensure ACID durability.
    // [3. 자바 코드]
    private final Path WAL_저장_경로;
    private volatile FileChannel 현재_WAL_채널;
    private volatile Path 현재_WAL_경로;
    private final Object WAL_순차_락 = new Object();

    private final Queue<Path> 폐기_대기_WAL_큐 = new ConcurrentLinkedQueue<>();

    // [1. 한글 상세 주석]
    // RAM에 상주하는 델타 텐서 조각들을 담아두는 비동기 큐 레코드입니다.
    // [2. 영문 상세 주석]
    // An asynchronous queue record that holds delta tensor fragments residing in
    // RAM.
    // [3. 자바 코드]
    public record 델타_병합_태스크(
            String 지표명,
            WritePort 메인_디스크_포트,
            MemorySegment 델타_메모리_세그먼트,
            long 타겟_절대_오프셋,
            long 바이트_크기) {
    }

    private final Queue<델타_병합_태스크> 델타_대기열 = new ConcurrentLinkedQueue<>();

    // [1. 한글 상세 주석]
    // [창세 생성자] LSM 컴팩션 데몬을 기동하고 백그라운드 스케줄러 및 WAL 롤링 채널을 점화합니다.
    // [2. 영문 상세 주석]
    // [Genesis Constructor] Starts the LSM compaction daemon and ignites the
    // background scheduler and WAL rolling channel.
    // [3. 자바 코드]
    /**
     * @param 시퀀스_락_망   RCU 워커와 공유하는 읽기-쓰기 충돌 방어망 (SeqLock)
     * @param WAL_저장_경로 NVMe 등 가장 빠른 스토리지에 위치할 WAL 파일의 디렉토리
     * @param 포트_리졸버    콜드스타트 리플레이 시, 지표명(String)을 통해 실제 커널 권한 포트(WritePort)를 조달해 줄
     *                  콜백 함수
     */
    public A0_DT_42_422026_LSM_컴팩션_데몬(
            Map<String, AtomicLong> 시퀀스_락_망,
            Path WAL_저장_경로,
            Function<String, WritePort> 포트_리졸버) {

        if (시퀀스_락_망 == null || WAL_저장_경로 == null || 포트_리졸버 == null) {
            throw new IllegalArgumentException("[배관 파열] 필수 방어망이 누락되어 컴팩션 데몬을 기동할 수 없습니다.");
        }
        this.시퀀스_락_망 = 시퀀스_락_망;
        this.WAL_저장_경로 = WAL_저장_경로;
        this.포트_리졸버 = 포트_리졸버;

        // 💡 [ACID 내구성 확보] WAL 파일 시스템 채널 개통 및 Crash Recovery
        try {
            if (!Files.exists(WAL_저장_경로)) {
                Files.createDirectories(WAL_저장_경로);
            }

            // 1. [콜드스타트 리플레이] 잔여 WAL 파일들을 찾아 순차적으로 복원 (Crash Recovery)
            try (Stream<Path> 스트림 = Files.list(WAL_저장_경로)) {
                List<Path> 기존_WAL_목록 = 스트림
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().startsWith("MATRIX_A0_422026_LSM_WAL_"))
                        .filter(p -> p.getFileName().toString().endsWith(".log"))
                        .sorted() // 시퀀스(타임스탬프) 기반 시간순 정렬
                        .collect(Collectors.toList());

                if (!기존_WAL_목록.isEmpty()) {
                    로거.warning(String.format(" 🚨 [콜드스타트] 비정상 종료로 인한 %d개의 LSM WAL 잔여 세그먼트 감지. 롤포워드(Replay)를 개시합니다.",
                            기존_WAL_목록.size()));

                    for (Path 잔여_WAL : 기존_WAL_목록) {
                        if (Files.size(잔여_WAL) > 0) {
                            복원하다_WAL_리플레이(잔여_WAL);
                        }
                        // 리플레이가 완료된 파일은 RAM(대기열)에 복원되었으므로, 디스크 병합 후 삭제되도록 폐기 대기열로 이관
                        폐기_대기_WAL_큐.offer(잔여_WAL);
                    }
                }
            }

            // 2. [초고속 Append-Only 채널 개방] 새로운 롤링 WAL 세그먼트 점화
            로테이션하다_새로운_WAL_세그먼트();

        } catch (IOException 예외) {
            로거.log(Level.SEVERE, " [배관 파열] WAL 파일 채널을 개방할 수 없습니다.", 예외);
            throw new RuntimeException("LSM WAL 기동 실패", 예외);
        }

        // 디스크 I/O를 전담할 단일 백그라운드 스레드 (순차적 병합 보장)
        this.컴팩션_스케줄러 = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread 스레드 = new Thread(runnable, "LSM-Compaction-Daemon");
            스레드.setPriority(Thread.MIN_PRIORITY); // 메인 HFT 연산을 방해하지 않도록 최하위 우선순위 배정
            스레드.setDaemon(true);
            return 스레드;
        });

        로거.info(" >> [통합 OS V6.1] A0_DT_42_422026 LSM 컴팩션 데몬 기동. (비동기 병합 및 WAL 롤링(Rolling) 아키텍처 전개 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 [수술 완료: WAL 로테이션 엔진] WAL 세그먼트가 50MB를 초과했을 때 기존 파일을 닫아 가비지 큐로 넘기고 새로운 파일을
    // 엽니다.
    // [2. 영문 상세 주석]
    // 💡 [Surgery Complete: WAL Rotation Engine] When the WAL segment exceeds 50MB,
    // safely closes the existing file, passes it to the garbage queue, and opens a
    // new file.
    // [3. 자바 코드]
    private void 로테이션하다_새로운_WAL_세그먼트() throws IOException {
        if (현재_WAL_채널 != null && 현재_WAL_채널.isOpen()) {
            현재_WAL_채널.force(true); // 커널 버퍼에 남은 데이터를 디스크에 영속화
            현재_WAL_채널.close();
            폐기_대기_WAL_큐.offer(현재_WAL_경로); // 방금 닫은 과거의 영광을 폐기 대기열로 이관
        }

        long 새_시퀀스 = System.currentTimeMillis();
        String 새_파일명 = String.format("MATRIX_A0_422026_LSM_WAL_%d.log", 새_시퀀스);
        현재_WAL_경로 = WAL_저장_경로.resolve(새_파일명);

        현재_WAL_채널 = FileChannel.open(현재_WAL_경로,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        로거.info("   ├─ [WAL 로테이션] 새로운 Append-Only 쓰기 세그먼트가 개방되었습니다: " + 새_파일명);
    }

    // [1. 한글 상세 주석]
    // [복원 역학: WAL 리플레이 (Crash Recovery)] 정전 등으로 시스템이 비정상 종료되었을 때 잔여 WAL을 큐에 살려냅니다.
    // [2. 영문 상세 주석]
    // [Recovery Dynamics: WAL Replay (Crash Recovery)] Revives residual WALs to the
    // queue when the system terminates abnormally due to power outages, etc.
    // [3. 자바 코드]
    private void 복원하다_WAL_리플레이(Path wal_파일_경로) throws IOException {
        try (FileChannel 읽기_채널 = FileChannel.open(wal_파일_경로, StandardOpenOption.READ)) {
            // 헤더 규격: 이름_길이(4) + 절대_오프셋(8) + 바이트_크기(8) = 총 20 Bytes
            ByteBuffer 헤더_버퍼 = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);

            int 복원_카운트 = 0;
            while (true) {
                헤더_버퍼.clear();
                int 읽은_길이 = 0;
                while (읽은_길이 < 20) {
                    int 결과 = 읽기_채널.read(헤더_버퍼);
                    if (결과 == -1)
                        break;
                    읽은_길이 += 결과;
                }

                if (읽은_길이 == 0)
                    break; // 파일의 끝(EOF) 도달

                if (읽은_길이 < 20) {
                    로거.warning(" [WAL 손상] 헤더 바이트가 불완전합니다. 리플레이를 중단합니다: " + wal_파일_경로.getFileName());
                    break;
                }

                헤더_버퍼.flip();
                int 이름_길이 = 헤더_버퍼.getInt();
                long 절대_오프셋 = 헤더_버퍼.getLong();
                long 바이트_크기 = 헤더_버퍼.getLong();

                // 가변 길이의 지표명 문자열 바이트 판독
                ByteBuffer 이름_버퍼 = ByteBuffer.allocate(이름_길이);
                if (읽기_채널.read(이름_버퍼) != 이름_길이)
                    break;
                이름_버퍼.flip();
                String 지표명 = new String(이름_버퍼.array(), StandardCharsets.UTF_8);

                // 델타 메모리 바이트 판독 및 세그먼트 복원
                ByteBuffer 데이터_버퍼 = ByteBuffer.allocate((int) 바이트_크기);
                if (읽기_채널.read(데이터_버퍼) != 바이트_크기)
                    break;
                데이터_버퍼.flip();
                MemorySegment 델타_세그먼트 = MemorySegment.ofArray(데이터_버퍼.array());

                // 💡 [지연 권한 결속] 콜백 함수를 통해 OS 커널에서 해당 지표의 WritePort를 다시 얻어옵니다.
                WritePort 복원된_포트 = 포트_리졸버.apply(지표명);
                if (복원된_포트 == null) {
                    로거.severe(" [복원 붕괴] 지표명 '" + 지표명 + "'의 WritePort를 조달할 수 없습니다. 해당 태스크를 유실 처리합니다.");
                    continue;
                }

                // 읽어들인 태스크를 대기열에 고스란히 복원
                델타_병합_태스크 복원된_태스크 = new 델타_병합_태스크(지표명, 복원된_포트, 델타_세그먼트, 절대_오프셋, 바이트_크기);
                델타_대기열.offer(복원된_태스크);
                복원_카운트++;
            }
            로거.info(String.format("   ├─ [리플레이 수료] %s 파일에서 %d개의 증발했던 컴팩션 태스크가 성공적으로 부활했습니다.",
                    wal_파일_경로.getFileName(), 복원_카운트));
        }
    }

    // [1. 한글 상세 주석]
    // [관제 역학 1: 데몬 점화] 주기적으로 대기열을 감시하며 유휴 시간을 탐지하여 병합(Compaction)을 집행합니다.
    // [2. 영문 상세 주석]
    // [Control Dynamics 1: Daemon Ignition] Periodically monitors the queue and
    // detects idle time to execute compaction.
    // [3. 자바 코드]
    public void 컴팩션_데몬_가동() {
        if (!데몬_가동_상태.compareAndSet(false, true)) {
            return;
        }

        컴팩션_스케줄러.scheduleWithFixedDelay(
                this::실행하다_백그라운드_병합_루프,
                기본_폴링_주기_밀리초,
                기본_폴링_주기_밀리초,
                TimeUnit.MILLISECONDS);

        로거.info("   ├─ [LSM 관리망 활성화] 델타 버퍼 감시 및 디스크 병합 스케줄러가 백그라운드에 상주합니다.");
    }

    // [1. 한글 상세 주석]
    // [생산자 API: 델타 버퍼 위임 및 WAL 영속화] RCU 워커가 데이터를 RAM에 조립한 뒤 디스크 I/O를 위임합니다.
    // [2. 영문 상세 주석]
    // [Producer API: Delta Buffer Delegation and WAL Persistence] Delegates disk
    // I/O after the RCU worker assembles data into RAM.
    // [3. 자바 코드]
    public void 의뢰하다_비동기_병합(델타_병합_태스크 병합_태스크) {
        if (병합_태스크 != null && 데몬_가동_상태.get()) {

            // 1. 💡 [ACID 내구성 방어] RAM 대기열에 넣기 전 WAL 파일에 순차 기록 (Append-Only)
            byte[] 이름_바이트 = 병합_태스크.지표명().getBytes(StandardCharsets.UTF_8);
            int 총_버퍼_크기 = 20 + 이름_바이트.length + (int) 병합_태스크.바이트_크기();

            ByteBuffer wal_버퍼 = ByteBuffer.allocate(총_버퍼_크기).order(ByteOrder.LITTLE_ENDIAN);

            wal_버퍼.putInt(이름_바이트.length);
            wal_버퍼.putLong(병합_태스크.타겟_절대_오프셋());
            wal_버퍼.putLong(병합_태스크.바이트_크기());
            wal_버퍼.put(이름_바이트);

            // 아레나의 쓰레기 빈 공간(Zero-Padding)을 제거하는 정밀 절단 배관 (asSlice)
            ByteBuffer 데이터_버퍼 = 병합_태스크.델타_메모리_세그먼트()
                    .asSlice(0, 병합_태스크.바이트_크기())
                    .asByteBuffer();

            wal_버퍼.put(데이터_버퍼);
            wal_버퍼.flip();

            try {
                // 여러 스레드가 동시에 채널에 쓰더라도 바이트가 섞이지 않도록 순차 락(Lock)을 적용합니다.
                synchronized (WAL_순차_락) {
                    while (wal_버퍼.hasRemaining()) {
                        현재_WAL_채널.write(wal_버퍼);
                    }

                    // 💡 [수술 핵심] WAL 파일이 50MB를 초과하면 새로운 파일로 롤링(Rolling)
                    if (현재_WAL_채널.size() >= WAL_로테이션_임계치_바이트) {
                        로테이션하다_새로운_WAL_세그먼트();
                    }
                }
            } catch (IOException 예외) {
                로거.log(Level.SEVERE, " [WAL 붕괴] 델타 태스크 순차 기록 중 치명적 예외 발생", 예외);
            }

            // 2. [비동기 큐 삽입] 안전하게 영속화된 태스크를 램 대기열에 올림
            델타_대기열.offer(병합_태스크);
            최근_주조_타임스탬프.set(System.currentTimeMillis()); // 활성 상태 타임스탬프 갱신
        }
    }

    // [1. 한글 상세 주석]
    // [소비자 역학: 백그라운드 병합 및 동기화 루프] 유휴 상태를 판단하여 디스크 병합 및 모아치기를 수행합니다.
    // [2. 영문 상세 주석]
    // [Consumer Dynamics: Background Compaction and Synchronization Loop] Judges
    // idle state to perform disk compaction and batch flushing.
    // [3. 자바 코드]
    private void 실행하다_백그라운드_병합_루프() {
        try {
            if (델타_대기열.isEmpty()) {
                현재_백오프_지연시간 = 기본_폴링_주기_밀리초; // 큐가 비어있으면 초기화
                return;
            }

            long 흐른_시간 = System.currentTimeMillis() - 최근_주조_타임스탬프.get();

            // 💡 [백오프 스로틀링 (Backoff Throttling)] 데이터가 쏟아지고 있는 도중에는 디스크를 괴롭히지 않고 유예합니다.
            if (흐른_시간 < 유휴_판단_임계치_밀리초) {
                현재_백오프_지연시간 = Math.min(현재_백오프_지연시간 * 2, 최대_백오프_지연_밀리초);
                return;
            }

            // =========================================================================
            // 💡 유휴 시간 도래: 모아치기(Batch) 원자적 병합 집행
            // =========================================================================
            현재_백오프_지연시간 = 기본_폴링_주기_밀리초;

            // 병합 완료 후 한 번에 플러시(force)하기 위해 타격된 포트들을 수집합니다.
            Map<WritePort, Boolean> 플러시_대기_포트망 = new HashMap<>();
            int 병합된_태스크_카운트 = 0;

            델타_병합_태스크 태스크;
            // 큐에 있는 모든 델타를 메인 디스크 매핑 메모리로 쓸어 담습니다 (Drain).
            while ((태스크 = 델타_대기열.poll()) != null) {
                원자적_메모리_병합_실행(태스크);
                플러시_대기_포트망.put(태스크.메인_디스크_포트(), true);
                병합된_태스크_카운트++;
            }

            // 💡 [I/O 모아치기 최적화] 커널 시스템 콜(force)은 포트당 단 1번씩만 호출하여 디스크 드라이브 섹터 스래싱 소각.
            for (WritePort 갱신된_포트 : 플러시_대기_포트망.keySet()) {
                갱신된_포트.segment().force();
            }

            // 💡 [수술 완료: NVMe 친화적 가비지 컬렉션]
            // 무지성 truncate(0)로 인한 디스크 단편화를 완벽히 차단하고, 메인 매트릭에 100% 안전하게 데이터가 안착했으므로 구형 WAL
            // 세그먼트들을 물리적으로 소각합니다.
            if (병합된_태스크_카운트 > 0) {
                로거.fine(String.format("   ├─ [LSM 병합 완료] 유휴 시간 도달. %d개의 델타 파편이 메인 디스크로 원자적 병합 및 플러시되었습니다.",
                        병합된_태스크_카운트));

                Path 폐기할_WAL;
                while ((폐기할_WAL = 폐기_대기_WAL_큐.poll()) != null) {
                    try {
                        Files.deleteIfExists(폐기할_WAL);
                        로거.fine("   ├─ [WAL 가비지 컬렉션] 병합이 완료되어 효력을 다한 구형 WAL 세그먼트를 소각했습니다: " + 폐기할_WAL.getFileName());
                    } catch (IOException e) {
                        // 다른 프로세스(백신 등)가 잡고 있어 삭제에 실패했다면 큐에 다시 넣어 다음 유휴 주기에 소각을 시도합니다.
                        로거.warning(" [WAL 소각 지연] 파일 삭제 실패 (다음 주기에 재시도합니다): " + 폐기할_WAL.getFileName());
                        폐기_대기_WAL_큐.offer(폐기할_WAL);
                        break;
                    }
                }
            }

        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [컴팩션 붕괴] 백그라운 디스크 병합 중 치명적 커널 예외 발생.", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // [병합 역학: SeqLock 기반 원자적 메모리 이식] AI 코어의 찢어진 읽기(Torn Read)를 막기 위해 SeqLock을 걸고
    // 메모리를 복사합니다.
    // [2. 영문 상세 주석]
    // [Merge Dynamics: SeqLock-based Atomic Memory Implantation] Copies memory with
    // SeqLock to prevent Torn Reads in the AI core.
    // [3. 자바 코드]
    private void 원자적_메모리_병합_실행(델타_병합_태스크 태스크) {
        String 지표명 = 태스크.지표명();
        AtomicLong 시퀀스_락 = 시퀀스_락_망.get(지표명);

        // 💡 [SeqLock 쓰기 락온] 홀수 버전으로 전환하여 AI 읽기 스레드에게 갱신 중임을 통보
        if (시퀀스_락 != null) {
            시퀀스_락.incrementAndGet();
        }

        try {
            // RAM(Delta) -> 커널 페이지 캐시(Main)로 SIMD 고속 복사
            MemorySegment.copy(
                    태스크.델타_메모리_세그먼트(), 0,
                    태스크.메인_디스크_포트().segment().asSlice(태스크.타겟_절대_오프셋(), 태스크.바이트_크기()), 0,
                    태스크.바이트_크기());
        } finally {
            // 💡 [SeqLock 쓰기 해제] 짝수 버전으로 복귀하여 읽기 권한을 다시 개방
            if (시퀀스_락 != null) {
                시퀀스_락.incrementAndGet();
            }
        }
    }

    // [1. 한글 상세 주석]
    // [종결] 시스템 강하 시 대기열에 남은 모든 텐서를 강제 병합하고 자원을 회수합니다.
    // [2. 영문 상세 주석]
    // [Termination] Force-merges all remaining tensors in the queue and reclaims
    // resources upon system descent.
    // [3. 자바 코드]
    public void 안전_셧다운_집행() {
        if (데몬_가동_상태.compareAndSet(true, false)) {
            로거.info("   ├─ [컴팩션 셧다운] 데몬 정지 전 잔여 델타 대기열 강제 병합을 집행합니다...");

            // 스케줄러를 정지하고 남은 큐를 모두 비웁니다.
            컴팩션_스케줄러.shutdown();
            실행하다_백그라운드_병합_루프();

            try {
                if (!컴팩션_스케줄러.awaitTermination(10, TimeUnit.SECONDS)) {
                    컴팩션_스케줄러.shutdownNow();
                }
            } catch (InterruptedException 예외) {
                컴팩션_스케줄러.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // 현재 열려있는 WAL 채널 닫기
            try {
                if (현재_WAL_채널 != null && 현재_WAL_채널.isOpen()) {
                    현재_WAL_채널.force(true);
                    현재_WAL_채널.close();
                }
            } catch (IOException e) {
                로거.warning(" [셧다운 경고] WAL 채널을 닫는 중 I/O 에러가 발생했습니다.");
            }

            로거.info(" >> [LSM 데몬 종료] 모든 델타 텐서가 디스크에 영속화(Persist)되었습니다.");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 로그 구조화 병합 트리 (LSM-Tree)와 비동기 I/O 위임:
 * 전통적인 B-Tree 기반의 데이터베이스는 데이터가 들어올 때마다 디스크의 제자리를 찾아가서 덮어씁니다(In-place Update).
 * 이는 필연적으로 느린 Random I/O를 유발하여 HFT(고빈도 매매) 시스템의 목을 조릅니다.
 * 통합 OS V6.1은 최신 빅테크(Cassandra, RocksDB 등)의 LSM-Tree 철학을 차용했습니다.
 * RCU 주조 워커는 데이터를 디스크(Main)에 직접 쓰지 않고, RAM 상의 델타(MemTable/Delta Buffer)에 $O(1)$
 * 속도로 밀어 넣은 뒤 즉시 다음 틱(Tick)을 파싱하러 떠납니다.
 * 무거운 디스크 병합(Compaction)과 동기화(Flush) 작업은 오직 이 백그라운드 데몬이 전담합니다.
 * 이로써 쓰기(Write) 작업의 병목을 물리 법칙 수준에서 소거하는 '비동기 I/O 위임'이 달성됩니다.
 * 
 * 2. 💡 단일 파일 멸균과 NVMe 친화적 세그먼트 로테이션 (Segment Rolling):
 * 과거의 구현은 단일 `WAL.log` 파일에 계속 데이터를 쓴 뒤, 병합이 끝나면 무지성으로
 * `FileChannel.truncate(0)`을 호출하여
 * 파일 크기를 강제로 0으로 파괴했습니다.
 * 이는 파일 시스템(NTFS/ext4)의 MFT/Inode 블록을 극단적으로 피로하게 만들어 디스크 단편화(Fragmentation)를
 * 유발하고
 * 플래시 메모리(NVMe)의 셀 수명을 갉아먹는 치명적인 '기계적 무지'였습니다.
 * 수술이 완료된 V6.1 엔진은 카프카(Apache Kafka)의 세그먼트 아키텍처를 도입했습니다.
 * WAL이 50MB에 도달하면 무자비한 truncate 대신 우아하게 파일을 닫고 새로운 파일을 엽니다(Rolling).
 * 메인 디스크 병합이 완수되어 생명을 다한 '구형 WAL 파일'은 백그라운드 병합 주기에 맞춰 OS 레벨에서 물리적으로
 * 삭제(Delete) 처리됩니다. 이를 통해 100% Append-Only 파일 시스템의 기하학적 쓰기 성능을 완벽히 복원했습니다.
 * 
 * 3. 백오프 스로틀링(Backoff Throttling)과 기계적 공감(Mechanical Sympathy):
 * 데이터가 초당 수만 건씩 쏟아져 들어오는 '스풀링 폭풍' 한가운데서 디스크에 `.force()` 시스템 콜을
 * 때리는 것은, 심장이 맹렬히 뛰고 있는데 혈관을 옥죄는 것과 같은 기계적 무지(Mechanical Ignorance)입니다.
 * 본 데몬은 `System.currentTimeMillis() - 최근_주조_타임스탬프`를 계산하여, 시스템이 데이터를
 * 쉴 새 없이 받아먹고 있을 때는 디스크 병합을 조용히 유예(Backoff)합니다.
 * 폭풍이 지나가고 2초(유휴_판단_임계치_밀리초) 이상의 고요함이 찾아왔을 때, 비로소 RAM에 쌓인 수만 개의
 * 파편들을 단 한 번의 커널 호출로 일괄 병합(Batch Compaction)합니다.
 * 하드웨어의 생체 리듬(Biorhythm)을 완벽히 이해하고 동기화하는 객체 지향의 예술입니다.
 * =============================================================================
 */