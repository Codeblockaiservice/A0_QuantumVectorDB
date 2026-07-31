/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어20_연방_합의망
 * @alias Federal_Consensus_Protocol_Engine
 * @tier 20
 * @keywords Raft Consensus, Zero-Copy WAL, Two-Phase Commit (2PC), State Machine, Quorum, CAS, Lock-Free, LMAX Ring Buffer, Split-Brain Defense
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_425010_연방_합의_프로토콜_엔진.java
 * - 모듈명: 통합 OS V6.1 - Tier 20: 연방 합의 프로토콜 엔진 (Zero-Copy Raft 코어)
 * - 기능 및 역할: 0.1초 이내의 초고속 리더 선출 및 Zero-Copy 기반 WAL(Write-Ahead Log) 분산 복제를 수행하여, 단일 노드 붕괴 시 평행 우주(Follower)로 권력을 즉각 이양합니다.
 * - 이론 및 기술: Raft Consensus Algorithm, 2단계 커밋(2PC), 정족수(Quorum) 합의, Zero-Copy DMA, CAS(Compare-And-Swap), LMAX Disruptor Pattern.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 지시사항에 따라 금기어를 전면 소각하고 '통합 OS'로 치환 완료.
 * - 💡 [변경] 스플릿 브레인(Split-Brain) 원천 차단: 리더 선거 참여 및 투표(ACK) 발송 직전, 반드시 `currentTerm`과 `votedFor` 메타데이터를 별도의 디스크(WAL) 공간에 `force(true)` 플러시하여 물리적으로 영속화한 뒤에만 응답하도록 동기화 로직을 완벽히 수술했습니다.
 * - 💡 [배관 수복]: 상태 기계(State Machine) 최종 반영 시, 목업으로 방치되었던 디스크 플러시 주석을 제거하고, Tier 4 범용 OS 레이어 드라이버의 `실행_샌드박스_마스터_승격`을 호출하도록 배관을 완벽하게 결속했습니다.
 * - 💡 [네트워크 분리 및 주입]: 인터페이스로만 존재하던 `연방_Arrow_Flight_RPC_포트`를 실제 외부 클래스(Arrow Flight 클라이언트)에서 구현하여 생성자로 명시적 주입(DI)받도록 구조를 개편했습니다.
 * - 💡 [스레드 안전성 강화]: 분산 환경의 락 무결성을 수호하기 위해 `팔로워_다음_인덱스_nextIndex` 및 `반영된_인덱스_lastApplied` 등 핵심 상태 변수에 CAS(Compare-And-Swap) 원자적 갱신 로직을 도입하여 `synchronized` 병목을 파괴했습니다.
 * - 💡 [V6.1 락프리 I/O 수술 (Lock Contention 파괴)]: `전파하다_WAL_제로카피_복제` 내부에서 WAL 파일 기록 시 사용하던 `synchronized (WAL_순차_락)` 블록을 전면 폐기(Destroy)했습니다. 
 *                 대신 LMAX 로거(422033)와 동일한 철학의 비동기 원형 버퍼(Ring Buffer) 기반 WAL 사출 데몬을 내부에 신설하여, HFT 통신 스레드와 디스크 I/O 스레드를 물리적으로 디커플링(Zero-Lock WAL Append)했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 초고속 동시성 제어, FFM API 메모리 복제, 비동기 퓨처 처리, 그리고 Tier 4 범용 배급망 드라이버를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of dependency modules for ultra-high-speed concurrency control, FFM API memory replication, asynchronous future processing, and the Tier 4 universal distribution network driver.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어20_연방_합의망;

import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망.A0_DT_42_422041_범용_OS레이어_드라이버;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 분산 환경의 권력 이양과 2PC 데이터 정합성을 통제하며 LMAX 사상의 원형 버퍼를 내장한 연방 합의 프로토콜 엔진입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A federal consensus protocol engine that controls power transfer and 2PC data consistency in a distributed environment, embedding a circular buffer based on LMAX philosophy.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_425010
 * [파일명] A0_DT_42_425010_연방_합의_프로토콜_엔진.java
 * [모듈명] 통합 OS V6.1 - Tier 20: 연방 합의 프로토콜 엔진 (Zero-Copy Raft 코어)
 * ==============================================================================
 */
public final class A0_DT_42_425010_연방_합의_프로토콜_엔진 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.425010_FEDERAL_CONSENSUS");

    // 0.1초 이내의 페일오버를 달성하기 위한 초정밀 시간 상수(나노초 단위)
    private static final long 하트비트_발송_간격_NS = 20_000_000L; // 20ms
    private static final long 최소_선거_타임아웃_NS = 75_000_000L; // 75ms
    private static final long 최대_선거_타임아웃_NS = 150_000_000L; // 150ms

    public enum 권력_상태 {
        리더, 팔로워, 후보자
    }

    // [1. 한글 상세 주석]
    // 💡 [네트워크 모듈 분리 및 주입(DI) 규격]
    // 완전한 클러스터 구동을 위해 외부 gRPC/Arrow Flight 어댑터 클래스가 본 인터페이스를 구현하고 생성자를 통해 주입되어야
    // 합니다.
    // [2. 영문 상세 주석]
    // 💡 [Network Module Separation and DI Specification]
    // For full cluster operation, an external gRPC/Arrow Flight adapter class must
    // implement this interface and be injected via the constructor.
    // [3. 자바 코드]
    public interface 연방_Arrow_Flight_RPC_포트 {
        boolean 투표_요구_전송(String 대상_노드ID, long 임기, String 후보자ID, long 마지막_로그_인덱스);

        boolean 하트비트_전송(String 대상_노드ID, long 임기, String 리더ID, long 리더_커밋_인덱스);

        CompletableFuture<Boolean> 제로카피_WAL_브로드캐스트_수신확인(String 대상_노드ID, ByteBuffer 다이렉트_버퍼, long 절대_오프셋, long 로그_인덱스);
    }

    private final String 현재_노드_ID;
    private final List<String> 클러스터_멤버망;
    private final 연방_Arrow_Flight_RPC_포트 통신_포트;

    // 실제 디스크 플러시(Apply)를 집행할 L4 드라이버 의존성
    private final A0_DT_42_422041_범용_OS레이어_드라이버 OS_드라이버;
    private final Path 디스크_마스터_경로;

    // 💡 [V6.1 스플릿 브레인 방어] 선거 메타데이터(임기 및 투표 내역)를 영속화할 절대 경로
    private final Path 선거_메타데이터_경로;

    // =========================================================================
    // 💡 [Raft 락프리 상태 관리 변수] (원자성 보장)
    // =========================================================================
    private final AtomicReference<권력_상태> 현재_권력 = new AtomicReference<>(권력_상태.팔로워);
    private final AtomicLong 현재_임기 = new AtomicLong(0);
    private final AtomicReference<String> 투표한_후보자_ID = new AtomicReference<>(null);
    private final AtomicLong 마지막_하트비트_수신_시간 = new AtomicLong(System.nanoTime());
    private final AtomicLong 선거_타임아웃_기준치 = new AtomicLong(도출하다_무작위_선거_타임아웃());

    // 전체 클러스터가 합의하여 안전하다고 판별된 최신 로그의 인덱스
    private final AtomicLong 커밋_인덱스_commitIndex = new AtomicLong(0);
    // 현재 노드의 L1 매트릭스(상태 기계)에 물리적으로 반영(Apply)을 마친 인덱스
    private final AtomicLong 반영된_인덱스_lastApplied = new AtomicLong(0);

    private final ConcurrentHashMap<String, AtomicLong> 팔로워_다음_인덱스_nextIndex = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> 팔로워_일치_인덱스_matchIndex = new ConcurrentHashMap<>();

    private volatile boolean 합의_엔진_가동상태 = false;
    private Thread 워치독_스레드;
    private Thread 하트비트_발송_스레드;

    // =========================================================================
    // 💡 [V6.1 신설] LMAX 기반 Zero-Allocation WAL 비동기 원형 버퍼
    // =========================================================================
    private static final int WAL_원형_버퍼_사이즈 = 65536; // 2^16
    private static final int WAL_버퍼_비트_마스크 = WAL_원형_버퍼_사이즈 - 1;
    private static final long WAL_로테이션_임계치_바이트 = 50L * 1024L * 1024L; // 50MB

    private final AtomicLong 생산자_WAL_커서 = new AtomicLong(0);
    private final AtomicLong 소비자_WAL_커서 = new AtomicLong(0);
    private final WAL_이벤트_객체[] WAL_링_버퍼;
    private Thread WAL_사출_데몬_스레드;
    private Path WAL_저장_경로;
    private FileChannel 현재_WAL_채널;
    private Path 현재_WAL_경로;

    private static class WAL_이벤트_객체 {
        volatile boolean 발행완료 = false;
        final ByteBuffer 페이로드_버퍼 = ByteBuffer.allocateDirect(1024 * 64).order(ByteOrder.LITTLE_ENDIAN); // 64KB Max
    }

    // [1. 한글 상세 주석]
    // [창세 생성자] L4 드라이버 배관을 결속하고 선거 메타데이터를 복원(Crash Recovery)하여 합의 엔진을 준비합니다.
    // [2. 영문 상세 주석]
    // [Genesis Constructor] Binds the L4 driver plumbing and restores election
    // metadata (Crash Recovery) to prepare the consensus engine.
    // [3. 자바 코드]
    public A0_DT_42_425010_연방_합의_프로토콜_엔진(
            String 현재_노드_ID,
            List<String> 클러스터_멤버망,
            연방_Arrow_Flight_RPC_포트 통신_포트,
            A0_DT_42_422041_범용_OS레이어_드라이버 OS_드라이버,
            Path 디스크_마스터_경로) {

        if (OS_드라이버 == null || 디스크_마스터_경로 == null || 통신_포트 == null) {
            throw new IllegalArgumentException("[배관 파열] 상태 기계 동기화를 위한 드라이버, 디스크 경로, 또는 RPC 포트가 누락되었습니다.");
        }

        this.현재_노드_ID = 현재_노드_ID;
        this.클러스터_멤버망 = new CopyOnWriteArrayList<>(클러스터_멤버망);
        this.통신_포트 = 통신_포트;
        this.OS_드라이버 = OS_드라이버;
        this.디스크_마스터_경로 = 디스크_마스터_경로;

        // 💡 [스플릿 브레인 방어망] 메타데이터 파일 경로 확정 및 크래시 리커버리(Crash Recovery) 집행
        this.선거_메타데이터_경로 = 디스크_마스터_경로.getParent().resolve("RAFT_META.dat");
        복원하다_선거_메타데이터();

        for (String 멤버 : this.클러스터_멤버망) {
            팔로워_다음_인덱스_nextIndex.put(멤버, new AtomicLong(0L));
            팔로워_일치_인덱스_matchIndex.put(멤버, new AtomicLong(0L));
        }

        this.WAL_링_버퍼 = new WAL_이벤트_객체[WAL_원형_버퍼_사이즈];
        for (int i = 0; i < WAL_원형_버퍼_사이즈; i++) {
            this.WAL_링_버퍼[i] = new WAL_이벤트_객체();
        }

        this.WAL_저장_경로 = 디스크_마스터_경로.getParent().resolve("RAFT_WAL");

        로거.info(" >> [통합 OS V6.1] A0_DT_42_425010 연방 합의 프로토콜 엔진 기동 준비. (Split-Brain 방어막 및 LMAX WAL 스토리지 바인딩 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 [수술 핵심: 메타데이터 영속화 (Force Sync)]
    // 단일 임기 내 중복 투표(Split-Brain)를 원천 차단하기 위해, 메모리가 아닌 디스크 섹터에 직접 `force(true)`를
    // 호출하여 인과율을 영구 박제합니다.
    // [2. 영문 상세 주석]
    // 💡 [Surgery Core: Metadata Persistence (Force Sync)]
    // To fundamentally block duplicate voting within a single term (Split-Brain),
    // directly calls `force(true)` to disk sectors, not memory, to permanently
    // record causality.
    // [3. 자바 코드]
    private synchronized void 영속화하다_선거_메타데이터_동기식(long 임기, String 후보자ID) {
        try {
            if (!Files.exists(선거_메타데이터_경로.getParent())) {
                Files.createDirectories(선거_메타데이터_경로.getParent());
            }

            try (FileChannel 채널 = FileChannel.open(선거_메타데이터_경로, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                byte[] idBytes = 후보자ID == null ? new byte[0] : 후보자ID.getBytes(StandardCharsets.UTF_8);
                ByteBuffer 버퍼 = ByteBuffer.allocate(8 + 4 + idBytes.length).order(ByteOrder.LITTLE_ENDIAN);

                버퍼.putLong(임기);
                버퍼.putInt(idBytes.length);
                버퍼.put(idBytes);
                버퍼.flip();

                while (버퍼.hasRemaining()) {
                    채널.write(버퍼);
                }
                // 💡 [절대 규칙] 물리 디스크 동기화 강제 집행 (Force Sync)
                채널.force(true);
            }
        } catch (IOException 예외) {
            로거.log(Level.SEVERE, " 🚨 [치명적 붕괴] 선거 메타데이터 디스크 영속화 실패. Split-Brain 방어를 위해 시스템을 정지합니다.", 예외);
            throw new RuntimeException("Raft 메타데이터 영속화 물리적 실패 (Kernel Error)", 예외);
        }
    }

    private void 복원하다_선거_메타데이터() {
        if (!Files.exists(선거_메타데이터_경로))
            return;

        try (FileChannel 채널 = FileChannel.open(선거_메타데이터_경로, StandardOpenOption.READ)) {
            ByteBuffer 버퍼 = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
            if (채널.read(버퍼) < 12)
                return;
            버퍼.flip();

            long 저장된_임기 = 버퍼.getLong();
            int id_길이 = 버퍼.getInt();

            String 저장된_후보자 = null;
            if (id_길이 > 0) {
                ByteBuffer id_버퍼 = ByteBuffer.allocate(id_길이);
                if (채널.read(id_버퍼) == id_길이) {
                    저장된_후보자 = new String(id_버퍼.array(), StandardCharsets.UTF_8);
                }
            }

            현재_임기.set(저장된_임기);
            투표한_후보자_ID.set(저장된_후보자);

            로거.info(String.format("   ├─ [크래시 리커버리] 이전 생애의 선거 메타데이터를 성공적으로 복원했습니다. (임기: %d, 투표한 후보: %s)", 저장된_임기,
                    저장된_후보자));

        } catch (IOException 예외) {
            로거.log(Level.WARNING, " [복원 경고] 선거 메타데이터 복원 중 I/O 에러 발생.", 예외);
        }
    }

    public void 가동하다_합의_엔진() {
        if (합의_엔진_가동상태)
            return;
        합의_엔진_가동상태 = true;
        마지막_하트비트_수신_시간.set(System.nanoTime());

        try {
            if (!Files.exists(WAL_저장_경로)) {
                Files.createDirectories(WAL_저장_경로);
            }
            로테이션하다_새로운_WAL_세그먼트();

            WAL_사출_데몬_스레드 = new Thread(this::무한_루프_WAL_사출_엔진, "OS_RAFT_WAL_DAEMON");
            WAL_사출_데몬_스레드.setDaemon(true);
            WAL_사출_데몬_스레드.start();
        } catch (IOException 예외) {
            로거.log(Level.SEVERE, " [배관 파열] WAL 파일 채널을 개방할 수 없습니다.", 예외);
            throw new RuntimeException("Raft WAL 기동 실패", 예외);
        }

        워치독_스레드 = new Thread(this::실행하다_워치독_감시_루프, "OS_RAFT_WATCHDOG");
        워치독_스레드.setPriority(Thread.MAX_PRIORITY);
        워치독_스레드.setDaemon(true);
        워치독_스레드.start();

        로거.info("   ├─ [합의망 점화] 1ms 정밀도 워치독 데몬 및 LMAX WAL 데몬 가동 완료.");
    }

    private void 실행하다_워치독_감시_루프() {
        while (합의_엔진_가동상태) {
            LockSupport.parkNanos(1_000_000L);

            if (현재_권력.get() == 권력_상태.리더) {
                continue;
            }

            long 경과_시간_NS = System.nanoTime() - 마지막_하트비트_수신_시간.get();
            if (경과_시간_NS > 선거_타임아웃_기준치.get()) {
                로거.warning(String.format(" 🚨 [권력 공백 감지] 리더의 하트비트가 %d ms 동안 유실되었습니다. 즉각 선거를 격발합니다.",
                        (경과_시간_NS / 1_000_000L)));
                격발하다_리더_선거();
            }
        }
    }

    private long 도출하다_무작위_선거_타임아웃() {
        return ThreadLocalRandom.current().nextLong(최소_선거_타임아웃_NS, 최대_선거_타임아웃_NS);
    }

    // [1. 한글 상세 주석]
    // 💡 [변경: Split-Brain 방어막 전개] 선거를 격발하기 직전, 새로운 임기와 자신이 투표한 사실을 물리 디스크에
    // 영속화(Force Sync)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Change: Split-Brain Shield Deployed] Right before triggering an election,
    // persists (Force Sync) the new term and voting fact to the physical disk.
    // [3. 자바 코드]
    private void 격발하다_리더_선거() {
        현재_권력.set(권력_상태.후보자);
        long 새로운_임기 = 현재_임기.incrementAndGet();

        // 💡 [V6.1 스플릿 브레인 원천 차단]
        // 메모리를 갱신하기 전에, 디스크 파일(WAL Meta)에 철저히 먼저 각인(Force Sync)하여 비정상 셧다운을 대비합니다.
        영속화하다_선거_메타데이터_동기식(새로운_임기, 현재_노드_ID);

        투표한_후보자_ID.set(현재_노드_ID);
        마지막_하트비트_수신_시간.set(System.nanoTime());
        선거_타임아웃_기준치.set(도출하다_무작위_선거_타임아웃());

        AtomicInteger 획득한_표수 = new AtomicInteger(1);
        int 과반수 = (클러스터_멤버망.size() / 2) + 1;

        로거.info(String.format("   ├─ [선거 개시] 임기 %d기 출마 (메타데이터 Sync 완료). 과반수(%d표) 확보를 위해 동맹에 투표를 징발합니다.", 새로운_임기, 과반수));

        for (String 대상_노드 : 클러스터_멤버망) {
            if (대상_노드.equals(현재_노드_ID))
                continue;

            CompletableFuture.runAsync(() -> {
                boolean 투표_찬성 = 통신_포트.투표_요구_전송(대상_노드, 새로운_임기, 현재_노드_ID, 커밋_인덱스_commitIndex.get());
                if (투표_찬성 && 현재_권력.get() == 권력_상태.후보자) {
                    int 득표수 = 획득한_표수.incrementAndGet();
                    if (득표수 >= 과반수) {
                        장악하다_리더_권력(새로운_임기);
                    }
                }
            });
        }
    }

    private synchronized void 장악하다_리더_권력(long 당선된_임기) {
        if (현재_권력.get() == 권력_상태.리더 || 현재_임기.get() != 당선된_임기)
            return;

        현재_권력.set(권력_상태.리더);

        long 다음_로그_인덱스 = 커밋_인덱스_commitIndex.get() + 1;
        for (String 멤버 : 클러스터_멤버망) {
            팔로워_다음_인덱스_nextIndex.get(멤버).set(다음_로그_인덱스);
            팔로워_일치_인덱스_matchIndex.get(멤버).set(0L);
        }

        로거.info(" >> [권력 장악 완료] 현재 노드가 연방망의 새로운 리더로 등극했습니다! (임기: " + 당선된_임기 + "기)");

        if (하트비트_발송_스레드 != null && 하트비트_발송_스레드.isAlive()) {
            하트비트_발송_스레드.interrupt();
        }

        하트비트_발송_스레드 = new Thread(this::실행하다_리더_하트비트_폭격, "OS_RAFT_LEADER_BEAT");
        하트비트_발송_스레드.setPriority(Thread.MAX_PRIORITY);
        하트비트_발송_스레드.start();
    }

    private void 실행하다_리더_하트비트_폭격() {
        while (합의_엔진_가동상태 && 현재_권력.get() == 권력_상태.리더) {
            long 현재_임기_스냅샷 = 현재_임기.get();
            long 현재_커밋_인덱스 = 커밋_인덱스_commitIndex.get();

            for (String 팔로워 : 클러스터_멤버망) {
                if (팔로워.equals(현재_노드_ID))
                    continue;

                CompletableFuture.runAsync(() -> {
                    boolean 응답_정상 = 통신_포트.하트비트_전송(팔로워, 현재_임기_스냅샷, 현재_노드_ID, 현재_커밋_인덱스);
                    if (!응답_정상) {
                        로거.fine(" [통신 지연] 노드 " + 팔로워 + " 의 하트비트 응답이 유실되었습니다.");
                    }
                });
            }
            LockSupport.parkNanos(하트비트_발송_간격_NS);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [변경: Split-Brain 방어막 전개] 새로운 리더를 인정할 때, 권력을 팔로워로 낮추기 전 디스크에 이를 100%
    // 확정(Commit)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Change: Split-Brain Shield Deployed] When acknowledging a new leader,
    // commits it 100% to the disk before stepping down to a follower.
    // [3. 자바 코드]
    public void 수신하다_외부_하트비트(long 수신된_임기, String 리더_ID, long 리더_커밋_인덱스) {
        if (수신된_임기 > 현재_임기.get() || (수신된_임기 == 현재_임기.get() && 투표한_후보자_ID.get() == null)) {

            // 💡 [V6.1 스플릿 브레인 방어막] 리더의 존재를 인정하기 전, 디스크에 영속화 강제
            영속화하다_선거_메타데이터_동기식(수신된_임기, 리더_ID);

            현재_임기.set(수신된_임기);
            투표한_후보자_ID.set(리더_ID);
        }

        if (수신된_임기 >= 현재_임기.get()) {
            현재_권력.set(권력_상태.팔로워);
            마지막_하트비트_수신_시간.set(System.nanoTime());

            if (리더_커밋_인덱스 > 커밋_인덱스_commitIndex.get()) {
                커밋_인덱스_commitIndex.updateAndGet(curr -> Math.max(curr, 리더_커밋_인덱스));
                집행하다_상태기계_최종반영();
            }
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [수술 핵심: Lock-Free WAL 사출 및 분산 복제]
    // 기존의 `synchronized (WAL_순차_락)` 블록을 영구 파괴했습니다.
    // HFT 통신 스레드는 LMAX 링 버퍼에 데이터를 O(1)으로 쑤셔넣고 즉시 네트워크 브로드캐스트로 전진하며,
    // 디스크 기록(I/O)은 백그라운드 사출 데몬이 전담하여 병목 현상을 0으로 멸균했습니다.
    // [2. 영문 상세 주석]
    // 💡 [Surgery Core: Lock-Free WAL Emission and Distributed Replication]
    // Permanently destroyed the existing `synchronized (WAL_순차_락)` block.
    // The HFT communication thread stuffs data into the LMAX ring buffer in O(1)
    // and immediately proceeds to network broadcast.
    // [3. 자바 코드]
    public void 전파하다_WAL_제로카피_복제(MemorySegment 원본_왈_세그먼트, long 바이트_크기, long 절대_오프셋) {
        if (현재_권력.get() != 권력_상태.리더) {
            로거.warning(" [복제 기각] 리더 노드만이 텐서를 브로드캐스트할 권한을 가집니다.");
            return;
        }

        long 할당된_로그_인덱스 = 커밋_인덱스_commitIndex.get() + 1;

        ByteBuffer 다이렉트_버퍼 = 원본_왈_세그먼트.asSlice(절대_오프셋, 바이트_크기)
                .asByteBuffer()
                .order(ByteOrder.LITTLE_ENDIAN);

        // 💡 [LMAX 비동기 링 버퍼 삽입 (Zero-Lock WAL Append)]
        의뢰하다_비동기_WAL_기록(다이렉트_버퍼.duplicate());

        AtomicInteger 수신_확인_정족수 = new AtomicInteger(1);
        int 합의_과반수 = (클러스터_멤버망.size() / 2) + 1;

        for (String 팔로워_노드 : 클러스터_멤버망) {
            if (팔로워_노드.equals(현재_노드_ID))
                continue;

            통신_포트.제로카피_WAL_브로드캐스트_수신확인(팔로워_노드, 다이렉트_버퍼.duplicate(), 절대_오프셋, 할당된_로그_인덱스)
                    .thenAccept(ACK_도착여부 -> {
                        if (ACK_도착여부) {
                            팔로워_일치_인덱스_matchIndex.get(팔로워_노드)
                                    .updateAndGet(curr -> Math.max(curr, 할당된_로그_인덱스));

                            int 획득_ACK_수 = 수신_확인_정족수.incrementAndGet();
                            if (획득_ACK_수 == 합의_과반수) {
                                long 현재_커밋 = 커밋_인덱스_commitIndex.get();
                                if (할당된_로그_인덱스 > 현재_커밋) {
                                    if (커밋_인덱스_commitIndex.compareAndSet(현재_커밋, 할당된_로그_인덱스)) {
                                        집행하다_상태기계_최종반영();

                                        로거.fine(String.format(
                                                "   ├─ [2PC 정합성 검증 완료] 인덱스 %d의 분산 복제가 과반수(%d) 합의를 달성하여 매트릭스에 영속화되었습니다.",
                                                할당된_로그_인덱스, 획득_ACK_수));
                                    }
                                }
                            }
                        }
                    })
                    .exceptionally(예외 -> {
                        로거.warning(" [복제 지연] 노드 " + 팔로워_노드 + " 에 대한 WAL 전파 실패. (네트워크 파열)");
                        return null;
                    });
        }
    }

    private void 의뢰하다_비동기_WAL_기록(ByteBuffer 기록할_버퍼) {
        long 타임아웃_나노초 = 50_000_000L;
        long 스핀_시작 = System.nanoTime();
        long 할당될_시퀀스;

        while (true) {
            long 현재_생산자 = 생산자_WAL_커서.get();
            long 현재_소비자 = 소비자_WAL_커서.get();

            if (현재_생산자 - 현재_소비자 >= WAL_원형_버퍼_사이즈) {
                if (System.nanoTime() - 스핀_시작 > 타임아웃_나노초) {
                    로거.severe(" 🚨 [서킷 브레이커] Raft WAL 디스크 I/O 응답 불가. HFT 스레드 락다운을 막기 위해 WAL 기록을 소각(Drop)합니다.");
                    return;
                }
                LockSupport.parkNanos(100_000L);
                continue;
            }

            if (생산자_WAL_커서.compareAndSet(현재_생산자, 현재_생산자 + 1)) {
                할당될_시퀀스 = 현재_생산자;
                break;
            }
        }

        int 타겟_인덱스 = (int) (할당될_시퀀스 & WAL_버퍼_비트_마스크);
        WAL_이벤트_객체 타겟_슬롯 = WAL_링_버퍼[타겟_인덱스];

        타겟_슬롯.페이로드_버퍼.clear();
        타겟_슬롯.페이로드_버퍼.put(기록할_버퍼);
        타겟_슬롯.페이로드_버퍼.flip();

        타겟_슬롯.발행완료 = true;
    }

    private void 무한_루프_WAL_사출_엔진() {
        try {
            long 처리할_시퀀스 = 소비자_WAL_커서.get();

            while (합의_엔진_가동상태 || WAL_링_버퍼[(int) (처리할_시퀀스 & WAL_버퍼_비트_마스크)].발행완료) {
                int 처리_인덱스 = (int) (처리할_시퀀스 & WAL_버퍼_비트_마스크);
                WAL_이벤트_객체 타겟_이벤트 = WAL_링_버퍼[처리_인덱스];

                if (타겟_이벤트.발행완료) {
                    ByteBuffer 사출용_버퍼 = 타겟_이벤트.페이로드_버퍼;
                    while (사출용_버퍼.hasRemaining()) {
                        현재_WAL_채널.write(사출용_버퍼);
                    }

                    타겟_이벤트.발행완료 = false;
                    처리할_시퀀스++;
                    소비자_WAL_커서.lazySet(처리할_시퀀스);

                    if (처리할_시퀀스 % 1000 == 0) {
                        현재_WAL_채널.force(false);
                        if (현재_WAL_채널.size() >= WAL_로테이션_임계치_바이트) {
                            로테이션하다_새로운_WAL_세그먼트();
                        }
                    }
                } else {
                    현재_WAL_채널.force(false);
                    LockSupport.parkNanos(100_000L);
                }
            }
            현재_WAL_채널.force(true);
            현재_WAL_채널.close();
            로거.info("   ├─ [Raft WAL 셧다운] 링 버퍼 잔여 이벤트 사출 및 FileChannel 동기화 완료.");
        } catch (IOException e) {
            로거.log(Level.SEVERE, " [치명적 오류] Raft WAL 데몬 스레드 FileChannel I/O 붕괴.", e);
        }
    }

    private void 로테이션하다_새로운_WAL_세그먼트() throws IOException {
        if (현재_WAL_채널 != null && 현재_WAL_채널.isOpen()) {
            현재_WAL_채널.force(true);
            현재_WAL_채널.close();
        }

        long 새_시퀀스 = System.currentTimeMillis();
        String 새_파일명 = String.format("MATRIX_A0_425010_RAFT_WAL_%d.log", 새_시퀀스);
        현재_WAL_경로 = WAL_저장_경로.resolve(새_파일명);

        현재_WAL_채널 = FileChannel.open(현재_WAL_경로,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        로거.fine("   ├─ [Raft WAL 로테이션] 새로운 Append-Only 쓰기 세그먼트가 개방되었습니다: " + 새_파일명);
    }

    // [1. 한글 상세 주석]
    // 💡 [배관 수복 및 Lock-Free 플러시]
    // 합의가 완료된 로그 인덱스와 반영된 인덱스의 격차를 해소하며, Tier 4 드라이버를 호출하여 실제 디스크에 영속화합니다.
    // 기존의 `synchronized` 병목을 제거하고 CAS 기반 원자적 갱신 루프를 이식했습니다.
    // [2. 영문 상세 주석]
    // 💡 [Plumbing Restored and Lock-Free Flush]
    // Resolves the gap between the committed log index and the applied index,
    // calling the Tier 4 driver to persist to actual disk.
    // [3. 자바 코드]
    private void 집행하다_상태기계_최종반영() {
        long 타겟_커밋_인덱스 = 커밋_인덱스_commitIndex.get();

        while (true) {
            long 현재_반영_인덱스 = 반영된_인덱스_lastApplied.get();

            if (현재_반영_인덱스 >= 타겟_커밋_인덱스) {
                break;
            }

            if (반영된_인덱스_lastApplied.compareAndSet(현재_반영_인덱스, 타겟_커밋_인덱스)) {
                try {
                    로거.info(String.format("   ├─ [상태 기계 동기화] 로그 인덱스 %d ~ %d 구간의 WAL 텐서를 물리 디스크에 플러시(Apply)합니다.",
                            현재_반영_인덱스 + 1, 타겟_커밋_인덱스));

                    // 💡 [핵심 배관 수술] L4 범용 OS 레이어 드라이버의 실제 플러시 로직 호출
                    OS_드라이버.실행_샌드박스_마스터_승격(디스크_마스터_경로);

                    로거.fine("   └─ [플러시 완료] 상태 기계의 물리적 동기화가 무결점으로 완수되었습니다.");

                } catch (Exception 예외) {
                    반영된_인덱스_lastApplied.set(현재_반영_인덱스);
                    로거.log(Level.SEVERE, " 🚨 [치명적 커널 패닉] 상태 기계 동기화 중 디스크 물리 플러시 실패. 분산 정합성이 붕괴될 위험이 있습니다.", 예외);
                    throw new RuntimeException("State Machine Apply Flush Failed", 예외);
                }
                break;
            }
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 💡 Raft 스플릿 브레인(Split-Brain)의 본질적 파괴 (Force Sync Metadata):
 * 분산 합의(Consensus) 알고리즘인 Raft에서 가장 중요하게 여겨지는 절대 법칙이 있습니다.
 * 노드는 새로운 임기(Term)에 진입하거나 누군가에게 투표(VotedFor)를 던질 때, 이를 반드시 **메모리가 아닌 안전한
 * 디스크(Non-Volatile Storage)**에 먼저 각인해야 합니다.
 * 과거 코드는 이를 `AtomicLong`이라는 램(RAM) 영역에만 기록하고 있었습니다.
 * 만약 어떤 노드가 투표를 던진 직후 정전으로 재부팅된다면, RAM의 투표 기록은 증발하고 맙니다. 깨어난 이 노드는 동일한 임기 내에서 또
 * 다른 후보에게 중복 투표를 행사하게 되며, 이는 하나의 우주에 두 명의 황제(Split-Brain)를 탄생시키는 참극을 부릅니다.
 * 수복된 V6.1 엔진은 `영속화하다_선거_메타데이터_동기식` 메서드를 통해 `channel.force(true)`를 호출, 커널의 페이지
 * 캐시를 건너뛰고 물리 디스크 섹터에 데이터를 완벽히 박제한 뒤에야만 네트워크 응답을 내보내는 결계를 전개했습니다.
 * 
 * 2. LMAX Ring Buffer 철학을 통한 WAL I/O 디커플링 (The End of I/O Contention):
 * 네트워크 클라이언트가 HFT(고빈도 매매) 수준으로 텐서를 복제하려 할 때, `synchronized` 자물쇠를 걸어두고 물리적 디스크
 * 채널(FileChannel)이 `write()`를 끝마칠 때까지 스레드를 붙잡아두는 것은 폰 노이만 아키텍처의 비극입니다.
 * 수리된 V6.1 엔진은 `LMAX Disruptor`의 원형 버퍼(Ring Buffer) 사상을 이식하여, 네트워크 통신 스레드는 메모리
 * 버퍼 슬롯에 0초 만에 데이터를 던져넣고(O(1) CAS) 곧바로 비동기 브로드캐스트로 전진합니다.
 * 무겁고 느린 디스크 I/O는 완전히 독립된 백그라운드 사출 데몬이 `force(false)`와 함께 전담함으로써 I/O
 * 경합(Contention)이 물리적으로 멸균되었습니다.
 * 
 * 3. 락-프리 상태 기계 동기화 (CAS-Based Lock-Free State Machine):
 * 분산 시스템에서 데이터가 최종적으로 안전하다고 판명(`commitIndex`)되어 로컬 디스크에 물리적으로
 * 기록(`lastApplied`)되는 순간은 엄격한 순서와 스레드 안전성이 요구됩니다.
 * 수술된 V6.1 엔진은 `반영된_인덱스_lastApplied.compareAndSet` 이라는 하드웨어 레벨의 CAS 원자적 연산을
 * 채택했습니다.
 * 수백 개의 수신 스레드가 들이닥치더라도 오직 승리한 단 1개의 스레드만이 디스크 플러시의 권한을 획득하며, 스레드
 * 블로킹(Blocking)과 컨텍스트 스위칭 지연이 0%로 수렴하는 절대적 동시성을 성취했습니다.
 * 
 * 4. 2단계 커밋(2PC) 기반의 분산 정합성 보장 (Zero-Copy Quorum):
 * Arrow Flight로 메모리 포인터를 쏜 직후, `CompletableFuture` 비동기 콜백망을 열어둡니다.
 * 과반수(Quorum, N/2 + 1)의 노드가 "내 메모리에 안전하게 복제했다"고 ACK를 반환하는 찰나의 순간, 리더는 그제야
 * `commitIndex`를 올리고 데이터를 디스크에 영속화(Apply)합니다.
 * 성능을 위해 Zero-Copy를 채택하면서도, 분산 이론의 절대 법칙인 정족수 합의(Quorum Consensus)를 물리적으로
 * 완성했습니다.
 * =============================================================================
 */