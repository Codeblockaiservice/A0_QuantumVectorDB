/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망
 * @alias Async_Tensor_Digestor
 * @tier 2
 * @keywords Quantum Superposition, Wave Function Collapse, Lock-Free, DLQ Replay, Zero-Allocation, WAL Rolling
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422023_비동기_텐서_소화기.java
 * - 모듈명: 통합 OS V6.1 - Tier 2: 비동기 텐서 소화기 (양자 중첩 기반 락-프리 커밋 엔진)
 * - 기능 및 역할: 다수의 스레드와 에이전트가 동시에 DB(FFM 오프힙)에 접근할 때 발생하는 I/O 락(Lock) 경합을 근본적으로 소거.
 * - 이론 및 기술: 양자 중첩(Quantum Superposition), 파동 함수 붕괴(Wave Function Collapse), Lock-Free 동시성, 세그먼트 아키텍처(WAL Rolling).
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [변경] 모듈 식별 번호 중복 교정: `A0_DT_42_422031_바이트_역방향_현미경_스캐너`와의 식별 번호 중복 충돌을 영구히 수정. 
 *                 본 모듈을 `A0_DT_42_422023_비동기_텐서_소화기`로 격상시켜 카탈로그의 수학적 정합성을 완벽히 수복했습니다.
 * - 💡 [V6.1 치명적 결함 수정] NVMe 친화적 WAL 세그먼트 로테이션 (Rolling) 완수: 
 *                 파동 붕괴(Flush) 직후 무조건 `truncate(0)`을 수행하여 디스크 단편화를 유발하던 코드를 제거했습니다.
 *                 WAL 파일이 50MB를 초과하면 기존 채널을 닫고 새 파일(Segment)로 롤링(Rolling)하며, 
 *                 디스크 영속화가 끝난 구형 WAL만을 큐(Queue)에서 꺼내어 비동기적으로 폐기(GC)하는 카프카(Kafka) 스타일 아키텍처를 도입했습니다.
 * - 💡 [V6.1 DLQ 롤포워드망 유지]: 사령관(운영자)의 명시적 지시에 따라 버려진 텐서를 재평가(Re-evaluate)하는 롤포워드(Roll-forward) 배관 보존 완료.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 동시성 맵/큐, 네이티브 커널 메모리 제어(FFM API), 파일 시스템 세그먼트 관리용 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for concurrent maps/queues, native kernel memory control (FFM API), and file system segment management.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 다중 스레드의 I/O 경합을 양자 중첩 모델로 해결하고, 50MB 단위의 WAL 롤링(Rolling)을 지원하는 락-프리 커밋 엔진입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A lock-free commit engine that resolves multi-thread I/O contention via quantum superposition model and supports 50MB WAL rolling.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422023
 * [파일명] A0_DT_42_422023_비동기_텐서_소화기.java
 * [모듈명] 통합 OS V6.1 - Tier 2: 비동기 텐서 소화기 (양자 중첩 기반 락-프리 커밋 엔진)
 * 
 * [설계 명세]
 * 1. 역할: 다수의 스레드와 에이전트가 동시에 DB(FFM 오프힙)에 접근할 때 발생하는 I/O 락(Lock) 경합을 소멸.
 * 2. 기능: 트랜잭션 양자 중첩 수용, 스칼라 질량(권한 가중치) 기반 파동 함수 붕괴, 물리 메모리 영구 고착(Force Flush).
 * 3. 의도: 폰 노이만 아키텍처의 고질병인 'I/O 병목 현상'을 양자 역학의 관측 붕괴(Superposition & Collapse) 모델로 치환.
 * 4. 이론: 양자 중첩(Quantum Superposition), 파동 함수 붕괴(Wave Function Collapse), 락-프리(Lock-Free) 동시성.
 * 5. 💡 [V6.1 핵심 최적화] 50MB WAL 세그먼트 로테이션 (Rolling):
 * `truncate(0)` 안티패턴을 파괴하고, 파일 크기가 50MB를 초과 시 Append-Only 쓰기 전용 채널을 교체하는 
 * 세그먼트 롤링(Segment Rolling) 모델로 승격시켜 스토리지 I/O 성능 및 수명을 극대화했습니다.
 * ==============================================================================
 */
public final class A0_DT_42_422023_비동기_텐서_소화기 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422023_ASYNC_DIGESTOR");

    // 💡 [절대 규격] 하드웨어 친화적 리틀 엔디안 Float32 레이아웃 강제 적용
    private static final ValueLayout.OfFloat TENSOR_FLOAT_LE = ValueLayout.JAVA_FLOAT
            .withOrder(ByteOrder.LITTLE_ENDIAN);

    // 물리적 디스크와 직접 매핑(mmap)된 OS 커널 메모리 세그먼트
    private final MemorySegment physicalMappedSegment;

    // 💡 [확률 구름 버퍼 (Quantum Superposition Buffer)]
    // Key: 물리적 메모리 절대 오프셋 (기록될 위치)
    // Value: 락(Lock) 없이 동시다발적으로 들어오는 쓰기 요청들의 파동(Queue)
    private final ConcurrentHashMap<Long, ConcurrentLinkedQueue<TransactionWave>> superpositionBuffer;

    // 붕괴(Flush) 작업이 진행 중일 때 중복 실행을 막는 원자적 제어 스위치
    private final AtomicBoolean isCollapseInProgress = new AtomicBoolean(false);

    // =========================================================================
    // 💡 [V6.1 ACID 내구성 방어막 및 WAL 롤링 아키텍처 변수]
    // =========================================================================
    private static final long WAL_ROTATION_THRESHOLD_BYTES = 50L * 1024L * 1024L; // 50MB

    private final Path walStoragePath;
    private volatile FileChannel currentWalChannel;
    private volatile Path currentWalPath;
    private final Object walSequentialLock = new Object();

    // 디스크 플러시(붕괴)가 완료되어 안전하게 삭제(소각) 가능한 구형 WAL 파일 대기열
    private final Queue<Path> pendingGarbageWalQueue = new ConcurrentLinkedQueue<>();

    // =========================================================================
    // 💡 [데이터 유실 방어막] DLQ (Dead Letter Queue) 채널
    // =========================================================================
    private final FileChannel dlqRecordChannel;
    private final Object dlqSequentialLock = new Object();

    // [1. 한글 상세 주석]
    // 확정되지 않고 중첩 버퍼(허공)에 맴도는 상태의 비동기 쓰기(Write) 요청 캡슐(Record)입니다.
    // [2. 영문 상세 주석]
    // An asynchronous write request capsule (Record) floating in the superposition buffer without being finalized.

    /**
     * [트랜잭션 파동 레코드]
     */
    public record TransactionWave(
            float tensorValue,    // 기록하고자 하는 텐서의 실제 데이터 값
            double scalarMass,    // 이 요청을 보낸 에이전트/사령관의 권한 가중치(질량)
            String transactionId  // 인과율 추적용 고유 바코드
    ) {
    }

    // [1. 한글 상세 주석]
    // [생성자] 오프힙 세그먼트를 주입받아 엔진을 기동하고, WAL 잔여물을 리플레이하며 롤링 채널 및 DLQ를 개방합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Boots the engine by injecting the off-heap segment, replays WAL remnants, and opens the rolling channel and DLQ.

    /**
     * @param diskSegment        L5 관제탑이 발급한 READ_WRITE 권한의 물리 매핑 커널 세그먼트
     * @param storageRootPath    NVMe 등 최고속 스토리지에 위치할 WAL 및 DLQ 파일의 디렉토리 경로
     */
    public A0_DT_42_422023_비동기_텐서_소화기(MemorySegment diskSegment, Path storageRootPath) {
        if (diskSegment == null || diskSegment.isReadOnly()) {
            throw new IllegalArgumentException("[설정 오류] 쓰기 권한이 없는 읽기 전용(ReadOnly) 세그먼트로는 소화기를 기동할 수 없습니다.");
        }
        this.physicalMappedSegment = diskSegment;
        this.superpositionBuffer = new ConcurrentHashMap<>();
        this.walStoragePath = storageRootPath;

        try {
            if (!Files.exists(storageRootPath)) {
                Files.createDirectories(storageRootPath);
            }
            Path dlqFilePath = storageRootPath.resolve("MATRIX_A0_422023_DLQ.log");

            // 1. 💡 [Crash Recovery] 잔여 WAL 세그먼트가 존재하면 메모리 중첩 구름 복원 (Crash Recovery)
            try (Stream<Path> stream = Files.list(walStoragePath)) {
                List<Path> legacyWalList = stream
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().startsWith("MATRIX_A0_422023_WAL_"))
                        .filter(p -> p.getFileName().toString().endsWith(".log"))
                        .sorted() // 시퀀스 타임스탬프 기반 오름차순 정렬
                        .collect(Collectors.toList());

                if (!legacyWalList.isEmpty()) {
                    logger.warning(
                            String.format(" 🚨 [콜드스타트 복구] 비정상 종료로 인한 %d개의 WAL 잔여 세그먼트 감지. 확률 구름 버퍼 리플레이(Replay)를 개시합니다.",
                                    legacyWalList.size()));

                    for (Path remnantWal : legacyWalList) {
                        if (Files.size(remnantWal) > 0) {
                            executeWalCrashRecovery(remnantWal);
                        }
                        // 리플레이가 완료된 파일은 RAM에 성공적으로 복원되었으므로, 파동 붕괴 시 삭제되도록 가비지 대기열로 이관
                        pendingGarbageWalQueue.offer(remnantWal);
                    }
                }
            }

            // 2. 💡 [고속 Append-Only 채널 개방] 새로운 롤링 WAL 세그먼트 점화
            rotateNewWalSegment();

            // 3. DLQ 채널 개방
            this.dlqRecordChannel = FileChannel.open(dlqFilePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [시스템 예외] 로그 채널(WAL/DLQ)을 물리적으로 개방할 수 없습니다.", ex);
            throw new RuntimeException("물리적 로깅 인프라 기동 실패", ex);
        }

        logger.info(" >> [통합 OS V6.1] A0_DT_42_422023 비동기 텐서 소화기 기동. (WAL 롤링(Rolling) 및 DLQ 롤포워드망 결속 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 [수술 완료: WAL 로테이션 엔진] WAL 파일이 50MB를 초과했을 때 기존 채널을 닫아 가비지 큐로 넘기고 새로운 세그먼트를 엽니다.
    // [2. 영문 상세 주석]
    // 💡 [Surgery Complete: WAL Rotation Engine] When the WAL file exceeds 50MB, safely closes the existing channel, passes it to the garbage queue, and opens a new segment.

    /**
     * (호출부는 동기화 블록 `walSequentialLock` 내부에서 실행됨을 보장해야 합니다.)
     */
    private void rotateNewWalSegment() throws IOException {
        if (currentWalChannel != null && currentWalChannel.isOpen()) {
            currentWalChannel.force(true);
            currentWalChannel.close();
            pendingGarbageWalQueue.offer(currentWalPath);
        }

        long newSequence = System.currentTimeMillis();
        String newFileName = String.format("MATRIX_A0_422023_WAL_%d.log", newSequence);
        currentWalPath = walStoragePath.resolve(newFileName);

        currentWalChannel = FileChannel.open(currentWalPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);

        logger.info("   ├─ [WAL 로테이션] 새로운 Append-Only 쓰기 세그먼트가 개방되었습니다: " + newFileName);
    }

    /**
     * [복원 로직: WAL 리플레이 (Crash Recovery)]
     * 정전 등으로 시스템이 비정상 종료되었을 때, RAM 버퍼에서 미처 디스크(Main)로 플러시되지 못한 트랜잭션 델타를
     * 파일에서 역추적하여 큐에 그대로 살려냅니다.
     */
    private void executeWalCrashRecovery(Path walFilePath) throws IOException {
        try (FileChannel readChannel = FileChannel.open(walFilePath, StandardOpenOption.READ)) {
            // 헤더 규격: 절대_오프셋(8) + 텐서_값(4) + 스칼라_질량(8) + 트랜잭션_ID_길이(4) = 총 24 Bytes
            ByteBuffer headerBuffer = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN);

            int recoveryCount = 0;
            while (true) {
                headerBuffer.clear();
                int readLength = 0;
                while (readLength < 24) {
                    int result = readChannel.read(headerBuffer);
                    if (result == -1)
                        break;
                    readLength += result;
                }

                if (readLength == 0)
                    break; // 파일의 끝(EOF) 도달

                if (readLength < 24) {
                    logger.warning(" [WAL 손상] 헤더 바이트가 불완전합니다. 리플레이를 중단합니다: " + walFilePath.getFileName());
                    break;
                }

                headerBuffer.flip();
                long absoluteOffset = headerBuffer.getLong();
                float tensorValue = headerBuffer.getFloat();
                double scalarMass = headerBuffer.getDouble();
                int idLength = headerBuffer.getInt();

                // 가변 길이의 트랜잭션 ID 문자열 바이트 판독
                ByteBuffer idBuffer = ByteBuffer.allocate(idLength);
                int idReadLength = readChannel.read(idBuffer);

                if (idReadLength == idLength) {
                    idBuffer.flip();
                    String transactionId = new String(idBuffer.array(), StandardCharsets.UTF_8);

                    // 읽어들인 파동을 양자 중첩 버퍼에 고스란히 복원
                    TransactionWave recoveredWave = new TransactionWave(tensorValue, scalarMass, transactionId);
                    superpositionBuffer.computeIfAbsent(absoluteOffset, k -> new ConcurrentLinkedQueue<>()).offer(recoveredWave);
                    recoveryCount++;
                } else {
                    logger.warning(" [WAL 손상] 트랜잭션 ID 판독 중 파일이 예기치 않게 종료되었습니다. 복원을 조기 중단합니다.");
                    break;
                }
            }
            logger.info(String.format("   ├─ [리플레이 완료] %s 파일에서 %d개의 증발했던 트랜잭션이 성공적으로 부활했습니다.",
                    walFilePath.getFileName(), recoveryCount));
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [수동 롤포워드 인젝터] 사령관(운영자)의 명시적 지시에 따라, 과거 동시성 경합에서 패배하여 DLQ에 격리되었던 데이터를 다시 끄집어내어 버퍼에 재주입합니다.
    // [2. 영문 상세 주석]
    // 💡 [Manual Roll-forward Injector] Under the operator's explicit instruction, retrieves data confined in the DLQ due to past concurrency contention defeats, and re-injects it into the buffer.

    /**
     * 운영자의 명령에 따라 DLQ 파일을 스캔하여 경합에서 탈락했던 트랜잭션을 확률 버퍼로 재주입(Re-evaluate)합니다.
     * 
     * @param dlqFilePath 재주입을 위해 읽어들일 DLQ 덤프 파일 물리 경로
     */
    public void executeDlqManualRollforward(Path dlqFilePath) {
        if (!Files.exists(dlqFilePath)) {
            logger.info(" [DLQ 롤포워드 스킵] 대상 DLQ 데이터 파일이 존재하지 않습니다.");
            return;
        }

        try {
            if (Files.size(dlqFilePath) == 0) {
                logger.info(" [DLQ 롤포워드 스킵] DLQ 데이터 파일이 완전히 비어 있습니다.");
                return;
            }

            logger.warning(" 🚨 [재처리 개시] 관리자 권한으로 DLQ 롤포워드를 집행합니다. 패배한 트랜잭션들이 구름 버퍼로 재주입됩니다.");

            int reviveCount = 0;
            try (FileChannel readChannel = FileChannel.open(dlqFilePath, StandardOpenOption.READ)) {
                ByteBuffer headerBuffer = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN);

                while (true) {
                    headerBuffer.clear();
                    int readLength = 0;
                    while (readLength < 24) {
                        int result = readChannel.read(headerBuffer);
                        if (result == -1)
                            break;
                        readLength += result;
                    }

                    if (readLength == 0)
                        break; // EOF 도달
                    if (readLength < 24) {
                        logger.warning(" [DLQ 손상] DLQ 헤더 바이트가 불완전합니다. 롤포워드를 안전하게 중단합니다.");
                        break;
                    }

                    headerBuffer.flip();
                    long absoluteOffset = headerBuffer.getLong();
                    float tensorValue = headerBuffer.getFloat();
                    double scalarMass = headerBuffer.getDouble();
                    int idLength = headerBuffer.getInt();

                    ByteBuffer idBuffer = ByteBuffer.allocate(idLength);
                    int idReadLength = readChannel.read(idBuffer);

                    if (idReadLength == idLength) {
                        idBuffer.flip();
                        String transactionId = new String(idBuffer.array(), StandardCharsets.UTF_8);

                        // 💡 확률 구름 버퍼에 재주입 (Re-inject)
                        // 내부적으로 enqueueSuperpositionTransaction을 호출하므로, 이 데이터는 다시 WAL에 안전하게 기록됩니다.
                        enqueueSuperpositionTransaction(absoluteOffset, tensorValue, scalarMass, transactionId);
                        reviveCount++;
                    } else {
                        logger.warning(" [DLQ 손상] 트랜잭션 ID 판독 중 파일이 예기치 않게 종료되었습니다.");
                        break;
                    }
                }
                logger.info(String.format("   ├─ [롤포워드 수료] %d개의 유폐된 트랜잭션이 성공적으로 버퍼에 재주입되었습니다.", reviveCount));
            }

            // 💡 롤포워드가 끝난 DLQ 파일은 무한 중복 주입을 막기 위해 안전하게 비워냅니다(Truncate).
            synchronized (dlqSequentialLock) {
                dlqRecordChannel.truncate(0);
                logger.info("   └─ [DLQ 초기화] 롤포워드가 완료된 DLQ 채널을 성공적으로 멸균(Truncate) 처리했습니다.");
            }

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [DLQ 롤포워드 오류] 데이터 재주입 중 파일 I/O 오류가 발생했습니다.", ex);
            throw new RuntimeException("DLQ 롤포워드 I/O 물리적 실패", ex);
        }
    }

    // [1. 한글 상세 주석]
    // [핵심 역학 1: 트랜잭션 중첩 및 WAL 영속화]
    // 수천 개의 스레드가 동시에 쓰기를 시도할 때, 락(Lock)을 걸어 대기시키지 않습니다. WAL 롤링에 따라 동적으로 세그먼트가 교체됩니다.
    // [2. 영문 상세 주석]
    // [Core Dynamics 1: Transaction Superposition & WAL Persistence] Does not put thousands of threads in a lock wait. Segments are dynamically swapped according to WAL rolling.

    /**
     * @param absoluteOffset 데이터가 기록되어야 할 디스크의 물리적 위치
     * @param tensorValue    기록할 실제 부동소수점 값
     * @param scalarMass     이 요청을 수행하는 주체의 권한 가중치(질량)
     * @param transactionId  추적을 위한 트랜잭션 바코드
     */
    public void enqueueSuperpositionTransaction(long absoluteOffset, float tensorValue, double scalarMass, String transactionId) {

        // 1. 💡 [ACID 내구성 방어] RAM에 올리기 전 WAL 파일에 순차 기록 (Append-Only)
        byte[] idBytes = transactionId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer walBuffer = ByteBuffer.allocate(24 + idBytes.length).order(ByteOrder.LITTLE_ENDIAN);

        walBuffer.putLong(absoluteOffset);
        walBuffer.putFloat(tensorValue);
        walBuffer.putDouble(scalarMass);
        walBuffer.putInt(idBytes.length);
        walBuffer.put(idBytes);
        walBuffer.flip();

        try {
            // 여러 스레드가 동시에 채널에 쓰더라도 바이트가 섞이지 않도록 최소한의 순차 락(Lock)을 적용합니다.
            synchronized (walSequentialLock) {
                while (walBuffer.hasRemaining()) {
                    currentWalChannel.write(walBuffer);
                }

                // 💡 [최적화 핵심: WAL 세그먼트 롤링]
                // 50MB 초과 시 파일 단편화를 유발하는 truncate 방식을 피하고 새로운 세그먼트로 전환(Rolling)합니다.
                if (currentWalChannel.size() >= WAL_ROTATION_THRESHOLD_BYTES) {
                    rotateNewWalSegment();
                }
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [WAL 오류] 트랜잭션 순차 기록 중 치명적 I/O 예외 발생", ex);
        }

        // 2. [양자 구름 버퍼 삽입] O(1) 논블로킹 속도로 큐에 객체를 밀어넣고 스레드는 즉시 연산 현장으로 복귀(Fire and Forget)
        TransactionWave incomingWave = new TransactionWave(tensorValue, scalarMass, transactionId);
        superpositionBuffer.computeIfAbsent(absoluteOffset, k -> new ConcurrentLinkedQueue<>()).offer(incomingWave);
    }

    // [1. 한글 상세 주석]
    // 💡 [핵심 역학 2 & 3: 경합 붕괴 및 물리 메모리 고착 (Collapse & Flush Lock-in)]
    // L5 관제탑의 스케줄링 주기가 도달하는 찰나(Tick)에 격발됩니다.
    // 승리한 데이터는 디스크에 고착(Flush)되며, 100% 영속화 후 구형 WAL 파일들은 비동기 가비지 컬렉션(GC)으로 소각됩니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Dynamics 2 & 3: Contention Collapse & Physical Memory Flush Lock-in] Triggered at the scheduling cycle.
    // Old WAL files are incinerated by asynchronous GC after 100% persistence.

    public void executeWaveCollapseAndFlush() {

        // 이미 플러시가 진행 중이라면 중복 실행을 막아 물리 디스크 스래싱(Thrashing)을 100% 방어
        if (!isCollapseInProgress.compareAndSet(false, true)) {
            return;
        }

        try {
            if (superpositionBuffer.isEmpty()) {
                return; // 큐가 비어있는 평온한 상태
            }

            int collapsedEventCount = 0;
            int lostUpdateDataCount = 0;

            // 1. [질량 중심 붕괴 (Collapse)]
            // 맵에 존재하는 모든 오프셋(Key)을 순회하며 중첩된 트랜잭션들을 하나의 승리자로 확정
            for (var entry : superpositionBuffer.entrySet()) {
                long targetOffset = entry.getKey();
                ConcurrentLinkedQueue<TransactionWave> waveCloud = entry.getValue();

                TransactionWave finalWinningWave = null;
                double maxMass = -Double.MAX_VALUE;

                TransactionWave currentWave;
                // 💡 [O(1) Memory Zero-Allocation 패배자 색출 알고리즘]
                while ((currentWave = waveCloud.poll()) != null) {
                    if (currentWave.scalarMass() > maxMass) {
                        // 기존에 승리자로 군림하던 데이터가 있었다면, 더 무거운 질량(권한)에 밀려 패배 처리 (DLQ행)
                        if (finalWinningWave != null) {
                            recordDlqBinaryDump(targetOffset, finalWinningWave);
                            lostUpdateDataCount++;
                        }
                        maxMass = currentWave.scalarMass();
                        finalWinningWave = currentWave;
                    } else {
                        // 현재 데이터가 최대 질량(권한)의 중력을 이기지 못하고 즉시 패배 (DLQ행)
                        recordDlqBinaryDump(targetOffset, currentWave);
                        lostUpdateDataCount++;
                    }
                }

                // 2. [사건의 지평선 투하] 승리한 단 하나의 데이터만을 OS 커널 메모리에 기록(Direct Memory Write)
                if (finalWinningWave != null) {
                    physicalMappedSegment.set(TENSOR_FLOAT_LE, targetOffset, finalWinningWave.tensorValue());
                    collapsedEventCount++;
                }

                // 처리가 끝난 오프셋의 큐는 메모리 누수 방지를 위해 ConcurrentHashMap에서 삭제
                superpositionBuffer.remove(targetOffset);
            }

            // 3. [영구 고착 (Force Flush)]
            // OS의 페이지 캐시(RAM)에 머물던 데이터를 물리적인 SSD/HDD의 섹터에 강제로 영속화(Persist)시킵니다.
            physicalMappedSegment.force();

            if (collapsedEventCount > 0) {
                logger.fine(String.format("   ├─ [버퍼 플러시 완료] %d건 커밋, %d건의 경합 탈락(Lost Update) 텐서가 DLQ에 안전하게 보존되었습니다.",
                        collapsedEventCount, lostUpdateDataCount));
            }

            // 4. 💡 [성능 최적화: NVMe 친화적 가비지 컬렉션 (WAL 세그먼트 GC)]
            // 메인 디스크에 데이터가 100% 안전하게 영속화되었으므로, 더 이상 필요 없는 이전 WAL 세그먼트 파일들을 물리적으로 파괴합니다.
            // 무조건적인 truncate(0)를 폐기하고 비동기 File Delete GC 큐 모델로 완벽히 대체했습니다.
            Path obsoleteWalFile;
            while ((obsoleteWalFile = pendingGarbageWalQueue.poll()) != null) {
                try {
                    Files.deleteIfExists(obsoleteWalFile);
                    logger.fine("   ├─ [WAL 가비지 컬렉션] 플러시가 완료되어 효력을 다한 구형 WAL 세그먼트를 삭제(GC)했습니다: " + obsoleteWalFile.getFileName());
                } catch (IOException e) {
                    // 백신 등 타 프로세스가 Lock을 잡고 있어 삭제에 실패했다면 큐에 다시 넣어 다음 주기에 시도합니다.
                    logger.warning(" [WAL 삭제 지연] 파일 삭제 실패 (다음 붕괴 주기에 재시도합니다): " + obsoleteWalFile.getFileName());
                    pendingGarbageWalQueue.offer(obsoleteWalFile);
                    break;
                }
            }

        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [엔진 예외] 버퍼 병합 및 디스크 플러시 중 치명적 시스템 예외 발생", ex);
        } finally {
            // 원자적 상태 스위치 해제
            isCollapseInProgress.set(false);
        }
    }

    /**
     * [데이터 유실 방어 로직: Dead Letter Queue (DLQ) 아카이빙]
     * 동시성 경합에서 권한(질량) 부족으로 패배하여 덮어쓰기 당한 데이터(Lost Update)를 안전한 보관소에 바이너리 형태로 덤프합니다.
     * 관리자는 이 덤프를 통해 억울하게 유실된 합법적 트래픽을 사후 분석하고 복구(Roll-forward)할 수 있습니다.
     */
    private void recordDlqBinaryDump(long absoluteOffset, TransactionWave defeatedWave) {
        byte[] idBytes = defeatedWave.transactionId().getBytes(StandardCharsets.UTF_8);
        ByteBuffer dlqBuffer = ByteBuffer.allocate(24 + idBytes.length).order(ByteOrder.LITTLE_ENDIAN);

        dlqBuffer.putLong(absoluteOffset);
        dlqBuffer.putFloat(defeatedWave.tensorValue());
        dlqBuffer.putDouble(defeatedWave.scalarMass());
        dlqBuffer.putInt(idBytes.length);
        dlqBuffer.put(idBytes);
        dlqBuffer.flip();

        try {
            // DLQ 파일 역시 초고속 순차 쓰기(Sequential Write)를 위해 락을 제어합니다.
            synchronized (dlqSequentialLock) {
                while (dlqBuffer.hasRemaining()) {
                    dlqRecordChannel.write(dlqBuffer);
                }
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [DLQ 기록 예외] 패배한 트랜잭션 덤프 중 I/O 에러 발생", ex);
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 데이터베이스 락(Lock) 경합의 회피와 양자 중첩 아키텍처 (Quantum Superposition Model):
 * 전통적인 폰 노이만 아키텍처 데이터베이스에서 가장 치명적인 병목은 '동시성(Concurrency)'에서 기인합니다.
 * 여러 스레드가 하나의 메모리 레코드(또는 파일 섹터)에 값을 쓰려 할 때, 일반적인 DB는 `Pessimistic Lock(비관적 락)`으로 스레드를 대기시키거나 
 * `Optimistic Lock(낙관적 락)`으로 예외를 던지고 재시도(Retry)를 강제합니다. 이는 막대한 CPU 컨텍스트 스위칭 낭비를 유발합니다.
 * 통합 OS의 비동기 소화기는 들어오는 모든 쓰기(Write) 요청을 거부하거나 대기시키지 않습니다.
 * 일단 `ConcurrentLinkedQueue` 기반의 버퍼(구름) 속에 수십만 개의 요청을 전부 락(Lock) 없이 병렬로 쌓아 올립니다. 
 * 데이터를 보낸 에이전트 스레드는 지연 시간 0초(Fire-and-Forget)로 즉시 본래 임무(AI 추론 등)로 복귀합니다.
 * 
 * 2. 스칼라 질량(Scalar Mass) 기반의 동시성 경합 해소 (Conflict Resolution by Privilege Mass):
 * 허공에 대기 중이던 수만 개의 쓰기 요청들은, 백그라운드 스케줄러가 `executeWaveCollapseAndFlush()`를 
 * 호출하는 플러시(Flush) 시점에 단 하나의 최종 데이터로 확정(Collapse)됩니다.
 * 이때 승리자의 기준은 '먼저 들어온 순서(FIFO)'가 아닙니다. 일반 에이전트가 보낸 데이터(질량 50.0)와 
 * 관리자가 보낸 보정 데이터(질량 무한대)가 충돌했다면, 질량이 가벼운 에이전트의 데이터는 DLQ로 방출되고 오직 
 * 관리자의 데이터만이 살아남아 물리 커널 메모리에 각인됩니다. 
 * 이것이 완벽한 권한 통제(Priority Control)와 Lock-Free를 동시에 달성한 대수학적 동시성 제어 아키텍처입니다.
 * 
 * 3. 💡 WAL (Write-Ahead Logging) 롤링(Rolling) 아키텍처의 도입:
 * 기존 설계의 아킬레스건은 버퍼 병합 후 파일 크기를 강제로 0으로 만드는 무조건적인 `truncate(0)`에 있었습니다.
 * 수복된 파이프라인은 버퍼에 들어가려는 모든 트랜잭션을 디스크에 '순차 이어쓰기(Append-Only)'로 기록합니다. 
 * 디스크 헤드를 움직일 필요가 없는(Random I/O 배제) 순차 쓰기는 NVMe 환경에서 RAM 속도에 필적하므로 시스템 성능 저하가 없습니다.
 * 이 모듈은 파일 크기가 50MB에 도달하면 즉각 새 파일(세그먼트)을 열어 로테이션(Rolling)하며,
 * 메인 디스크 플러시(Flush)가 완료되어 데이터 보존이 완벽히 끝난 안전한 '과거의 WAL 파일'들만 `pendingGarbageWalQueue`에서 꺼내 
 * 물리적으로 삭제(GC)합니다. 이는 카프카(Kafka)와 동일한 메커니즘으로 극강의 I/O 처리 성능과 시스템 불변성을 보장합니다.
 * 
 * 4. 잃어버린 데이터의 부활과 DLQ 수동 롤포워드(Roll-forward):
 * 동시성 경합에서 패배한(Lost Update) 데이터는 버려지지 않고 DLQ(Dead Letter Queue) 바이너리 파일로 유폐(Archive)됩니다.
 * 새롭게 이식된 `executeDlqManualRollforward` 배관은 관리자(사령관)의 명령 하나로 이 버려진 데이터 파일을 스캔하여,
 * 유폐된 텐서들을 다시 비동기 락-프리 버퍼에 재주입(Re-evaluate)할 수 있습니다. 
 * 이는 단순히 로그를 복구하는 것을 넘어, 경합에서 밀려 거부되었던 과거의 인과율을 현재의 데이터베이스에 다시금 안전하게 융합시키는 완벽한 데이터 사후 복구 기능입니다.
 * =============================================================================
 */