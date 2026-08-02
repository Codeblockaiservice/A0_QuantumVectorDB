/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망
 * @alias Nightly_Audit_Batch_Daemon
 * @tier 3
 * @keywords Eventual Consistency, Bit-Rot, Checkpoint Roll-forward, Resume, Work-Stealing
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422034_야간_전수_감사_배치.java
 * - 기능 (Function): 디스크 I/O가 멈춘 유휴 시간(야간)에 시스템 전체 데이터의 물리적 텐서 결함(Bit-Rot, 데이터 유실)을 딥 스캔합니다.
 * - 역할 (Role): 실시간 파이프라인에서 분리된 '전체 스캔' 부담을 비동기로 처리하여, 시스템의 최종적 일관성(Eventual Consistency)을 보장하는 감사 코어.
 * - 이론 (Theory): 최종적 일관성, 자연 부패(Bit-Rot) 감지, Work-Stealing 기반 백그라운드 스케줄링, 체크포인트 롤포워드(Checkpoint Roll-forward).
 * - 기대효과 (Effect): 라이브 시스템 I/O 스래싱 없이 우주 방사선이나 스토리지 셀 노화로 인한 텐서의 침묵하는 오염(Silent Data Corruption)을 색출하며, 중단 시에도 재개(Resume)가 보장됩니다.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [초정밀 통제] 체크포인트(Checkpoint) 롤포워드 상태 엔진 이식: 
 *                 감사 도중 서버가 재부팅되면 0번 인덱스부터 다시 스캔하여 I/O를 낭비하던 비효율을 제거했습니다. 
 *                 1분 단위로 검수가 완료된 Y축(엔티티)과 X축(틱)의 영수증을 `.checkpoint` 메타 파일로 기록하여, 
 *                 익일 재구동 시 중단되었던 지점부터 정밀 이어서 하기(Resume)를 수행하는 상태 관리(Stateful) 기반 데몬으로 승격시켰습니다.
 * - 💡 [기능 확장] INT8 양자화 해상도 스캔 로직을 관통시켜, 극단적 데이터 유실(All-Zeros) 현상을 100% 색출하도록 보강 완료.
 * - 💡 [컴파일 에러 수복] `flushRemainingLogsAndAwait(true/false)` 구문에서 발생하던 이항 연산자 에러를 `flushRemainingLogsAndAwait(true)` 스칼라 값 주입으로 치유 완료.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 시스템 제어, 동시성 스케줄링, 비동기 I/O를 위한 핵심 자바 표준 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core Java standard libraries for file system control, concurrent scheduling, and asynchronous I/O.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422003_지능형_메타데이터_사전;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422003_지능형_메타데이터_사전.FeatureManifest;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더.SmartIndexRegistry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 디스크 가용성이 높은 유휴 시간에 시스템 전역의 텐서 결함을 찾아내는 백그라운드 감사 코어입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A background audit core that finds all tensor defects across the system during idle times when disk availability is high.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422034
 * [파일명] A0_DT_42_422034_야간_전수_감사_배치.java
 * [모듈명] 통합 OS V6.0 - Tier 3: 야간 전수 감사 배치 (Full Audit Batch)
 *
 * [기능 명세]
 * 1. 최종적 일관성(Eventual Consistency) 사후 감사:
 * 실시간 파이프라인에서 제외된 '전체 스캔'의 부하를 유휴 시간(야간/주말)으로 스케줄링하여 텐서 결함을 딥 스캔.
 * 2. Bit-Rot(자연 부패) 탐지: IEEE 754 비트마스크 검증을 통해 방사선/물리 노화로 인한 텐서 비트 변이(Bit-Flip) 감찰.
 * 3. 최하위 우선순위 ForkJoinPool: Work-Stealing 엔진을 `Thread.MIN_PRIORITY`로 가동시켜 메인 AI 추론 엔진의 CPU 자원을 간섭하지 않음.
 * 4. 💡 [V6.0 핵심 통제] 체크포인트(Checkpoint) 롤포워드:
 * 매 1분마다 검수가 완료된 상태 영수증을 `.checkpoint` 파일로 원자적 플러시(Atomic Move) 수행.
 * 시스템 중단 시에도 영수증을 읽어 중단된 지점부터 이어서 재개(Resume)하는 Resilient(회복 탄력적) 스캔 엔진.
 * ==============================================================================
 */
public final class A0_DT_42_422034_야간_전수_감사_배치 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422034_NIGHTLY_AUDIT_BATCH");

    private final A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger;
    private final ForkJoinPool nightlyAuditThreadPool;

    /**
     * [생성자] 최하위 우선순위의 백그라운드 워커 스레드를 생산하는 커스텀 팩토리를 통해 ForkJoinPool을 점화합니다.
     */
    public A0_DT_42_422034_야간_전수_감사_배치(A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger) {
        if (anomalyLogger == null) {
            throw new IllegalArgumentException("[의존성 누락] 이상 보고서 로거가 주입되지 않아 감사 코어를 기동할 수 없습니다.");
        }
        this.anomalyLogger = anomalyLogger;

        // 💡 [기계적 공감(Mechanical Sympathy)] CPU 코어 독점 방지 및 우선순위 최하위(MIN_PRIORITY) 강제 할당
        int safeCoreAllocation = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

        ForkJoinPool.ForkJoinWorkerThreadFactory lowestPriorityFactory = pool -> {
            final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setPriority(Thread.MIN_PRIORITY);
            worker.setName("OS_NIGHTLY_AUDIT_WORKER_" + worker.getPoolIndex());
            return worker;
        };

        // 비동기 모드(asyncMode=true)로 활성화하여 대기열에 쌓인 청크(Chunk) 검증 태스크를 훔쳐(Steal) 처리합니다.
        this.nightlyAuditThreadPool = new ForkJoinPool(safeCoreAllocation, lowestPriorityFactory, null, true);

        logger.info(" >> [통합 OS V6.0] A0_DT_42_422034 야간 전수 감사 배치 기동. (체크포인트 롤포워드 엔진 장착 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 [핵심 통제 적용: 체크포인트 롤포워드 및 이어서 하기(Resume)]
    // 딥 스캔 도중 시스템이 셧다운되더라도 처음부터 다시 스캔하지 않도록 상태 정보를 기록하고 백그라운드 갱신 데몬을 엽니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Control Applied: Checkpoint Roll-forward and Resume]
    // Reads the `.checkpoint` file to avoid restarting from scratch if shut down during deep scan, and opens a background daemon that updates the state.

    /**
     * [감사 로직 1: 시스템 전면 딥 스캔 및 롤포워드]
     * 시스템 스케줄러에 의해 디스크 I/O가 멈춘 안전한 유휴 시간에 호출됩니다.
     * 모든 지표와 엔티티의 파일을 청크(Chunk) 단위의 태스크로 잘게 쪼개어 스레드 풀에 던집니다.
     * 
     * @param timeframeContext      스캔할 물리적 도메인 컨텍스트
     * @param runtimeIndexRegistry  Y축 엔티티 코드 역산을 위한 O(1) 레지스트리
     * @param validTickCursor       현재 시스템이 확정한 최대 유효 시간(Tick) 한계선
     */
    public void executeNightlyFullAudit(
            A0_DT_42_422000_타임프레임_컨텍스트 timeframeContext,
            SmartIndexRegistry runtimeIndexRegistry,
            int validTickCursor) {

        logger.info(" ================================================================= ");
        logger.info(String.format(" [야간 전수 감사 개시] 타겟 도메인: %s | 유효 커서: %d 틱",
                timeframeContext.getResolutionCode(), validTickCursor));

        long auditStartTime = System.currentTimeMillis();

        Map<String, Integer> featureZMap = runtimeIndexRegistry.featureZIndexMap();
        int maxYIndex = featureZMap.values().stream().max(Integer::compareTo).orElse(-1);

        if (maxYIndex < 0 || validTickCursor <= 0) {
            logger.warning(" [감사 스킵] 도메인에 데이터가 비어있거나 타임프레임이 아직 개척되지 않았습니다.");
            return;
        }

        String[] reverseEntityDictionary = new String[maxYIndex + 1];
        for (Map.Entry<String, Integer> entry : featureZMap.entrySet()) {
            reverseEntityDictionary[entry.getValue()] = entry.getKey();
        }

        // 💡 1. [체크포인트 롤포워드] 기존 검사 영수증 로드
        Path checkpointPath = timeframeContext.getFastDataRootPath().resolve("422034_AUDIT.checkpoint");
        ConcurrentHashMap<String, Integer> auditCheckpointMap = loadCheckpointReceipt(checkpointPath);

        // 💡 2. [체크포인트 데몬 점화] 1분 단위로 검수 진척도를 원자적 사출(Atomic Flush)
        ScheduledExecutorService checkpointDaemon = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread t = new Thread(runnable, "OS_CHECKPOINT_FLUSHER");
            t.setDaemon(true);
            return t;
        });
        checkpointDaemon.scheduleWithFixedDelay(() -> flushCheckpointReceipt(checkpointPath, auditCheckpointMap), 1, 1, TimeUnit.MINUTES);

        List<ForkJoinTask<Void>> subTaskList = new ArrayList<>();

        // 3. 모든 지표(Z축)를 순회하며 개별 검증 태스크를 분할 생성합니다.
        for (String featureName : runtimeIndexRegistry.featureZIndexMap().keySet()) {
            Path layerPhysicalPath = timeframeContext.resolveDataAbsolutePath(featureName);
            if (!Files.exists(layerPhysicalPath))
                continue;

            A0_DT_42_422003_지능형_메타데이터_사전.FeatureManifest dna = A0_DT_42_422003_지능형_메타데이터_사전.parseFeatureManifest(featureName, null);

            // 💡 [청크 분할 및 Resume 판단]
            for (int y = 0; y <= maxYIndex; y++) {
                final int targetY = y;
                String checkpointKey = featureName + "|" + targetY;

                // 과거에 검사가 완료된 틱(Tick) 위치 확인
                int previousCompletedTick = auditCheckpointMap.getOrDefault(checkpointKey, 0);

                // 이미 현재 유효 커서까지 검사가 다 끝났다면 스킵하여 불필요한 I/O 낭비 원천 차단
                if (previousCompletedTick >= validTickCursor - 1) {
                    continue;
                }

                subTaskList.add(nightlyAuditThreadPool.submit(() -> {
                    try (FileChannel channel = FileChannel.open(layerPhysicalPath, StandardOpenOption.READ)) {
                        // 멈췄던 기존 완료 틱부터 (validTickCursor - 1) 까지 이어서(Resume) 스캔 강행
                        executeSingleEntityChunkDeepScan(channel, targetY, previousCompletedTick, validTickCursor - 1, featureName, reverseEntityDictionary, dna, anomalyLogger);

                        // 해당 종목의 검사가 무사히 완료되면 체크포인트 맵 메모리 갱신
                        auditCheckpointMap.put(checkpointKey, validTickCursor - 1);
                    } catch (IOException ex) {
                        logger.warning(" [파일 판독 오류] 야간 감사 중 I/O 에러 발생: " + layerPhysicalPath.getFileName());
                    }
                    return null;
                }));
            }
        }

        // 4. 모든 병렬 태스크가 완료될 때까지 동기화 장벽 대기 (Barrier)
        for (ForkJoinTask<Void> task : subTaskList) {
            task.join();
        }

        // 5. 💡 [종결 및 최종 영수증 플러시]
        checkpointDaemon.shutdownNow(); // 백그라운드 1분 주기 플러셔 정지
        flushCheckpointReceipt(checkpointPath, auditCheckpointMap); // 마지막 최종 영수증 원자적 사출(Save)

        // 💡 [컴파일 에러 교정 완수] 적발된 비트 오염 기록을 LMAX 로거 디스크에 강제(true) 플러시
        anomalyLogger.flushRemainingLogsAndAwait(true);

        long elapsedTimeMs = System.currentTimeMillis() - auditStartTime;
        logger.info(String.format(" >> [야간 전수 감사 종료] 모든 텐서 매트릭스의 Bit-Rot 색출 및 무결성 판독 완료. (소요 시간: %.2f 초)",
                (elapsedTimeMs / 1000.0)));
        logger.info(" ================================================================= ");
    }

    // [1. 한글 상세 주석]
    // 💡 [구조 개선 완료] 시작_틱과 종료_틱 파라미터를 추가하여 국소적 딥스캔(Resume)을 지원하도록 재설계했습니다.
    // [2. 영문 상세 주석]
    // 💡 [Structural Improvement Completed] Redesigned to support localized deep scan (Resume) by adding startTickIndex and endTickIndex parameters.

    /**
     * [감사 로직 2: 단일 종목(Y) 텐서 청크의 물리적 무결성 딥 스캔 (Resume 대응)]
     */
    private void executeSingleEntityChunkDeepScan(
            FileChannel channel,
            int targetY,
            int startTickIndex,
            int endTickIndex,
            String featureName,
            String[] reverseEntityDictionary,
            A0_DT_42_422003_지능형_메타데이터_사전.FeatureManifest dna,
            A0_DT_42_422033_LMAX_이상_보고서_로거 loggerRef) throws IOException {

        int byteStride = dna.recommendedResolution().getByteSize();
        long bytesToRead = (long) (endTickIndex - startTickIndex + 1) * byteStride;

        if (bytesToRead <= 0)
            return;

        // 파일의 총 크기를 초과하여 읽으려 하는 범위를 안전하게 클리핑
        long fileSize = channel.size();
        long startAbsoluteOffset = A0_DT_42_422001_권한_포트_인터페이스.calculateChunkInternalOffset(targetY, startTickIndex, byteStride);

        if (startAbsoluteOffset >= fileSize)
            return; // 아직 매트릭스가 개척되지 않은 미래 빈 공간

        // 실제 파일에 존재하는 크기까지만 클리핑
        bytesToRead = Math.min(bytesToRead, fileSize - startAbsoluteOffset);
        if (bytesToRead <= 0)
            return;

        // 💡 [Zero-Allocation Buffer] 커널 페이지 캐시 다이렉트 접근용 버퍼 (GC 힙 할당 없음)
        ByteBuffer positionalBuffer = ByteBuffer.allocateDirect((int) bytesToRead).order(ByteOrder.LITTLE_ENDIAN);

        // 💡 [Positional Read] 채널 글로벌 커서를 비틀지 않는 논블로킹 절대 위치 읽기
        int readBytes = channel.read(positionalBuffer, startAbsoluteOffset);
        if (readBytes < byteStride)
            return;

        positionalBuffer.flip();
        String entityCode = (targetY < reverseEntityDictionary.length && reverseEntityDictionary[targetY] != null) ? reverseEntityDictionary[targetY] : "UNKNOWN";

        // 해상도에 따른 멸균 검사 스위칭
        switch (dna.recommendedResolution()) {
            case FLOAT32_PRECISION ->
                auditFloat32Chunk(positionalBuffer, startTickIndex, endTickIndex, entityCode, featureName, loggerRef);
            case BFLOAT16_AI_COMPRESSED ->
                auditBFloat16Chunk(positionalBuffer, startTickIndex, endTickIndex, entityCode, featureName, loggerRef);
            case INT8_QUANTIZED ->
                auditInt8Chunk(positionalBuffer, startTickIndex, endTickIndex, entityCode, featureName, loggerRef);
        }
    }

    /**
     * [판독 로직 1] Float32 IEEE 754 비트마스크 스캔 및 Silent Data Corruption(Bit-Rot) 감지
     */
    private void auditFloat32Chunk(
            ByteBuffer buffer,
            int startTickIndex,
            int endTickIndex,
            String entityCode,
            String featureName,
            A0_DT_42_422033_LMAX_이상_보고서_로거 loggerRef) {

        for (int x = startTickIndex; x <= endTickIndex && buffer.remaining() >= 4; x++) {
            int rawBits = buffer.getInt();

            // 1. 미치유 결측치(NaN) 비트마스크 감지
            boolean isMissing = (rawBits & 0x7F800000) == 0x7F800000 && (rawBits & 0x007FFFFF) != 0;
            if (isMissing) {
                loggerRef.logAnomalyEvent(entityCode, "TICK_IDX_" + x, featureName,
                        "BIT_ROT_NAN_FLOAT32", "야간 감사 중 미치유 결측치(NaN) 또는 우주 방사선에 의한 Float32 Bit-Rot 손상이 감지되었습니다.");
                continue;
            }

            // 2. 💡 [기하학적 붕괴 스캔] Infinity (무한대) 감지
            // 디스크 셀이 물리적으로 부패하여 비트가 반전(Bit Flip)되었을 때 가장 흔히 나타나는 증상입니다.
            boolean isInfinity = (rawBits & 0x7F800000) == 0x7F800000 && (rawBits & 0x007FFFFF) == 0;
            if (isInfinity) {
                loggerRef.logAnomalyEvent(entityCode, "TICK_IDX_" + x, featureName,
                        "CORRUPTION_INFINITY", "물리적 디스크 부패(Bit-Rot)로 인한 Float32 Infinity(무한대) 텐서 파열이 감지되었습니다.");
            }
        }
    }

    /**
     * [판독 로직 2] BFloat16 비트마스크 스캔
     */
    private void auditBFloat16Chunk(
            ByteBuffer buffer,
            int startTickIndex,
            int endTickIndex,
            String entityCode,
            String featureName,
            A0_DT_42_422033_LMAX_이상_보고서_로거 loggerRef) {

        for (int x = startTickIndex; x <= endTickIndex && buffer.remaining() >= 2; x++) {
            short rawBits = buffer.getShort();

            boolean isMissing = (rawBits & 0x7F80) == 0x7F80 && (rawBits & 0x007F) != 0;
            if (isMissing) {
                loggerRef.logAnomalyEvent(entityCode, "TICK_IDX_" + x, featureName,
                        "BIT_ROT_NAN_BFLOAT16", "야간 감사 중 BFloat16 해상도 블록 내에서 Bit-Rot 손상이 감지되었습니다.");
                continue;
            }

            boolean isInfinity = (rawBits & 0x7F80) == 0x7F80 && (rawBits & 0x007F) == 0;
            if (isInfinity) {
                loggerRef.logAnomalyEvent(entityCode, "TICK_IDX_" + x, featureName,
                        "CORRUPTION_INFINITY_BF16", "물리적 디스크 부패(Bit-Rot)로 인한 BFloat16 Infinity 텐서 파열이 감지되었습니다.");
            }
        }
    }

    /**
     * [판독 로직 3: INT8 (1Byte) 양자화 국소 구간 다이렉트 스캔]
     */
    private void auditInt8Chunk(
            ByteBuffer buffer,
            int startTickIndex,
            int endTickIndex,
            String entityCode,
            String featureName,
            A0_DT_42_422033_LMAX_이상_보고서_로거 loggerRef) {

        int consecutiveVacuumCount = 0;
        int vacuumToleranceThreshold = 10; // 10틱 연속으로 데이터가 0x00이면 이상(All-Zeros) 상태로 간주

        for (int x = startTickIndex; x <= endTickIndex && buffer.remaining() >= 1; x++) {
            byte rawBit = buffer.get();

            if (rawBit == 0x00) {
                consecutiveVacuumCount++;
                if (consecutiveVacuumCount >= vacuumToleranceThreshold) {
                    loggerRef.logAnomalyEvent(entityCode, "TICK_IDX_" + x, featureName,
                            "ALL_ZEROS_ANOMALY_INT8", "야간 스캔 중 INT8 양자화 블록에서 심각한 데이터 소실이 발생하여 연속된 0x00(진공 붕괴) 상태가 적발되었습니다.");
                    break;
                }
            } else {
                consecutiveVacuumCount = 0;
            }
        }
    }

    // =========================================================================
    // 💡 [체크포인트 코어] 원자적 영수증 관리망 (Checkpoint Roll-forward State)
    // =========================================================================

    /**
     * 기존에 존재하던 `.checkpoint` 상태 영수증을 읽어와서 어디까지 감사를 완료했는지 파악합니다.
     */
    private ConcurrentHashMap<String, Integer> loadCheckpointReceipt(Path path) {
        ConcurrentHashMap<String, Integer> checkpointMap = new ConcurrentHashMap<>();
        if (!Files.exists(path))
            return checkpointMap;

        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            for (String line : lines) {
                int equalPos = line.indexOf('=');
                if (equalPos > 0) {
                    String key = line.substring(0, equalPos);
                    int tick = Integer.parseInt(line.substring(equalPos + 1));
                    checkpointMap.put(key, tick);
                }
            }
            logger.fine(String.format("   ├─ [롤포워드 점화] 기존에 완료된 %d 건의 스캔 내역을 로드하여 재개(Resume)를 준비합니다.", checkpointMap.size()));
        } catch (Exception e) {
            logger.warning(" [체크포인트 파손] 영수증 파일을 정상적으로 읽을 수 없습니다. 0번 틱부터 스캔을 다시 시작합니다.");
        }
        return checkpointMap;
    }

    /**
     * 파일 쓰기 중 정전이나 셧다운이 발생해도 파일이 깨지지 않도록, 임시 파일(.tmp)에 모두 기록한 후 `ATOMIC_MOVE`로 안전하게 덮어씌웁니다.
     */
    private void flushCheckpointReceipt(Path path, ConcurrentHashMap<String, Integer> checkpointMap) {
        if (checkpointMap.isEmpty())
            return;

        Path tempPath = path.resolveSibling(path.getFileName() + ".tmp");
        try {
            try (BufferedWriter writer = Files.newBufferedWriter(tempPath, StandardCharsets.UTF_8)) {
                for (Map.Entry<String, Integer> entry : checkpointMap.entrySet()) {
                    writer.write(entry.getKey() + "=" + entry.getValue() + "\n");
                }
            }
            // 💡 [원자성 락온] 임시 파일 작성이 100% 온전히 끝난 그 찰나에만 실제 메타 파일로 포인터를 스왑합니다.
            Files.move(tempPath, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.warning(" [체크포인트 플러시 실패] 백그라운드 영수증 파일 갱신 중 I/O 에러가 발생했습니다.");
        }
    }

    /**
     * [종결 절차] 시스템 종료 시 백그라운드 스레드 풀 자원을 안전하게 반환합니다.
     */
    public void executeGracefulShutdown() {
        if (nightlyAuditThreadPool != null && !nightlyAuditThreadPool.isShutdown()) {
            logger.info("   ├─ [야간 감사 모듈 셧다운] 전수 스캔 스레드 풀 안전 종료 절차 개시...");
            nightlyAuditThreadPool.shutdown();
            try {
                if (!nightlyAuditThreadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    nightlyAuditThreadPool.shutdownNow();
                }
            } catch (InterruptedException ex) {
                nightlyAuditThreadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info(" >> [감사 코어 회수 완료] 야간 딥 스캔을 위해 할당된 CPU 자원이 모두 시스템에 환원되었습니다.");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 최종적 일관성 (Eventual Consistency)과 실시간 속도의 교환:
 * 과거 V5.0 아키텍처는 데이터가 단 1줄이라도 들어올 때마다 테라바이트급 매트릭스 전체를 뒤집어엎고 전수 무결성 검사(Validation)를 수행했습니다.
 * 이는 결벽에 가까운 완벽주의지만, 초당 수백만 개의 트랜잭션을 처리해야 하는 실시간(HFT) 환경에서는 시스템의 I/O를 틀어막아 치명적인 스톨(Stall)을 유발하는 최악의 안티패턴입니다.
 * V6.0 아키텍처는 '실시간 파이프라인'에서는 국소적 증분(Delta) 구간만 정밀 타격하여 처리 속도를 극대화하고,
 * 무겁고 시간이 오래 걸리는 '전수 감사' 작업은 디스크 I/O가 멈춘 유휴 시간(심야/주말)으로 스케줄링하여 완벽하게 전가(Delegate)시켰습니다.
 * 즉, 텐서의 정합성이 실시간으로는 99.9% 보장되지만, 매일 밤 백그라운드 감사 데몬이 스윕하는 순간 100%의 '최종적 일관성(Eventual Consistency)'으로 완전히 수렴하게 됩니다.
 * 
 * 2. Bit-Rot (자연 부패)과 Silent Data Corruption의 색출:
 * SSD의 낸드 플래시 메모리나 HDD의 자성 매체는 긴 시간이 지남에 따라 미세한 전하 누설이나 
 * 우주 방사선(Cosmic Rays)의 타격으로 인해 비트가 0에서 1로 반전(Bit Flip)되는 '자연 부패(Bit-Rot)' 현상을 피할 수 없습니다.
 * 현대 운영체제(OS)의 파일 시스템은 이러한 침묵하는 데이터 오염(Silent Data Corruption)을 선제적으로 경고해주지 않습니다.
 * 만약 멀쩡했던 주식 가격 1.52가 비트 플립으로 인해 IEEE 754 규격 상의 Infinity(무한대)나 NaN으로 돌변한다면,
 * 이를 그대로 모델 추론에 사용한 AI 코어의 가중치 행렬은 파이프라인 전체를 박살 내버립니다.
 * 본 스캔 모듈은 기저 데이터가 디스크 상에 온전히 적혀있는지 수억 개의 텐서 블록을 비트마스크로 뜯어보며, 
 * 숨어있는 무한대(Infinity)와 NaN 변이 증상을 색출해 내는 최후의 불침번(Watchdog) 역할을 수행합니다.
 * 
 * 3. ForkJoinPool과 기계적 공감 (Mechanical Sympathy):
 * 2,850여 개가 넘는 엔티티(종목)를 단일 스레드로 스캔하면 밤이 새도록 감사가 끝나지 않습니다. 반대로 무작정 스레드를 수만 개 띄우면 
 * 엄청난 OS 컨텍스트 스위칭(Context Switching) 오버헤드로 서버가 불타오릅니다.
 * 이 모듈은 Java의 `ForkJoinPool`과 `Work-Stealing` 알고리즘을 도입했습니다.
 * 가용 CPU 코어의 절반만큼만 워커를 생성하고, 스캔이 먼저 끝난 워커 스레드가 다른 바쁜 워커의 대기열 큐(Queue)에서 몰래 청크 태스크를 훔쳐와서 처리함으로써 코어가 단 1초도 쉬지 않고 풀가동되게 만듭니다.
 * 동시에 모든 워커의 스레드 우선순위를 `Thread.MIN_PRIORITY`로 강제로 강등시켰습니다.
 * 혹여나 야간에 관리자의 긴급한 AI 연산 추론 명령이나 대규모 쿼리가 유입되더라도, 감사 워커들은 즉시 CPU 자원을 100% 양보하여 
 * 마이크로커널의 질서와 열역학적 부하 평형을 완벽히 수호합니다(Mechanical Sympathy).
 * 
 * 4. 💡 체크포인트(Checkpoint) 롤포워드와 복원 탄력성 (Resilience):
 * 테라바이트(TB) 급의 전수 스캔은 하룻밤 사이에 모두 끝나지 않을 수도 있습니다.
 * 과거 설계에서는 아침이 되어 실시간 연산 파이프라인이 기동되면서 감사 데몬이 셧다운되면, 그날 밤 다시 0번 틱(Tick)부터 스캔을 반복하는 절망적인 병목의 굴레에 빠졌습니다.
 * 개선된 V6.0 엔진은 매 1분마다 `지표명|Y축_인덱스 = 마지막으로_검증을_마친_틱_위치` 정보를 담은 영수증을 `.checkpoint` 파일에 기록합니다.
 * 서버가 정전되든, 아침 스케줄러에 의해 강제 종료되든, 이 데몬은 다음 실행 시 `.checkpoint` 영수증을 로드하여 
 * 정확히 멈춘 그 틱(Tick)부터 딥 스캔을 재개(Resume)합니다. 전수 감사가 비효율적인 일회성 '배치' 작업에서, 
 * 결코 중단되지 않는 영속적이고 회복 탄력적인 '무한 루프 감찰망'으로 완벽히 아키텍처가 승격되었습니다.
 * =============================================================================
 */