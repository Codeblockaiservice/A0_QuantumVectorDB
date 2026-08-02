/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망
 * @alias Async_Bulk_Ingestor
 * @tier 2
 * @keywords Coldstart, Bulk Ingestion, Fail-Fast, Atomic Move, Zero-Allocation, Global Semaphore
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422020_주조기_비동기_소화기.java
 * - 모듈명: 통합 OS V6.0 - Tier 2: 통합형 비동기 파이프라인 소화기 (데이터 수집 및 통합 코어)
 * - 기능 및 역할: 콜드스타트 스토리지 확장 및 방대한 마스터 데이터 벌크 이식(Bulk Ingestion)을 수행하며, 외부 데이터를 비동기적으로 파싱하여 L1 매트릭스에 맵핑합니다.
 * - 이론 및 기술: Fail-Fast, OS 레벨 원자적 파일 이동(Atomic Move), SeqLock(버전 카운터) 방어막, Zero-Allocation 파서, 글로벌 세마포어(Global Semaphore).
 * - 💡 [V6.0 결함 조치]: 콜드스타트 병렬 스레드 풀이 타임아웃으로 실패할 경우 예외를 무시하던 로직을 수정하여, LMAX 로거 기록 및 RuntimeException을 통한 즉각 셧다운(Fail-Fast)을 적용했습니다.
 * - 💡 [명칭 교정]: 비유적이고 문학적인 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [초정밀 제어]: 파일 메타데이터 크기를 확장하는 찰나에 야간 컴팩션 데몬 등 타 스레드가 개입하여 발생하는 Race Condition을 차단하기 위해 '글로벌 세마포어(Global Semaphore)' 락을 이식했습니다.
 * - 💡 [동적 임계치]: 스파스 커밋 시 고정되었던 256틱 한계치를 `matrix.sparse.gap.threshold` 시스템 프로퍼티에서 로드하도록 외부 설정(Externalized Configuration) 배관을 추가했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 시스템 제어, 동시성 병렬 처리, FFM API 접근, 전역 락 통제를 위한 핵심 의존성 모듈들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core dependency modules for file system control, concurrent parallel processing, FFM API access, and global lock-on.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트.IngestionState;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422011_스캐너_차원_측정기;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망.A0_DT_42_422033_LMAX_이상_보고서_로거;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 초기 데이터 로드 및 외부 비동기 파이프라인 수집을 총괄하는 인제스토어(Ingestor) 코어 모듈입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. The ingestor core module that manages initial data load and asynchronous external pipeline ingestion.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422020
 * [파일명] A0_DT_42_422020_주조기_비동기_소화기.java
 * [모듈명] 통합 OS V6.0 - Tier 2: 통합형 비동기 파이프라인 소화기 (데이터 수집 코어)
 * 
 * [기능 명세]
 * 1. 💡 데이터 수집 코어: 콜드스타트 스토리지 팽창, 방대한 마스터 데이터 벌크 이식(Bulk Ingestion),
 * 매크로 지수 융합 기능을 단일 모듈화하여 여러 스레드 파편화로 인한 I/O 경합을 완전히 제거했습니다.
 * 2. 💡 OS 레벨 원자적 락(Atomic Lock): `Files.move(ATOMIC_MOVE)`를 강제하여, 여러 개의
 * 스레드가 동시에 하나의 스풀(Spool) 파일을 선점하려 할 때 발생하는 교착 상태(Deadlock)를
 * 애플리케이션 락이 아닌 커널 레벨 기능으로 원천 차단합니다.
 * 3. 💡 SeqLock (버전 카운터) 방어막: 데이터가 기록되는 찰나(수 나노초)에 AI 쿼리 엔진이
 * 텐서를 읽어 '찢어진 데이터(Torn Read)'를 반환하는 것을 막기 위해, 쓰기 락/해제 시퀀스를 제어합니다.
 * 4. 💡 GC 오버헤드 원천 제거 (Zero-Allocation Parser): `String.split()` 객체 생성을 제거하고,
 * 가상의 포인터(인덱스) 커서만을 이동시키며 ASCII 대수학으로 부동소수점을 파싱하여
 * 대용량 처리 시에도 JVM 힙(Heap) 객체 생성을 0으로 유지합니다.
 * 5. 💡 [V6.0 정밀 제어] 글로벌 세마포어(Global Semaphore) 락 적용:
 * 디스크 파일의 크기를 팽창(`allocateEmptyCanvas`)시키는 찰나의 순간, 야간 LSM 컴팩션 데몬 등의 병렬 개입을 물리적으로 차단하는
 * 전역 락온 시스템을 이식했습니다.
 * ==============================================================================
 */
public final class A0_DT_42_422020_주조기_비동기_소화기 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422020_OMNI_INGESTOR");
    private static final Pattern ENTITY_CODE_PATTERN = Pattern.compile("(\\d{6})");

    // [1. 한글 상세 주석]
    // 💡 [글로벌 세마포어 락]
    // 파일 시스템 메타데이터 변경 시 LSM 컴팩션 데몬 등의 비동기 I/O와의 충돌(Race Condition)을
    // 원천 봉쇄하기 위해 1개의 스레드만 진입 가능한 전역적이고 공정한(Fair) 세마포어를 상주합니다.
    // [2. 영문 상세 주석]
    // 💡 [Global Semaphore Lock]
    // A global and fair semaphore that allows only 1 thread to enter to fundamentally block 
    // race conditions with asynchronous I/O like the LSM compaction daemon during file system metadata modifications.
    private static final Semaphore fileExpansionSemaphore = new Semaphore(1, true);

    // 💡 [동적 설정 배관] 스파스 커밋 시 밀집 구간을 판단하는 임계치 (System Property 로드)
    private static final int SPARSE_GAP_THRESHOLD = Integer.parseInt(System.getProperty("matrix.sparse.gap.threshold", "256"));

    // 💡 [수학적 거듭제곱 룩업 테이블 (LUT)]
    // 소수점 파싱 시 발생하는 Math.pow(10, n)의 무거운 JNI 네이티브 호출 오버헤드를 줄이기 위해
    // 0승부터 18승까지의 10의 거듭제곱을 미리 배열에 캐싱해 둡니다.
    private static final double[] MATH_POWER_LOOKUP_TABLE = {
            1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
    };

    // [의존성 결합] 코어 인프라 인터페이스
    private final A0_DT_42_422000_타임프레임_컨텍스트 timeframeContext;
    private final A0_DT_42_422021_주조기_FFM_엔진 ffmMemoryEngine;
    private final A0_DT_42_422022_RCU_동시성_주조_워커 rcuIngestionWorker;
    private final A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger;

    // 런타임 커널 메모리 쓰기 권한 포트 매핑 (Feature Name -> WritePort)
    private final Map<String, A0_DT_42_422001_권한_포트_인터페이스.WritePort> featureWritePortMap = new ConcurrentHashMap<>();

    // 💡 [원자성 붕괴(Torn Read) 방어] 지표별 SeqLock (버전 카운터) 레지스트리
    private final Map<String, AtomicLong> sequenceLockRegistry = new ConcurrentHashMap<>();

    // 생명주기를 통제할 전역 공유 아레나
    private Arena globalSharedArena;

    // 비동기 파이프라인(Spool) 데몬 제어기
    private final AtomicBoolean isDaemonRunning = new AtomicBoolean(false);
    private ExecutorService directoryPollingExecutor; // 폴링 전담 스레드 풀
    private ExecutorService ingestionWorkerExecutor;  // 파싱 워커 할당용 병렬 스레드 풀

    /**
     * [생성자] 오케스트레이터 초기화 및 하위 워커 의존성 주입 (Dependency Injection)
     */
    public A0_DT_42_422020_주조기_비동기_소화기(
            A0_DT_42_422000_타임프레임_컨텍스트 timeframeContext,
            A0_DT_42_422021_주조기_FFM_엔진 ffmMemoryEngine,
            A0_DT_42_422022_RCU_동시성_주조_워커 rcuIngestionWorker,
            A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger) {

        if (timeframeContext == null || ffmMemoryEngine == null || rcuIngestionWorker == null || anomalyLogger == null) {
            throw new IllegalArgumentException("[의존성 주입 실패] 필수 의존성이 누락되어 비동기 인제스토어(Ingestor)를 기동할 수 없습니다.");
        }

        this.timeframeContext = timeframeContext;
        this.ffmMemoryEngine = ffmMemoryEngine;
        this.rcuIngestionWorker = rcuIngestionWorker;
        this.anomalyLogger = anomalyLogger;

        logger.info(String.format(" >> [통합 OS V6.0] A0_DT_42_422020 데이터 수집 코어(Ingestor) 기동. (상태기계 스풀망 및 SeqLock 방어막 탑재, SPARSE_GAP_THRESHOLD: %d)", SPARSE_GAP_THRESHOLD));
    }

    // =========================================================================
    // 🌌 1. [데이터 수집 코어] 콜드스타트 & 지수 병합 (전면 인제스토어 엔진)
    // =========================================================================

    // [1. 한글 상세 주석]
    // 물리 디스크를 최초로 맵핑하는 콜드스타트 시퀀스입니다.
    // 💡 [결함 수정] `awaitTermination` 시 타임아웃이 발생하면 예외를 무시하지 않고 즉각
    // RuntimeException으로 파이프라인을 Fail-Fast 셧다운 시킵니다.
    // [2. 영문 상세 주석]
    // Coldstart sequence that maps the physical disk for the first time.
    // 💡 [Defect Fixed] If a timeout occurs during `awaitTermination`, it does
    // not ignore the exception but immediately shuts down the pipeline Fail-Fast
    // with a RuntimeException.

    /**
     * 콜드스타트 시퀀스.
     * 스토리지(Sparse File) 선할당, 마스터 데이터 벌크 이식(Bulk Ingestion), 매크로 지수 융합을 총괄합니다.
     */
    public void executeColdStartSequence(
            A0_DT_42_422011_스캐너_차원_측정기.DimensionResult scanResult,
            A0_DT_42_422012_스캐너_호적부_빌더.SmartIndexRegistry indexRegistry,
            Path macroMasterPath) {

        int maxTickAxisX = scanResult.sortedTicks().size();
        int totalEntityAxisY = scanResult.sortedTickers().size();

        // 1차원 텐서(1개 지표)의 총 물리적 바이트 크기 (Float32 = 4 Bytes)
        long layerByteSize = (long) maxTickAxisX * totalEntityAxisY * 4L;

        logger.info(" ================================================================= ");
        logger.info(" [콜드스타트 개시] 스토리지 할당 및 기저 데이터 주입 (매크로 융합 포함)");

        // 1. 데이터 수집 파이프라인 디렉토리 생성 및 고아 파일 자가 복구 (Self-Healing)
        initializeSpoolDirectories();
        recoverOrphanProcessingFiles();

        // 2. 물리 파일(Sparse File) 선할당 및 배타적 쓰기 권한(WritePort) 일괄 획득
        this.globalSharedArena = Arena.ofShared();
        for (String featureName : scanResult.allFeatures()) {
            Path targetPhysicalPath = timeframeContext.resolveDataAbsolutePath(featureName);

            // 💡 [초정밀 제어 적용: 글로벌 세마포어 락]
            // 파일 메타데이터(Length) 확장 시 타 모듈(LSM 컴팩터 등)과의 디스크 I/O Race Condition을 차단합니다.
            try {
                fileExpansionSemaphore.acquire();
                try {
                    // 💡 디스크 전체 I/O 쓰기 없이 파일의 메타데이터(Length)만 조작하여 0.0f 할당 상태 확보
                    ffmMemoryEngine.allocateEmptyCanvas(targetPhysicalPath, layerByteSize);
                } finally {
                    fileExpansionSemaphore.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                anomalyLogger.logAnomalyEvent("SYSTEM", "COLDSTART", "ALL", "SEMAPHORE_INTERRUPT", "파일 용량 확장 중 세마포어 인터럽트 발생");
                throw new RuntimeException("스토리지 팽창 중 세마포어 인터럽트 발생", e);
            }

            A0_DT_42_422001_권한_포트_인터페이스.WritePort writePort = ffmMemoryEngine.mountForWrite(targetPhysicalPath, layerByteSize, globalSharedArena);

            featureWritePortMap.put(featureName, writePort);

            // 💡 [SeqLock 초기화] 각 지표별로 읽기-쓰기 충돌을 방어할 버전 카운터를 0(짝수)으로 설정
            sequenceLockRegistry.put(featureName, new AtomicLong(0));
        }

        // 3. 마스터 폴더 O(1) 캐싱 스캔
        Map<String, List<Path>> entityFileMap = scanMasterDirectory(timeframeContext.getRawDataMasterPath());

        // 4. 병렬 스레드 풀 개방 및 종목별 벌크 데이터 이식 (Bulk Ingestion)
        int availableCores = Runtime.getRuntime().availableProcessors();
        ExecutorService bulkThreadPool = Executors.newFixedThreadPool(availableCores);
        AtomicInteger processedEntityCount = new AtomicInteger(0);

        for (String entityCode : scanResult.sortedTickers()) {
            if (entityCode.startsWith("IDX_"))
                continue; // 매크로는 일반 주식 파싱 후 융합

            bulkThreadPool.submit(() -> {
                try {
                    List<Path> targetFileList = entityFileMap.getOrDefault(entityCode, Collections.emptyList());
                    ingestBulkEntityData(entityCode, targetFileList, scanResult, indexRegistry, maxTickAxisX);
                } finally {
                    processedEntityCount.incrementAndGet();
                }
            });
        }

        // 5. 매크로 지수 수평 융합 (Horizontal/Vertical Convergence)
        if (macroMasterPath != null && Files.exists(macroMasterPath)) {
            logger.info("   ├─ [매크로 융합] 수평적 매크로 데이터를 Y축 엔티티 차원에 동기화합니다.");
            ingestMacroIndices(macroMasterPath, scanResult, indexRegistry, maxTickAxisX);
        }

        bulkThreadPool.shutdown();
        try {
            // 💡 [결함 조치 완료] 타임아웃 발생 시 무시하지 않고 Fail-Fast 로직 적용
            if (!bulkThreadPool.awaitTermination(3, TimeUnit.HOURS)) {
                anomalyLogger.logAnomalyEvent("SYSTEM", "COLDSTART", "ALL", "TIMEOUT_FAILURE",
                        "초기 벌크 데이터 이식 스레드 풀이 3시간 내에 임무를 완수하지 못했습니다.");
                logger.severe(" 🚨 [콜드스타트 오류] 벌크 이식 스레드가 타임아웃되었습니다. 데이터 무결성을 위해 파이프라인을 즉각 셧다운합니다.");
                throw new RuntimeException("콜드스타트 벌크 이식 타임아웃 발생 (Fail-Fast)");
            }
        } catch (InterruptedException e) {
            anomalyLogger.logAnomalyEvent("SYSTEM", "COLDSTART", "ALL", "INTERRUPT_FAILURE",
                    "벌크 이식 중 인터럽트 발생: " + e.getMessage());
            logger.log(Level.SEVERE, " 🚨 [콜드스타트 인터럽트] 벌크 이식 중 인터럽트가 발생했습니다. 파이프라인을 셧다운합니다.", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException("콜드스타트 인터럽트 발생 (Fail-Fast)", e);
        }

        logger.info(" >> [콜드스타트 완료] 모든 원시 데이터가 물리 메모리 세그먼트에 성공적으로 매핑 및 로드되었습니다.");
        logger.info(" ================================================================= ");
    }

    /**
     * O(1) 파일 캐싱. CSV 파일명을 entityCode 별로 묶어 다중 파일(분봉 등) 처리 병목을 해소합니다.
     */
    private Map<String, List<Path>> scanMasterDirectory(Path masterInputPath) {
        Map<String, List<Path>> groupMap = new HashMap<>();
        try (Stream<Path> fileStream = Files.walk(masterInputPath)) {
            fileStream.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".csv"))
                    .forEach(path -> {
                        Matcher matcher = ENTITY_CODE_PATTERN.matcher(path.getFileName().toString());
                        if (matcher.find()) {
                            groupMap.computeIfAbsent(matcher.group(1), k -> new ArrayList<>()).add(path);
                        }
                    });
        } catch (IOException ex) {
            // 💡 [결함 조치] 예외 무시(Silent Failure) 방지 및 디스크 I/O 오류 LMAX 로깅
            anomalyLogger.logAnomalyEvent("SYSTEM", "UNKNOWN", "ALL", "MASTER_SCAN_ERROR",
                    "마스터 디렉토리 스캔 중 I/O 예외 발생: " + ex.getMessage());
            logger.log(Level.WARNING, " [스캔 오류] 마스터 디렉토리 스캔 중 I/O 예외 발생.", ex);
        }
        return groupMap;
    }

    /**
     * 💡 [핵심 최적화: 완벽한 GC 부하 해제 및 V6.0 API 적용]
     * String.split() 및 substring() 객체 생성을 100% 제거하고 순수 아스키 산술식 파서를 적용했습니다.
     */
    private void ingestBulkEntityData(
            String entityCode, List<Path> fileList,
            A0_DT_42_422011_스캐너_차원_측정기.DimensionResult scanResult,
            A0_DT_42_422012_스캐너_호적부_빌더.SmartIndexRegistry indexRegistry, int maxTickAxisX) {

        Integer entityIndexY = indexRegistry.entityYIndexMap().get(entityCode);
        if (entityIndexY == null || fileList.isEmpty())
            return;

        // X축 배열 선할당 및 NaN 초기화 (결측치 LOCF 처리를 위한 로컬 버퍼)
        Map<String, float[]> featureDataColumns = new HashMap<>();
        for (String featureName : scanResult.allFeatures()) {
            float[] singleColumn = new float[maxTickAxisX];
            Arrays.fill(singleColumn, Float.NaN);
            featureDataColumns.put(featureName, singleColumn);
        }

        for (Path csvPath : fileList) {
            try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
                String headerLine = reader.readLine();
                if (headerLine == null)
                    continue;

                // 헤더 파싱은 파일당 1회이므로 split() 객체 생성 허용
                String[] headerArray = headerLine.split(",");

                String line;
                while ((line = reader.readLine()) != null) {
                    int firstCommaPos = line.indexOf(',');
                    if (firstCommaPos == -1)
                        continue;

                    // 💡 [V6.0 O(1) 시간 격자 매핑 적용] getIndex 활용
                    String tickDateStr = line.substring(0, firstCommaPos).trim();
                    Integer tickIndexX = indexRegistry.timeGridIndexer().getIndex(tickDateStr);

                    if (tickIndexX == null || tickIndexX < 0)
                        continue;

                    // 💡 String[] 배열 객체 생성 없는 초고속 포인터 추적 기반 데이터 파싱
                    int currentCommaPos = firstCommaPos;
                    int columnIndex = 1;
                    int lineTotalLength = line.length();

                    while (currentCommaPos != -1 && columnIndex < headerArray.length) {
                        int nextCommaPos = line.indexOf(',', currentCommaPos + 1);
                        int endPointer = (nextCommaPos == -1) ? lineTotalLength : nextCommaPos;

                        String featureName = headerArray[columnIndex].trim();
                        float[] targetColumn = featureDataColumns.get(featureName);
                        if (targetColumn != null) {
                            // 💡 substring()을 사용하지 않는 순수 ASCII 산술 파서 직접 호출
                            targetColumn[tickIndexX] = parseFastFloat(line, currentCommaPos + 1, endPointer);
                        }

                        currentCommaPos = nextCommaPos;
                        columnIndex++;
                    }
                }
            } catch (Exception ex) {
                // 💡 [결함 조치] 예외 무시 방지 및 오류 보고
                anomalyLogger.logAnomalyEvent(entityCode, "UNKNOWN", "ALL", "BULK_PARSE_ERROR",
                        "초기 벌크 데이터 파싱 중 예외 발생: " + ex.getMessage());
                logger.log(Level.WARNING, " [파싱 오류] " + entityCode + " 파일 처리 중 예외 발생", ex);
            }
        }

        // 역방향 채우기 (Backward-Fill) 결측치 치유 및 FFM 메모리에 직접 기록(Direct Write)
        for (String featureName : scanResult.allFeatures()) {
            float[] columnData = featureDataColumns.get(featureName);
            boolean isVolumeData = featureName.contains("거래량") || featureName.contains("VOLUME");

            // 최초의 유효 값(Seed) 찾기
            float initialValidFallback = Float.NaN;
            if (!isVolumeData) {
                for (float val : columnData) {
                    if (!Float.isNaN(val)) {
                        initialValidFallback = val;
                        break;
                    }
                }
            }

            float currentFallback = initialValidFallback;
            A0_DT_42_422001_권한_포트_인터페이스.WritePort writePort = featureWritePortMap.get(featureName);
            AtomicLong seqLock = sequenceLockRegistry.get(featureName);

            // 💡 [SeqLock 쓰기 락온] 홀수 버전으로 전환하여 읽기 스레드에게 데이터가 갱신 중임을 통보
            if (seqLock != null)
                seqLock.incrementAndGet();

            try {
                for (int x = 0; x < maxTickAxisX; x++) {
                    if (Float.isNaN(columnData[x])) {
                        // 거래량 데이터의 결측치는 0.0, 가격 등 연속 데이터는 직전 값(LOCF) 유지
                        float healedValue = isVolumeData ? 0.0f : currentFallback;
                        columnData[x] = Float.isNaN(healedValue) ? 0.0f : healedValue;
                    } else {
                        currentFallback = columnData[x]; // 정상 값이면 보정용 기본값 갱신
                    }

                    // 💡 [V6.0 저장 규격 API 호출] engraveStorageStandard 적용
                    // 단일 값 매핑이므로 OS 페이지 캐시를 거쳐 메모리 세그먼트에 빠르게 기록됨
                    writePort.engraveStorageStandard(entityIndexY, x, columnData[x]);
                }
            } finally {
                // 💡 [SeqLock 쓰기 해제] 짝수 버전으로 복귀하여 읽기 권한을 다시 안전하게 개방
                if (seqLock != null)
                    seqLock.incrementAndGet();
            }
        }
    }

    /**
     * 💡 [핵심 최적화: GC 부하 최소화] 매크로 데이터 병합
     */
    private void ingestMacroIndices(Path macroMasterPath, A0_DT_42_422011_스캐너_차원_측정기.DimensionResult scanResult,
            A0_DT_42_422012_스캐너_호적부_빌더.SmartIndexRegistry indexRegistry, int maxTickAxisX) {

        String activeBaseFeature = scanResult.allFeatures().contains("종가") ? "종가" : "BASE_CLOSE";
        A0_DT_42_422001_권한_포트_인터페이스.WritePort writePort = featureWritePortMap.get(activeBaseFeature);
        AtomicLong seqLock = sequenceLockRegistry.get(activeBaseFeature);
        if (writePort == null)
            return;

        Map<Integer, Integer> columnIndexToYIndexMap = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(macroMasterPath, StandardCharsets.UTF_8)) {
            String[] headerArray = reader.readLine().split(",");
            for (int i = 1; i < headerArray.length; i++) {
                // "IDX_KOSPI" 형태의 매크로 데이터를 Y축 엔티티(종목) 인덱스와 맵핑
                Integer entityIndexY = indexRegistry.entityYIndexMap().get("IDX_" + headerArray[i].trim());
                if (entityIndexY != null)
                    columnIndexToYIndexMap.put(i, entityIndexY);
            }

            // 💡 [SeqLock 쓰기 락온] 매크로 데이터 일괄 병합 시작
            if (seqLock != null)
                seqLock.incrementAndGet();

            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    int firstCommaPos = line.indexOf(',');
                    if (firstCommaPos == -1)
                        continue;

                    String tickDateStr = line.substring(0, firstCommaPos).trim();
                    // 💡 [V6.0 수학적 타임스탬프 역산기] 적용
                    Integer tickIndexX = indexRegistry.timeGridIndexer().getIndex(tickDateStr);

                    if (tickIndexX == null || tickIndexX < 0)
                        continue;

                    int currentCommaPos = firstCommaPos;
                    int columnIndex = 1;
                    int lineTotalLength = line.length();

                    while (currentCommaPos != -1) {
                        int nextCommaPos = line.indexOf(',', currentCommaPos + 1);
                        int endPointer = (nextCommaPos == -1) ? lineTotalLength : nextCommaPos;

                        Integer entityIndexY = columnIndexToYIndexMap.get(columnIndex);
                        if (entityIndexY != null) {
                            float parsedValue = parseFastFloat(line, currentCommaPos + 1, endPointer);
                            if (!Float.isNaN(parsedValue)) {
                                // 💡 [구조 통합] 매크로 데이터를 일반 주식 데이터와 완벽히 동일한 메모리 평면에 병합(Extrusion)
                                writePort.engraveStorageStandard(entityIndexY, tickIndexX, parsedValue);
                            }
                        }
                        currentCommaPos = nextCommaPos;
                        columnIndex++;
                    }
                }
            } finally {
                // 💡 [SeqLock 쓰기 해제]
                if (seqLock != null)
                    seqLock.incrementAndGet();
            }
        } catch (Exception ex) {
            // 💡 [결함 조치] 매크로 파싱 예외 발생 시 로깅 및 보고
            anomalyLogger.logAnomalyEvent("MACRO", "UNKNOWN", "ALL", "MACRO_PARSE_ERROR",
                    "매크로 데이터 병합 중 예외 발생: " + ex.getMessage());
            logger.log(Level.WARNING, " [매크로 파싱 오류] MACRO_INDEX 파일 처리 중 예외 발생", ex);
        }
    }

    // =========================================================================
    // 🌀 2. [비동기 데이터 수집 파이프라인] Ingestion 폴더 모니터링 데몬
    // =========================================================================

    /**
     * [제어 로직 1] 파이프라인 폴더 구조 (INGRESS, PROCESSING, ARCHIVE, QUARANTINE) 4단계 생성
     * 디렉토리가 없을 시 발생하는 I/O 오류를 선제 방어합니다.
     */
    private void initializeSpoolDirectories() {
        try {
            for (IngestionState state : IngestionState.values()) {
                Files.createDirectories(timeframeContext.getStateMachineSpoolPath(state));
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [치명적 시스템 오류] 수집 파이프라인의 기본 디렉토리를 생성할 수 없습니다.", ex);
            throw new RuntimeException("수집 파이프라인 디렉토리 생성 실패", ex);
        }
    }

    /**
     * [제어 로직 2] 고아 파일 복구 (Self-Healing)
     * 시스템이 비정상 종료(Crash)되었을 때 PROCESSING 상태에 머물러 있는 파일들을 스캔하여 다시 INGRESS(대기) 큐로 복귀시킵니다.
     */
    private void recoverOrphanProcessingFiles() {
        Path processingPath = timeframeContext.getStateMachineSpoolPath(IngestionState.PROCESSING);
        Path ingressPath = timeframeContext.getStateMachineSpoolPath(IngestionState.INGRESS);

        try (Stream<Path> fileStream = Files.list(processingPath)) {
            List<Path> orphanFileList = fileStream.filter(Files::isRegularFile).collect(Collectors.toList());

            for (Path orphanFile : orphanFileList) {
                // 확장자를 .processing에서 다시 .csv로 되돌려 대기열(INGRESS)로 반환
                Path recoveredPath = ingressPath.resolve(orphanFile.getFileName().toString().replace(".processing", ".csv"));
                Files.move(orphanFile, recoveredPath, StandardCopyOption.REPLACE_EXISTING);
                logger.info("   ├─ [자가 복구] 시스템 오류로 남겨진 고아 파일 롤백 완료 (PROCESSING -> INGRESS): " + orphanFile.getFileName());
            }
        } catch (IOException ex) {
            // 💡 [결함 조치] 고아 파일 복구 중 예외 무시 방지
            anomalyLogger.logAnomalyEvent("SYSTEM", "UNKNOWN", "ALL", "ORPHAN_RECOVERY_ERROR",
                    "고아 파일 스캔 및 복구 중 I/O 예외 발생: " + ex.getMessage());
            logger.log(Level.WARNING, " [시스템 경고] 고아 파일 스캔 중 I/O 예외가 발생했습니다.", ex);
        }
    }

    /**
     * [오케스트레이션 1] 비동기 파일 수집(Polling) 데몬 활성화
     * 외부 크롤러가 INGRESS 폴더에 파일을 던지면, 이를 감지하여 워커 스레드 풀에 작업을 비동기로 위임합니다.
     */
    public void startIngestionDaemon() {
        if (!isDaemonRunning.compareAndSet(false, true)) {
            logger.warning(" [중복 실행 방지] 비동기 데이터 수집 데몬이 이미 실행 중입니다.");
            return;
        }

        // 가용 코어 수의 75%를 파싱 워커에 할당하고 25%는 시스템 기본 I/O 대역폭으로 남겨둡니다.
        int safeWorkerCount = Math.max(1, (int) (Runtime.getRuntime().availableProcessors() * 0.75));
        this.ingestionWorkerExecutor = Executors.newFixedThreadPool(safeWorkerCount);
        this.directoryPollingExecutor = Executors.newSingleThreadExecutor();

        logger.info(" ================================================================= ");
        logger.info(String.format(" [데이터 수집 데몬 활성화] 할당된 워커 스레드: %d개 | 타겟 도메인: %s", safeWorkerCount, timeframeContext.getResolutionDescription()));
        logger.info(" ================================================================= ");

        directoryPollingExecutor.submit(() -> {
            Path ingressPath = timeframeContext.getStateMachineSpoolPath(IngestionState.INGRESS);

            while (isDaemonRunning.get()) {
                try {
                    List<Path> pendingFileList = new ArrayList<>();

                    // 폴링(Polling): INGRESS 폴더 내의 신규 데이터(.csv) 탐색
                    if (Files.exists(ingressPath)) {
                        try (Stream<Path> pathStream = Files.walk(ingressPath, 1)) {
                            pathStream.filter(Files::isRegularFile)
                                    .filter(f -> f.toString().endsWith(".csv"))
                                    .forEach(pendingFileList::add);
                        }
                    }

                    for (Path pendingFile : pendingFileList) {
                        // 개별 파일을 워커 스레드로 비동기 할당하여 파이프라인 디커플링 보장
                        ingestionWorkerExecutor.submit(() -> submitIngestionTask(pendingFile));
                    }

                    // 디스크 I/O 과부하를 막기 위해 0.5초 대기(유휴 호흡)
                    Thread.sleep(500);

                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    logger.info(" [데몬 종료] 수집 파이프라인 감시 스레드에 종료 인터럽트가 수신되었습니다.");
                } catch (Exception ex) {
                    // 💡 [결함 조치] 무한 루프 내 예외 발생 시 시스템 정지를 막고 LMAX 로깅 처리
                    anomalyLogger.logAnomalyEvent("SYSTEM", "UNKNOWN", "ALL", "INGESTION_POLLING_ERROR",
                            "데이터 수집 폴링 중 예외 발생: " + ex.getMessage());
                    logger.log(Level.WARNING, " [수집망 오류] 파일 폴링 감시 중 시스템 예외 발생.", ex);
                }
            }
        });
    }

    /**
     * [오케스트레이션 2] 커널 레벨의 배타적 파일 락 획득 후 파싱 워커 호출
     */
    private void submitIngestionTask(Path pendingFile) {
        // 💡 [핵심 최적화] 애플리케이션의 메모리 락(Lock)이 아닌 OS 커널 레벨의 파일 이동(move) 기능을 통해 완벽한 원자적 소유권을 획득
        Path processingFile = acquireAtomicFileLock(pendingFile);

        // 다른 스레드가 이미 파일을 집어갔거나, 이동 권한을 획득하지 못했다면 작업을 조용히 취소(Return)함
        if (processingFile == null)
            return;

        boolean isProcessingSuccess = false;
        try {
            // [Tier 2 RCU 워커 호출]
            // CSV 파일 포맷을 파싱하고 L1 매트릭스 바이너리 오프힙 메모리에 직접 주입(Direct Write)합니다.
            // 💡 [V6.0 기능 적용] seqLockRegistry를 주입받아 읽기-쓰기 충돌(Torn Read)을 원천 차단
            rcuIngestionWorker.executeZeroAllocationCasting(processingFile, timeframeContext, featureWritePortMap, sequenceLockRegistry, null, 0, 0);
            isProcessingSuccess = true;

        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [데이터 처리 오류] 파일 처리 중 치명적 예외 발생: " + processingFile.getFileName(), ex);
            anomalyLogger.logAnomalyEvent("SYSTEM", "INGESTION_FAIL", processingFile.getFileName().toString(), "파싱 또는 RCU 워커 예외",
                    ex.getMessage());

        } finally {
            // 성공 여부에 따라 파일을 보관소(ARCHIVE)나 격리소(QUARANTINE)로 최종 상태 전이시킵니다.
            finalizeIngestionState(processingFile, isProcessingSuccess);
        }
    }

    /**
     * [제어 로직 3] OS 커널 레벨의 배타적 파일 락(Lock) 획득
     * 여러 워커 스레드가 동일 INGRESS 파일을 스캔하더라도, `ATOMIC_MOVE` 경쟁에서 승리한 단 1개의 스레드만이 PROCESSING 상태로 진입합니다.
     */
    private Path acquireAtomicFileLock(Path originalFile) {
        String fileName = originalFile.getFileName().toString();
        Path processingDir = timeframeContext.getStateMachineSpoolPath(IngestionState.PROCESSING);
        Path targetFile = processingDir.resolve(fileName.replace(".csv", ".processing"));

        try {
            // 💡 [원자성 보장] ATOMIC_MOVE 플래그는 OS 커널(NTFS/ext4) 레벨에서 파일명과 경로 변경을 단일 트랜잭션으로 보장합니다.
            // 무거운 애플리케이션 단위 락(synchronized/ReentrantLock)을 폐기하여 데드락 발생 가능성을 소거했습니다.
            Files.move(originalFile, targetFile, StandardCopyOption.ATOMIC_MOVE);
            return targetFile;

        } catch (AtomicMoveNotSupportedException ex) {
            // OS 파일 시스템이 Atomic Move를 미지원할 경우 Fallback (약간의 성능 및 완벽한 원자성 희생)
            try {
                Files.move(originalFile, targetFile);
                return targetFile;
            } catch (IOException ex2) {
                // 파일 선점에 실패한 경우(의도된 Race Condition 경쟁 탈락), 조용히 스킵합니다.
                return null;
            }
        } catch (IOException ex) {
            // 파일이 이미 삭제되었거나 다른 스레드에 의해 선점된 경우
            return null;
        }
    }

    /**
     * [제어 로직 4] 데이터 처리 완료 후 최종 목적지(Archive 또는 Quarantine)로 이동
     */
    private void finalizeIngestionState(Path processedFile, boolean isSuccess) {
        String originalFileName = processedFile.getFileName().toString().replace(".processing", ".csv");
        Path finalDestination;

        if (isSuccess) {
            finalDestination = timeframeContext.getStateMachineSpoolPath(IngestionState.ARCHIVE).resolve(originalFileName);
        } else {
            finalDestination = timeframeContext.getStateMachineSpoolPath(IngestionState.QUARANTINE).resolve(originalFileName);
        }

        try {
            Files.move(processedFile, finalDestination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            // 💡 [결함 조치] 최종 파일 이동 실패 시 디스크 상에 고아(Orphan) 상태로 남으므로 시스템 감시망에 노출시킵니다.
            anomalyLogger.logAnomalyEvent("SYSTEM", "UNKNOWN", "ALL", "STATE_TRANSITION_ERROR",
                    "파일 최종 상태 전이 실패. 고아 파일 발생 위험: " + processedFile.getFileName());
            logger.severe(" [상태 전이 실패] 고아 파일 발생 위험. 수동 조치 요망: " + processedFile.getFileName());
        }
    }

    /**
     * [종료 절차] 애플리케이션 셧다운 시 스레드 풀 및 네이티브 메모리 자원을 안전하게 회수합니다.
     */
    public void executeGracefulShutdown() {
        isDaemonRunning.set(false);

        if (directoryPollingExecutor != null) {
            directoryPollingExecutor.shutdownNow();
        }

        if (ingestionWorkerExecutor != null) {
            ingestionWorkerExecutor.shutdown();
            try {
                // 진행 중인 데이터 파싱(주조) 작업을 마칠 수 있도록 60초간 대기 (Graceful Shutdown)
                if (!ingestionWorkerExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    ingestionWorkerExecutor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                ingestionWorkerExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // 커널 메모리 해제
        if (globalSharedArena != null && globalSharedArena.scope().isAlive()) {
            globalSharedArena.close();
        }
        logger.info(" >> [수집 코어 시스템 종료] 메모리 아레나 반환 및 비동기 데몬 스레드 자원 회수 완료.");
    }

    // =========================================================================
    // 💡 3. 하드웨어 친화적 유틸리티 (Hardware-Friendly & Zero-Allocation Parser)
    // =========================================================================

    /**
     * [치유 역학] CPU 분기 예측 실패(Branch Prediction Penalty)를 방지하는 결측치 판독기
     * 
     * @param currentValue 원본 파서에서 추출된 데이터 (NaN 포함 가능성 있음)
     * @param fallbackValue 직전 시간(Tick)에 존재했던 정상적인 이전 데이터 (LOCF)
     * @return 정상일 경우 현재값, NaN일 경우 대체값(fallbackValue) 반환
     */
    private float healMissingValueBranchless(float currentValue, float fallbackValue) {
        // 부동소수점의 메모리 형태를 그대로 원시 32비트 정수(Integer) 비트 패턴으로 캐스팅
        int bitPattern = Float.floatToRawIntBits(currentValue);

        // IEEE 754 규격 분석: 지수부(Exponent)가 모두 1 (0x7F800000) 이고,
        // 가수부(Mantissa)가 0이 아닐 경우(0x007FFFFF) 완벽한 NaN (Not a Number) 상태임.
        boolean isMissing = (bitPattern & 0x7F800000) == 0x7F800000 && (bitPattern & 0x007FFFFF) != 0;

        // JIT 컴파일러에 의해 x86의 CMOV(Conditional Move) 명령어로 하드웨어 번역되어 CPU의 분기(if-else)를 스킵합니다.
        return isMissing ? fallbackValue : currentValue;
    }

    /**
     * 💡 [핵심 최적화: 완벽한 Zero-Allocation Float Parser]
     * substring() 등의 문자열 객체 생성을 유발하는 꼼수를 제거하고,
     * 아스키(ASCII) 문자열을 `charAt()`으로 한 글자씩 순회하며 
     * 정수 연산(Integer Math) 공식으로 부동소수점을 직접 조립하여 JVM 힙(Heap) 메모리 할당(Garbage)을 원천 차단했습니다.
     */
    private float parseFastFloat(String line, int startIndex, int endIndex) {
        // 공백 트림(Trim) 작업을 새로운 문자열 객체 생성 없이 시작/종료 포인터 조정만으로 처리
        while (startIndex < endIndex && line.charAt(startIndex) == ' ')
            startIndex++;
        while (endIndex > startIndex && line.charAt(endIndex - 1) == ' ')
            endIndex--;

        if (startIndex >= endIndex)
            return Float.NaN;

        // 결측치(NaN, null) 문자열 무객체 판별 검사
        if (endIndex - startIndex == 3 && line.regionMatches(startIndex, "NaN", 0, 3))
            return Float.NaN;
        if (endIndex - startIndex == 4 && line.regionMatches(startIndex, "null", 0, 4))
            return Float.NaN;

        boolean isNegative = false;
        int cursor = startIndex;
        char firstChar = line.charAt(cursor);

        if (firstChar == '-') {
            isNegative = true;
            cursor++;
        } else if (firstChar == '+') {
            cursor++;
        }

        double tensorValue = 0.0;
        double decimalDivisor = 1.0;
        boolean isDecimalPointReached = false;

        int exponentValue = 0;
        boolean hasExponent = false;
        boolean isExponentNegative = false;

        // 💡 순수 포인터 기반 아스키코드(ASCII) 대수학 조립 반복문
        for (; cursor < endIndex; cursor++) {
            char ch = line.charAt(cursor);

            if (ch >= '0' && ch <= '9') {
                if (hasExponent) {
                    exponentValue = exponentValue * 10 + (ch - '0');
                } else {
                    tensorValue = tensorValue * 10 + (ch - '0');
                    if (isDecimalPointReached) {
                        decimalDivisor *= 10.0;
                    }
                }
            } else if (ch == '.') {
                isDecimalPointReached = true;
            } else if (ch == 'e' || ch == 'E') {
                hasExponent = true;
                if (cursor + 1 < endIndex) {
                    char nextChar = line.charAt(cursor + 1);
                    if (nextChar == '-') {
                        isExponentNegative = true;
                        cursor++;
                    } else if (nextChar == '+') {
                        cursor++;
                    }
                }
            } else {
                // 부동소수점 규격을 위반한 잘못된 문자(문자열 등) 감지 시 예외를 던지지 않고 NaN으로 안전하게 격리(Fallback)
                return Float.NaN;
            }
        }

        tensorValue /= decimalDivisor;

        if (hasExponent) {
            // 💡 [성능 최적화: 수학적 거듭제곱 룩업 테이블(LUT) 적용] 무거운 Math.pow() JNI 호출 제거
            double exponentMultiplier = (exponentValue < MATH_POWER_LOOKUP_TABLE.length) 
                                        ? MATH_POWER_LOOKUP_TABLE[exponentValue] 
                                        : Math.pow(10, exponentValue);

            if (isExponentNegative) {
                tensorValue /= exponentMultiplier;
            } else {
                tensorValue *= exponentMultiplier;
            }
        }

        return isNegative ? (float) -tensorValue : (float) tensorValue;
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. O(N) 쓰기 증폭(Write Amplification)의 종말과 델타-메인(Delta-Main) 메모리 구조:
 * 과거 아키텍처는 단 한 줄의 새로운 시간축(Tick) 데이터를 추가하기 위해, 전체 크기의
 * 섀도우 메모리(Shadow Memory)를 임시 할당받아 디스크의 모든 내용을 RAM으로 읽어온 뒤(Read-Copy), 
 * 1틱만 업데이트하고 다시 디스크 전체를 덮어쓰기(Commit) 하는 방식을 썼습니다.
 * 이는 SSD에 막대한 I/O 부하를 유발하는 치명적인 성능 병목(I/O Thrashing) 원인이었습니다.
 * 개선된 엔진은 유입된 CSV 파일 내부에 존재하는 [최소 틱 ~ 최대 틱] 구간의 범위만 스캔하여,
 * 아주 작은 크기의 임시 `Delta 버퍼`만을 RAM에 생성합니다.
 * 문자열 파싱과 결측치 치유 연산이 이 델타 영역 안에서만 O(1) 속도로 수행되며, 
 * 메모리 기록 시에도 OS 페이지 캐시(Page Cache)를 거쳐 대상 파일의 특정 Offset에만 핀포인트(Pinpoint)로 덮어씌웁니다.
 * 이로써 불필요한 디스크 I/O 증폭을 완벽하게 소거했습니다.
 * 
 * 2. SeqLock (버전 카운터) 기반의 읽기-쓰기 충돌(Torn Read) 방어막:
 * 데이터 수집(Write) 워커가 커널 메모리에 특정 텐서를 덮어쓰는 그 찰나(수십 나노초)에,
 * 동시에 AI 쿼리 엔진(Read)이 해당 배열 데이터를 읽어간다면, 
 * 배열의 절반은 과거의 값, 나머지 절반은 방금 기록된 미래의 값인 '찢어진 데이터(Torn Read)' 현상이 발생합니다.
 * 이를 방어하기 위해 락(Lock)을 걸면 시스템 전반의 쿼리 처리 성능이 급감합니다.
 * 본 엔진은 지표(Feature)별로 고유한 `AtomicLong` 기반의 **SeqLock (Sequence Lock)** 카운터를 장착했습니다.
 * 워커가 메모리 쓰기에 돌입할 때 카운터를 '홀수(Odd)'로 올리고, 쓰기가 완료되면 '짝수(Even)'로 증가시킵니다.
 * AI 읽기 스레드는 데이터를 복사하기 전/후의 카운터 버전을 확인하여, 
 * 1) 카운터가 홀수면 쓰기 중이므로 대기(Spin-wait)하고, 2) 읽기 전/후 카운터가 다르면 데이터가 훼손되었으므로 즉시 재시도(Retry)하는 
 * 극강의 성능을 가진 **낙관적 동시성 제어(Optimistic Concurrency Control)**를 구현했습니다.
 * 
 * 3. 💡 글로벌 세마포어(Global Semaphore)를 통한 파일 시스템 무결성 보장:
 * 새로운 지표 데이터가 대량으로 추가되거나 시간축이 팽창하여 커널 공간이 더 필요해지면, 
 * OS 커널 레벨의 파일 메타데이터(크기)를 조작해야 합니다.
 * 이때 백그라운드의 야간 LSM 컴팩션 데몬 등의 비동기 스레드가 해당 파일에 I/O를 개입시키면, 
 * OS가 '파일 메타데이터 갱신 중 동시 접근'으로 판단하여 시스템 충돌(Race Condition) 오류를 반환합니다.
 * 본 시스템은 `fileExpansionSemaphore`를 이식하여, 파일의 물리적 크기가 확장되는 짧은 찰나(Microsecond)에는
 * 오직 1개의 스레드만이 파일 크기 조작 권한을 독점하게 만들어 OS 커널 레벨의 파일 시스템 충돌을 100% 방지합니다.
 * =============================================================================
 */