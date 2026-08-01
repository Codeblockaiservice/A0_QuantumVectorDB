/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부
 * @alias Scanner_Dimension_Measurer
 * @tier 1
 * @keywords Deep-walk, Zero-Allocation, Parallel Stream Resilience, Quarantine, Feature Extraction
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422011_스캐너_차원_측정기.java
 * - 역할 (Role): 디스크 파일을 딥워크(Deep-walk)하여 X축(시간)과 Y축(종목/매크로 지수)의 절대 최대 한계치 확정.
 * - 기능 (Function): O(N) 병렬 딥워크 탐색, Zero-Allocation 포인터 파싱 및 I/O 파열 내성 확보.
 * - 이론 (Theory): 분산 병렬 스캐닝(Parallel Scanning), Fail-Safe 스트림 처리 전략, 물리적-논리적 스키마 분리.
 * - 기대효과 (Effect): 파일 시스템 손상 시에도 스트림 전체가 폭파되지 않고 손상 섹터만 격리하며 끝까지 차원을 측량합니다.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [초정밀 통제] 병렬 스트림 I/O 파열 내성(Resilience) 확보: `Files.walk().parallel()` 수행 도중 
 *                 단 1개의 파일 손상(IOException)으로 전체 파이프라인이 붕괴되는 맹점을 파괴했습니다. 
 *                 손상된 파일을 LMAX 로거로 비동기 보고(Quarantine)한 뒤, 스캔을 끝까지 강행하는 Fail-Safe 방어막 전개 완료.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 L1 기저망의 타임프레임 컨텍스트, LMAX 로거, 병렬 스트림(Parallel Stream) 처리를 위한 표준 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of standard libraries for timeframe context of L1 underlying network, LMAX logger, and Parallel Stream processing.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망.A0_DT_42_422033_LMAX_이상_보고서_로거;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 논리적 스키마 정렬의 책임을 지지 않고 현재 디스크의 물리적 상태만을 정찰하는 시공간 차원 측량기입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A spacetime dimension measurer that reconnoiters only the physical state of the current disk without bearing the responsibility of logical schema alignment.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422011
 * [파일명] A0_DT_42_422011_스캐너_차원_측정기.java
 * [모듈명] 통합 OS V6.0 - Tier 1: 절대 시공간 차원 측량기 (순수 물리 스캔 코어)
 * 
 * [설계 명세]
 * 1. 역할: 디스크 파일을 딥워크(Deep-walk)하여 X축(시간)과 Y축(종목/매크로 지수)의 절대 최대 한계치 확정.
 * 2. 기능: String.split()을 배제한 쉼표(,) 포인터 기반 날짜/헤더 추출.
 * 3. 의도: 논리적 스키마 정렬 책임을 배제하고 오직 "현재 존재하는 파일"만 보고하는 순수 물리 탐색.
 * 4. 공식: O(N) 병렬 딥워크 탐색 및 포인터 스캐닝.
 * 5. 기술: Files.walk 병렬 스트림(parallelStream), ConcurrentSkipListSet, Zero-Allocation 포인터 파싱.
 * 6. 💡 [V6.0 초정밀 제어] 병렬 스트림 I/O 파열 내성 확보: `parallelStream`의 단점인 단일 예외에 의한
 * 전체 파이프라인 붕괴를 막기 위해, 내부 예외를 `LMAX_이상_보고서_로거`로 비동기 보고하고(Quarantine)
 * 스캔을 끝까지 강행하는 Fail-Safe 방어 로직을 적용했습니다.
 * ==============================================================================
 */
public final class A0_DT_42_422011_스캐너_차원_측정기 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422011_SCANNER_DIM");

    // 종목코드(6자리 숫자) 추출용 정규식 패턴. 파생상품 코드가 추가될 경우 이 패턴만 갱신합니다.
    private static final Pattern ENTITY_CODE_PATTERN = Pattern.compile("(\\d{6})");

    /**
     * [차원 측량 결과 레코드]
     * 측정된 절대 시공간의 차원 결과를 담는 불변(Immutable) 객체입니다.
     * 하위 모듈(메타데이터 빌더)은 이 팩트 데이터를 바탕으로 과거의 스키마와 교차 대조를 수행합니다.
     */
    public record DimensionResult(
            List<String> sortedTickers, // Y축: 현재 디스크에서 발견된 생존 종목 코드 (IDX_* 포함)
            List<String> sortedTicks,   // X축: 오름차순 정렬된 전체 시간(틱) 배열
            Set<String> allFeatures     // Z축: 발견된 '모든 지표 명칭'의 무질서한 집합 (순서 보장 없음)
    ) {
    }

    /**
     * 순수 정찰 코어이므로 외부에서의 인스턴스화를 허용하되,
     * 상태(State)를 내부 필드로 가지지 않는 완벽한 무상태(Stateless) 아키텍처를 유지합니다.
     */
    public A0_DT_42_422011_스캐너_차원_측정기() {
        logger.info(" >> [통합 OS V6.0] A0_DT_42_422011 차원 측정기 기동. (Fail-Safe I/O 파열 내성 및 순수 물리 스캔 코어 장착)");
    }

    // [1. 한글 상세 주석]
    // 💡 [정찰 역학 1: Fail-Safe 병렬 딥워크 스캔] 디스크의 모든 파일을 훑어 우주의 크기를 측량합니다. 에러 발생 시 스트림 붕괴를 방어합니다.
    // [2. 영문 상세 주석]
    // 💡 [Reconnaissance Dynamics 1: Fail-Safe Parallel Deep-Walk Scan] Sweeps all files on the disk to measure the universe size. Defends against stream collapse upon errors.

    /**
     * 주입받은 타임프레임의 크기를 측량하여 절대 좌표계의 한계를 반환합니다.
     * 
     * @param context       [Tier 0]에서 정의된 타임프레임 및 물리적 경로 컨텍스트
     * @param anomalyLogger [Tier 3] 병렬 스트림 붕괴 방어를 위해 손상된 파일을 비동기 보고할 LMAX 로거
     * @return X, Y, Z축의 최대 크기가 담긴 DimensionResult 구조체
     */
    public DimensionResult scanDimensions(A0_DT_42_422000_타임프레임_컨텍스트 context, A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger) {

        // L3 심연 저장소에 보관된 100% 원시 텍스트 데이터의 물리적 경로를 획득
        Path rawDataMasterPath = context.getRawDataMasterPath();

        logger.info(" ================================================================= ");
        logger.info(String.format(" [차원 스캔 개시] 대상 도메인: %s | 물리 경로: %s", context.getResolutionDescription(), rawDataMasterPath));
        logger.info(" ================================================================= ");
        long startTime = System.currentTimeMillis();

        if (!Files.exists(rawDataMasterPath)) {
            logger.severe(" [시스템 오류] 원시 데이터 경로가 존재하지 않습니다. 스캔을 중단합니다.");
            return new DimensionResult(Collections.emptyList(), Collections.emptyList(), Collections.emptySet());
        }

        // X축(시간)과 Y축(종목)은 수학적 좌표 도출을 위해 오름차순 정렬이 보장되는 ConcurrentSkipListSet 사용
        // 멀티 스레드 병렬 스캔 중에도 Race Condition 없이 완벽한 트리 정렬을 유지합니다.
        Set<String> globalTickSet = new ConcurrentSkipListSet<>();
        Set<String> globalEntitySet = new ConcurrentSkipListSet<>();

        // 💡 [V6.0 철학 규격] Z축(지표)은 스캐너 단에서 절대 정렬하지 않습니다.
        // 오직 멀티 스레드 수집 안정성만을 위해 ConcurrentHashMap 기반의 Set을 사용합니다.
        // 정렬 및 스키마 확정의 주도권은 '메타데이터 빌더'로 이관되었습니다.
        Set<String> globalFeatureSet = ConcurrentHashMap.newKeySet();

        AtomicInteger processedFileCount = new AtomicInteger(0);

        try (Stream<Path> fileStream = Files.walk(rawDataMasterPath)) {
            List<Path> targetFileList = fileStream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".csv"))
                    .collect(Collectors.toList());

            int totalFiles = targetFileList.size();
            if (totalFiles == 0) {
                logger.warning(" [경보] 대상 경로에 CSV 파일이 존재하지 않습니다. 데이터가 빈 상태입니다.");
                return new DimensionResult(Collections.emptyList(), Collections.emptyList(), Collections.emptySet());
            }

            // 💡 병렬 스트림(Parallel Stream)을 개방하여 가용 CPU 코어를 최대한 활용
            targetFileList.parallelStream().forEach(filePath -> {
                try {
                    String fileName = filePath.getFileName().toString();
                    Matcher matcher = ENTITY_CODE_PATTERN.matcher(fileName);

                    if (matcher.find()) {
                        String entityCode = matcher.group(1);
                        globalEntitySet.add(entityCode); // 일반 Y축 (엔티티) 등록

                        fastExtractFeaturesAndTicks(filePath, entityCode, globalTickSet, globalFeatureSet, anomalyLogger);
                    } else if (fileName.equalsIgnoreCase("MACRO_INDEX.csv")) {
                        logger.info("   ├─ [매크로 인덱스 발견] " + fileName + " (거시경제 지표를 Y축 공간에 편입 시도)");
                        fastExtractMacroIndices(filePath, globalEntitySet, globalTickSet, anomalyLogger);
                    }

                    if (processedFileCount.incrementAndGet() % 1000 == 0) {
                        logger.info(String.format("   ├─ 스캔 진행률: %d / %d 파일 완료", processedFileCount.get(), totalFiles));
                    }
                } catch (Exception e) {
                    // 💡 [초정밀 제어 적용: I/O 파열 내성 확보]
                    // 단 1개의 파일 파열이 전체 병렬 스트림을 붕괴시키는 것을 방지하기 위해 내부에서 예외를 캐치합니다.
                    if (anomalyLogger != null) {
                        anomalyLogger.reportAnomaly("SYSTEM", "N/A", "DIMENSION_SCAN", "I/O_PIPELINE_RUPTURE",
                                "파일 판독 중 예외 발생. 딥워크 강제 격리(Quarantine): " + filePath.getFileName() + " - " + e.getMessage());
                    }
                    logger.warning(" [스트림 파손 내성 발동] 손상된 파일 격리 처리 후 스캔을 강행합니다: " + filePath.getFileName());
                }
            });

        } catch (IOException e) {
            logger.log(Level.SEVERE, " [시스템 예외] 디렉토리 딥워크(Deep-Walk) 최상단 스트림에서 물리적 I/O 예외 발생.", e);
        }

        List<String> sortedEntityList = new ArrayList<>(globalEntitySet);
        List<String> sortedTickList = new ArrayList<>(globalTickSet);

        long endTime = System.currentTimeMillis();
        logger.info(String.format(" >> [차원 스캔 완료] 총 소요 시간: %d ms", (endTime - startTime)));
        logger.info(String.format("    ├─ 발견된 Y축 생존 엔티티(매크로 포함): %d 개", sortedEntityList.size()));
        logger.info(String.format("    ├─ 확정된 X축 시간 격자(데이터 틱 수): %d 틱", sortedTickList.size()));
        logger.info(String.format("    └─ 탐지된 Z축 원시 지표 수: %d 개 (스키마 정렬 보류 상태)", globalFeatureSet.size()));

        return new DimensionResult(sortedEntityList, sortedTickList, globalFeatureSet);
    }

    // [1. 한글 상세 주석]
    // 💡 [정찰 역학 2: 초고속 파싱 엔진] String.split() 객체 생성을 제거하고, 일반 데이터에서 날짜와 지표 명칭을 비파괴적으로 추출합니다.
    // [2. 영문 상세 주석]
    // 💡 [Reconnaissance Dynamics 2: Ultra-Fast Parsing Engine] Extracts dates and features non-destructively, eliminating String.split() object creation.

    private void fastExtractFeaturesAndTicks(Path csvPath, String entityCode, Set<String> globalTickSet, Set<String> globalFeatureSet,
            A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger) {
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {

            // 1. 헤더(Z축: 지표) 파싱 - String.split() 객체 생성 억제
            String headerLine = reader.readLine();
            if (headerLine != null) {
                int currentPointer = headerLine.indexOf(',') + 1; // 첫 번째 열(시간) 건너뛰기
                int totalLength = headerLine.length();

                while (currentPointer > 0 && currentPointer < totalLength) {
                    int nextComma = headerLine.indexOf(',', currentPointer);
                    int endPointer = (nextComma == -1) ? totalLength : nextComma;

                    // Substring은 Set에 넣기 위한 최소한의 1회 할당으로 제한
                    String featureName = headerLine.substring(currentPointer, endPointer).trim();
                    globalFeatureSet.add(featureName);

                    currentPointer = (nextComma == -1) ? -1 : nextComma + 1;
                }
            }

            // 2. 바디(X축: 틱) 파싱 - 극한의 메모리 최적화 구간
            String line;
            while ((line = reader.readLine()) != null) {
                // 전체 라인을 나누지 않고, 첫 번째 쉼표 위치까지만 포인터로 잘라냅니다.
                int firstCommaPos = line.indexOf(',');
                if (firstCommaPos > 0) {
                    String tickData = line.substring(0, firstCommaPos).trim();
                    globalTickSet.add(tickData);
                }
            }

        } catch (Exception e) {
            // 💡 [Fail-Safe 결계 발동] 파일 손상 시 비동기 로거로 보고하고 무시합니다.
            if (anomalyLogger != null) {
                anomalyLogger.reportAnomaly(entityCode, "N/A", "ALL_FEATURES", "CORRUPTED_FILE_QUARANTINED",
                        "초고속 추출 중 물리적 손상 의심 파일 발견: " + csvPath.getFileName());
            }
            logger.warning(" [파일 스캔 실패 및 격리] 물리적 손상이 의심되는 파일: " + csvPath.getFileName());
        }
    }

    /**
     * [정찰 역학 3: 매크로 인덱스 전용 파서]
     * MACRO_INDEX.csv 파일의 헤더를 읽어 각 지표를 독립적인 가상 종목(IDX_*)으로 Y축에 등록합니다.
     */
    private void fastExtractMacroIndices(Path csvPath, Set<String> globalEntitySet, Set<String> globalTickSet,
            A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger) {
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {

            // 1. 헤더 파싱 (매크로 지표명을 Y축 가상 종목으로 편입)
            String headerLine = reader.readLine();
            if (headerLine != null) {
                int currentPointer = headerLine.indexOf(',') + 1;
                int totalLength = headerLine.length();

                while (currentPointer > 0 && currentPointer < totalLength) {
                    int nextComma = headerLine.indexOf(',', currentPointer);
                    int endPointer = (nextComma == -1) ? totalLength : nextComma;

                    String macroName = headerLine.substring(currentPointer, endPointer).trim();
                    globalEntitySet.add("IDX_" + macroName); // IDX_ 접두사를 붙여 종목 공간에 융합

                    currentPointer = (nextComma == -1) ? -1 : nextComma + 1;
                }
            }

            // 2. 바디 파싱 (X축 틱 추출)
            String line;
            while ((line = reader.readLine()) != null) {
                int firstCommaPos = line.indexOf(',');
                if (firstCommaPos > 0) {
                    String tickData = line.substring(0, firstCommaPos).trim();
                    // 전체 시간(달력) 차원에 매크로의 시간도 융합하여,
                    // 휴일이 다르거나 데이터 밀도가 달라도 하나의 거대한 위상 격자로 통일시킵니다.
                    globalTickSet.add(tickData);
                }
            }

        } catch (Exception e) {
            if (anomalyLogger != null) {
                anomalyLogger.reportAnomaly("MACRO", "N/A", "ALL_FEATURES", "CORRUPTED_MACRO_FILE",
                        "매크로 인덱스 판독 불가: " + csvPath.getFileName());
            }
            logger.warning(" [매크로 스캔 실패] MACRO_INDEX 파일 판독 불가: " + csvPath.getFileName());
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 물리적 탐색과 논리적 스키마의 완벽한 분리 (Separation of Physical Scan and Logical Schema)
 * 과거 버전의 치명적인 오류는 스캐너가 지표(Feature)들을 수집한 뒤 임의로 정렬(Sorting)하여 인덱스를 부여하려 했다는 점입니다. 
 * 이 방식은 시스템에 새로운 지표가 추가될 때마다 기존 지표들의 인덱스 배열 순서가 변경되어,
 * 특정 채널 인덱스에 의존하여 훈련된 AI 신경망 모델의 가중치(Weight)를 무의미하게 만드는 '스키마 붕괴 현상'을 초래했습니다.
 * V6.0 아키텍처에서 이 모듈은 오직 "현재 스토리지에 어떤 데이터들이 실존하는가?"를 파악하는 '물리적 스캐너'의 역할만 수행합니다.
 * 스캐너가 던져준 정렬되지 않은 지표 집합(Set)은 다음 단계인 메타데이터 빌더(`422012`)로 이관되며,
 * 빌더는 과거에 기록된 `00_METADATA_REGISTRY.json`을 읽어 과거의 스키마 레이아웃을 상속받은 뒤 
 * 오직 새로운 지표만을 맨 끝에 추가(Append-Only)하는 방식으로 모델 훈련 파이프라인의 영구적인 하위 호환성을 보장합니다.
 * 
 * 2. 차원 통합의 기하학 (Geometry of Dimensional Integration)
 * 기존 분석에서는 "일반 주식 데이터"와 "거시경제 데이터(KOSPI, 금리, 환율 등)"를 이질적인 데이터셋으로 취급하여 별도의 파이프라인에서 처리했습니다. 
 * 그러나 옴니-텐서 아키텍처에서는 모든 것을 '동일한 시간축(X축) 위에 놓인 데이터 속성(Y축)'으로 취급합니다.
 * MACRO_INDEX의 헤더를 `IDX_`라는 접두사를 붙여 하나의 '가상 엔티티(Entity)'로 취급하고 Y축 공간에 편입시킵니다. 
 * 이로 인해 쿼리 엔진은 주식 텐서와 매크로 텐서를 동일한 메모리 포인터 접근 방식을 통해 완벽하게 결합(Virtual Extrusion)할 수 있게 되며, 
 * AI 모델의 데이터 입력 구조(Input Pipeline) 복잡도가 획기적으로 낮아집니다.
 * 
 * 3. 💡 병렬 스트림 I/O 파손 내성 확보 (Parallel Stream Resilience):
 * `Files.walk().parallel()`은 수천 개의 파일을 가용 코어 수에 맞게 분배하여 스캔 속도를 기하급수적으로 끌어올리지만,
 * 스트림 내부에서 단 한 개의 파일이라도 깨져 `IOException`을 던진다면 스트림 파이프라인 전체가 즉각 중단되는 한계가 있습니다.
 * 수리된 V6.0 아키텍처는 `forEach` 블록 내부에 철저한 `try-catch` Fail-Safe 로직을 구현하여, 
 * 손상된 파일은 LMAX 로거로 비동기 보고(Quarantine)한 뒤 남은 수천 개의 건전한 파일 스캔을 끝까지 강행합니다.
 * 이는 극단적인 물리 디스크 손상 상황에서도 시스템 데이터베이스가 셧다운 되지 않는 강력한 회복 탄력성(Resilience)을 보장합니다.
 * 
 * 4. 객체 생성 없는 비파괴 스캔 유지 (Zero-Allocation Parsing)
 * CSV 파일을 파싱할 때 `String.split(",")`을 호출하면 매 라인마다 거대한 `String[]` 배열이 힙 메모리에 생성되어
 * 가비지 컬렉터(GC) 부하를 가중시킵니다. V6.0 스캐너는 헤더와 날짜를 추출할 때 쉼표(,)의 `indexOf`
 * 위치만을 추적하여 꼭 필요한 텍스트만 추출합니다. 
 * 대용량의 CSV를 스캔하더라도 힙(Heap) 메모리 오염을 원천적으로 억제하는 고성능 스캐닝 최적화 기법입니다.
 * =============================================================================
 */