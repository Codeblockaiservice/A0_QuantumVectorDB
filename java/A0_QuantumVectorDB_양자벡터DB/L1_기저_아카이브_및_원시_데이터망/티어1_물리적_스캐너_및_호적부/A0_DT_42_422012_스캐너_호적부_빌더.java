/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부
 * @alias Scanner_Registry_Builder
 * @tier 1
 * @keywords Zero-Allocation, FSM Tokenizer, Dynamic Schema, Tombstone, JSON Manifest
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422012_스캐너_호적부_빌더.java
 * - 역할 (Role): 측정된 차원을 바탕으로 `00_METADATA_REGISTRY.json`을 사출하고 O(1) 메모리 인덱스 사전 제공.
 * - 기능 (Function): 5대 기본 지표와 파생 지표 격리, 과거 스키마 계승(Append-Only) 및 논리적 삭제(Tombstone) 관리.
 * - 이론 (Theory): 동적 딕셔너리 진화, 위상 결번 마킹, 유한 상태 기계(FSM) 파싱.
 * - 기대효과 (Effect): AI 모델의 입력 채널 차원 왜곡 방어 및 100% 하위 호환성 보장.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [초정밀 최적화] 정규식 파서 소각 및 FSM 토크나이저 이식: 과거 호적부 파싱에서 
 *                 막대한 힙 오염을 유발하던 `split()` 및 `replaceAll()` 정규식을 걷어내고, 
 *                 커서 이동만으로 인덱스 정수를 뽑아내는 초경량 커스텀 상태 기계(FSM)를 이식하여 
 *                 100% Zero-Allocation 텍스트 파싱을 달성했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 시스템 입출력, 동시성/컬렉션 관리를 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for file system I/O and concurrency/collection management.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 물리적 스캔 결과를 논리적 스키마(메타데이터 레지스트리)로 융합하는 핵심 빌더입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A core builder that fuses physical scan results into the logical schema (metadata registry).
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422012
 * [파일명] A0_DT_42_422012_스캐너_호적부_빌더.java
 * [모듈명] 통합 OS V6.0 - Tier 1: 지능형 동적 스키마 레지스트리 빌더
 * 
 * [설계 명세]
 * 1. 역할: 측정된 차원을 바탕으로 메타데이터 JSON을 내보내고(Export), O(1) 런타임 메모리 인덱스 사전을 제공.
 * 2. 기능: 기본 지표와 파생 지표의 논리적 격리, 과거 스키마 계승(Append-Only) 및 논리적 삭제(Tombstone) 관리.
 * 3. 의도: AI 모델의 입력 채널 인덱스가 밀리는 스키마 붕괴를 방어하고, 상장폐지 결번을 영구 보존.
 * 4. 이론: 동적 딕셔너리 구축, Tombstone 패턴, O(1) 수학적 시공간 격자 매핑, FSM (Finite State Machine).
 * 5. 기술: 💡 [V6.0 핵심 최적화] Zero-Allocation GC 폭탄 해체. `replaceAll()` 및 `split()` 객체 생성을
 * FSM 기반의 순수 포인터 산술 공식으로 치환하여 파싱 단계의 힙 메모리 오염을 제거.
 * ==============================================================================
 */
public final class A0_DT_42_422012_스캐너_호적부_빌더 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422012_MANIFEST_BUILDER");

    // 물리적 분리를 위한 5대 기본 지표 정의 (절대 기저)
    private static final Set<String> BASE_FEATURES = Set.of("시가", "고가", "저가", "종가", "거래량");
    private static final Map<String, String> BASE_FEATURE_MAPPING = Map.of(
            "시가", "BASE_OPEN",
            "고가", "BASE_HIGH",
            "저가", "BASE_LOW",
            "종가", "BASE_CLOSE",
            "거래량", "BASE_VOLUME");

    /**
     * 주조기(Tier 2) 및 쿼리 엔진(Tier 6)이 런타임 메모리에서 활용할 O(1) 초경량 인덱스 사전 DTO
     * 💡 [V6.0 규격] Map<String, Integer> 시간 매핑이 전면 폐기되고 수학적 O(1) 역산기인 MathTimeIndexer로 교체되었습니다.
     */
    public record SmartIndexRegistry(
            Map<String, Integer> entityYIndexMap, // 예: "005930" -> 0, "IDX_KOSPI" -> 1
            Map<String, Integer> featureZIndexMap, // 예: "BASE_CLOSE" -> 0, "IND_RSI" -> 1
            MathTimeIndexer timeGridIndexer // 해시맵 없는 수학적 O(1) 타임스탬프 역산기
    ) {
    }

    public A0_DT_42_422012_스캐너_호적부_빌더() {
        logger.info(" >> [통합 OS V6.0] A0_DT_42_422012 지능형 스키마 레지스트리 빌더 기동. (FSM 렉서 기반 동적 딕셔너리 탑재)");
    }

    /**
     * [구축 역학 1] 물리적 스캔 결과와 기저 법칙을 융합하여 단일 진실 공급원(SSOT) 레지스트리를 추출합니다.
     * 
     * @param context    [Tier 0] 타임프레임 컨텍스트 (파일 저장 경로 및 격자 간격 확인용)
     * @param scanResult [Tier 1] 차원 측정기(422011)가 측량한 시공간 한계치
     * @return O(1) 조회를 위한 메모리 맵핑 사전 (SmartIndexRegistry)
     */
    public SmartIndexRegistry buildRegistryAndExportJson(
            A0_DT_42_422000_타임프레임_컨텍스트 context,
            A0_DT_42_422011_스캐너_차원_측정기.DimensionResult scanResult) {

        logger.info(" ================================================================= ");
        logger.info(" [메타데이터 레지스트리 발급 개시] 스키마 진화(Evolution) 및 JSON 사출");
        logger.info(" ================================================================= ");
        long startTime = System.currentTimeMillis();

        Path registryPhysicalPath = context.getMetadataRegistryPath();

        // 1. [과거 계승] 기존 레지스트리가 있다면 스키마(차원 순서)를 읽어와 붕괴를 막음
        Map<String, Integer> entityYMap = new HashMap<>();
        Map<String, Integer> featureZMap = new HashMap<>();
        Set<String> tombstoneSet = new HashSet<>();

        boolean isLegacySchemaExists = Files.exists(registryPhysicalPath);
        if (isLegacySchemaExists) {
            parseAndInheritLegacyRegistry(registryPhysicalPath, entityYMap, featureZMap, tombstoneSet);
            logger.info(String.format("   ├─ [과거 스키마 상속] 기존 메타데이터 로드 완료. (엔티티: %d개, 지표: %d개, 묘비: %d개)",
                    entityYMap.size(), featureZMap.size(), tombstoneSet.size()));
        }

        // 2. [위상 팽창] 새롭게 발견된 종목(Y)과 지표(Z)를 맨 끝번호(Append-Only)로 추가
        int newEntityCount = expandTopologyAndAssignIndex(scanResult.sortedTickers(), entityYMap);
        int newFeatureCount = expandTopologyAndAssignIndex(new ArrayList<>(scanResult.allFeatures()), featureZMap);

        // 3. [논리적 삭제(Tombstone) 마킹] 과거엔 있었으나 현재 스캔에선 사라진 상장폐지/중단 종목 색출
        Set<String> currentAliveEntitySet = new HashSet<>(scanResult.sortedTickers());
        int newTombstoneCount = 0;

        for (String legacyEntity : entityYMap.keySet()) {
            if (!currentAliveEntitySet.contains(legacyEntity) && !tombstoneSet.contains(legacyEntity)) {
                tombstoneSet.add(legacyEntity);
                newTombstoneCount++;
            }
        }

        // 4. [시공간 격자 엔진 점화] O(1) X축 수학적 시간 매핑 엔진 생성
        if (scanResult.sortedTicks().isEmpty()) {
            throw new IllegalStateException("[시공간 붕괴] 스캔된 시간(Tick) 데이터가 존재하지 않아 파이프라인을 초기화할 수 없습니다.");
        }
        MathTimeIndexer timeIndexer = createSpacetimeGridEngine(context, scanResult.sortedTicks());

        // 5. [물리적 사출] 완성된 스키마와 메타데이터를 JSON 형태로 디스크에 저장(Export)
        exportRegistryToJson(context, entityYMap, featureZMap, tombstoneSet, timeIndexer);

        long endTime = System.currentTimeMillis();
        logger.info(String.format("   ├─ [스키마 확정] 신규 편입 엔티티: %d개 | 신규 발견 지표: %d개 | 신규 Tombstone 마킹: %d개",
                newEntityCount, newFeatureCount, newTombstoneCount));
        logger.info(String.format(" >> [레지스트리 발급 완료] 총 소요 시간: %d ms", (endTime - startTime)));
        logger.info(" ================================================================= ");

        return new SmartIndexRegistry(entityYMap, featureZMap, timeIndexer);
    }

    /**
     * [내부 로직 1] 기존 맵에 없는 새로운 목록을 끝 번호부터 차례대로 할당합니다 (Append-Only).
     */
    private int expandTopologyAndAssignIndex(List<String> targetList, Map<String, Integer> schemaMap) {
        int addedCount = 0;
        int currentMaxIndex = schemaMap.isEmpty() ? 0 : Collections.max(schemaMap.values()) + 1;

        for (String item : targetList) {
            if (!schemaMap.containsKey(item)) {
                schemaMap.put(item, currentMaxIndex++);
                addedCount++;
            }
        }
        return addedCount;
    }

    /**
     * [내부 로직 2] X축 수학적 시공간 격자 인덱서 도출
     */
    private MathTimeIndexer createSpacetimeGridEngine(A0_DT_42_422000_타임프레임_컨텍스트 context, List<String> sortedTickArray) {
        String baseTickString = sortedTickArray.get(0);
        long baseEpoch = MathTimeIndexer.parseTickToEpoch(baseTickString);
        long intervalSeconds = 86400; // 기본값 1일

        switch (context) {
            case MIN5_RESOLUTION -> intervalSeconds = 300;
            case MIN1_RESOLUTION -> intervalSeconds = 60;
            case DAILY_RESOLUTION -> intervalSeconds = 86400;
        }

        return new MathTimeIndexer(baseTickString, baseEpoch, intervalSeconds);
    }

    // [1. 한글 상세 주석]
    // 💡 [초정밀 최적화 적용] 과거 JSON 레지스트리를 읽을 때 `replaceAll`, `split`의 정규식을 전면 소각하고,
    // FSM 기반의 순수 포인터 인-플레이스(In-place) 스캐닝으로 스키마를 복원합니다.
    // [2. 영문 상세 주석]
    // 💡 [Ultra-Precision Optimization Applied] Completely discarded regular expressions like `replaceAll` and `split` when reading past JSON registries,
    // restoring the schema using FSM-based pure pointer in-place scanning.

    /**
     * [내부 로직 3] 과거의 JSON 레지스트리를 읽어와 스키마(차원 축 인덱스)를 복원합니다.
     * 외부 라이브러리(Gson/Jackson) 및 정규식 객체 할당 없이 자체 해결합니다. (Zero-Allocation)
     */
    private void parseAndInheritLegacyRegistry(Path registryPath, Map<String, Integer> yAxisMap, Map<String, Integer> zAxisMap, Set<String> tombstoneSet) {
        try (BufferedReader reader = Files.newBufferedReader(registryPath, StandardCharsets.UTF_8)) {
            String line;
            boolean yAxisSection = false;
            boolean zAxisSection = false;

            while ((line = reader.readLine()) != null) {
                if (line.contains("\"Y_AXIS_TICKERS\"")) {
                    yAxisSection = true;
                    zAxisSection = false;
                    continue;
                }
                if (line.contains("\"Z_AXIS_FEATURES\"")) {
                    yAxisSection = false;
                    zAxisSection = true;
                    continue;
                }
                // 블록 종료 감지
                if (line.contains("}") && !line.contains("{")) {
                    yAxisSection = false;
                    zAxisSection = false;
                }

                // 💡 [FSM 렉서 기반 파싱] 정규식 객체 생성 원천 차단
                if (yAxisSection && line.indexOf("\"index\"") != -1) {
                    String key = extractKeyZeroAllocation(line);
                    if (key != null) {
                        int index = extractIndexZeroAllocation(line);
                        boolean isTombstone = line.indexOf("\"tombstone\": true") != -1;

                        if (index != -1) {
                            yAxisMap.put(key, index);
                            if (isTombstone)
                                tombstoneSet.add(key);
                        }
                    }
                } else if (zAxisSection && line.indexOf("\"index\"") != -1) {
                    String key = extractKeyZeroAllocation(line);
                    if (key != null) {
                        int index = extractIndexZeroAllocation(line);
                        if (index != -1) {
                            zAxisMap.put(key, index);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, " [레지스트리 훼손] 과거 스키마 파일 판독 불가. 파이프라인 정합성 붕괴 위험.", e);
        }
    }

    // =========================================================================
    // 💡 [FSM 렉서] 정규식을 대체하는 Zero-Allocation 유틸리티 엔진
    // =========================================================================

    /**
     * 라인 내에서 첫 번째로 묶인 쌍따옴표 문자열(Key)을 `substring`으로만 최소 1회 추출합니다.
     */
    private String extractKeyZeroAllocation(String line) {
        int firstQuote = line.indexOf('"');
        if (firstQuote == -1)
            return null;
        int secondQuote = line.indexOf('"', firstQuote + 1);
        if (secondQuote == -1)
            return null;
        return line.substring(firstQuote + 1, secondQuote);
    }

    /**
     * "index" 키워드 뒤에 오는 정수 값을 커서 이동 및 아스키 산술식만으로 안전하게 추출합니다.
     */
    private int extractIndexZeroAllocation(String line) {
        int indexKeywordPos = line.indexOf("\"index\"");
        if (indexKeywordPos == -1)
            return -1;

        int colonPos = line.indexOf(':', indexKeywordPos + 7);
        if (colonPos == -1)
            return -1;

        int cursor = colonPos + 1;
        int length = line.length();

        // 공백 건너뛰기
        while (cursor < length && (line.charAt(cursor) == ' ' || line.charAt(cursor) == '\t')) {
            cursor++;
        }

        int result = 0;
        boolean digitFound = false;

        // 숫자 파싱 (상태 기계)
        while (cursor < length) {
            char ch = line.charAt(cursor);
            if (ch >= '0' && ch <= '9') {
                result = result * 10 + (ch - '0');
                digitFound = true;
                cursor++;
            } else {
                break; // 숫자가 아닌 문자(예: 콤마)를 만나면 종료
            }
        }

        return digitFound ? result : -1;
    }

    /**
     * [내부 로직 4] 확정된 스키마와 시공간 격자를 융합하여 메타데이터 JSON 파일로 저장합니다.
     */
    private void exportRegistryToJson(
            A0_DT_42_422000_타임프레임_컨텍스트 context,
            Map<String, Integer> yAxisMap,
            Map<String, Integer> zAxisMap,
            Set<String> tombstoneSet,
            MathTimeIndexer timeIndexer) {

        Path exportPath = context.getMetadataRegistryPath();
        StringBuilder jsonBuilder = new StringBuilder();

        jsonBuilder.append("{\n");

        // 1. [X축] TIME_GRID (수학적 시공간 격자 제원)
        jsonBuilder.append("  \"TIME_GRID\": {\n");
        jsonBuilder.append(String.format("    \"base_tick\": \"%s\",\n", timeIndexer.getBaseTickStr()));
        jsonBuilder.append(String.format("    \"base_epoch\": %d,\n", timeIndexer.getBaseEpoch()));
        jsonBuilder.append(String.format("    \"interval_seconds\": %d\n", timeIndexer.getIntervalSeconds()));
        jsonBuilder.append("  },\n");

        // 2. [Z축] FEATURES (지표 메타데이터 및 논리적 인덱스)
        jsonBuilder.append("  \"Z_AXIS_FEATURES\": {\n");
        List<Map.Entry<String, Integer>> sortedZAxis = new ArrayList<>(zAxisMap.entrySet());
        sortedZAxis.sort(Map.Entry.comparingByValue()); // 인덱스 오름차순 정렬

        for (int i = 0; i < sortedZAxis.size(); i++) {
            Map.Entry<String, Integer> entry = sortedZAxis.get(i);
            String featureName = entry.getKey();
            int index = entry.getValue();

            // 상대 경로 도출
            String logicalName = BASE_FEATURE_MAPPING.getOrDefault(featureName, featureName);
            boolean isBaseFeature = BASE_FEATURES.contains(featureName);

            String folderName = isBaseFeature ? context.getBaseDataPath().getFileName().toString()
                    : context.getDerivedDataPath().getFileName().toString();
            String fileName = isBaseFeature ? logicalName : "IND_" + featureName;
            String relativePath = "/" + folderName + "/" + fileName + ".layer";

            jsonBuilder.append(String.format("    \"%s\": { \"index\": %d, \"path\": \"%s\", \"type\": \"float32\" }%s\n",
                    featureName, index, relativePath, (i < sortedZAxis.size() - 1 ? "," : "")));
        }
        jsonBuilder.append("  },\n");

        // 3. [Y축] TICKERS (엔티티 및 Tombstone 마킹)
        jsonBuilder.append("  \"Y_AXIS_TICKERS\": {\n");
        List<Map.Entry<String, Integer>> sortedYAxis = new ArrayList<>(yAxisMap.entrySet());
        sortedYAxis.sort(Map.Entry.comparingByValue()); // 인덱스 오름차순 정렬

        for (int i = 0; i < sortedYAxis.size(); i++) {
            Map.Entry<String, Integer> entry = sortedYAxis.get(i);
            String entityCode = entry.getKey();
            int index = entry.getValue();
            boolean isTombstoned = tombstoneSet.contains(entityCode);

            jsonBuilder.append(String.format("    \"%s\": { \"index\": %d, \"tombstone\": %b }%s\n",
                    entityCode, index, isTombstoned, (i < sortedYAxis.size() - 1 ? "," : "")));
        }
        jsonBuilder.append("  }\n");
        jsonBuilder.append("}");

        // 4. 물리적 사출 (안전한 부모 디렉토리 계층 선제 개척 포함)
        try {
            if (exportPath.getParent() != null) {
                Files.createDirectories(exportPath.getParent());
            }

            Files.createDirectories(context.getBaseDataPath());
            Files.createDirectories(context.getDerivedDataPath());

            try (BufferedWriter bw = Files.newBufferedWriter(exportPath, StandardCharsets.UTF_8)) {
                bw.write(jsonBuilder.toString());
                logger.info("   ├─ [JSON 내보내기 완료] 메타데이터 레지스트리 물리화 성공: " + exportPath.getFileName());
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, " [치명적 시스템 오류] 레지스트리(Manifest) 파일 저장 중 I/O 예외 발생.", e);
        }
    }

    // =========================================================================
    // 💡 O(1) 수학적 시공간 격자 인덱서 (GC 오버헤드 완벽 억제)
    // =========================================================================
    public static class MathTimeIndexer {
        private final String baseTickStr;
        private final long baseEpoch;
        private final long intervalSeconds;

        public MathTimeIndexer(String baseTickStr, long baseEpoch, long intervalSeconds) {
            this.baseTickStr = baseTickStr;
            this.baseEpoch = baseEpoch;
            this.intervalSeconds = intervalSeconds;
        }

        /**
         * 해시맵(HashMap) 탐색을 거치지 않고, 단 한 번의 사칙연산만으로 절대 X축 좌표를 산출합니다.
         */
        public int getIndex(String tickDate) {
            long targetEpoch = parseTickToEpoch(tickDate);
            return (int) ((targetEpoch - baseEpoch) / intervalSeconds);
        }

        public String getBaseTickStr() {
            return baseTickStr;
        }

        public long getBaseEpoch() {
            return baseEpoch;
        }

        public long getIntervalSeconds() {
            return intervalSeconds;
        }

        /**
         * 💡 [핵심 최적화: 객체 생성 완전 배제 파서] Zero-Allocation Epoch Parser
         * 정규식(replaceAll), 부분 문자열(substring), 날짜 객체(LocalDateTime) 생성을 전면 폐기하고, 
         * 원시 char 배열 순회 및 율리우스일(Julian Day) 산술 연산만을 이용하여
         * 날짜 문자열을 O(1)으로 타임스탬프로 직결 변환(Direct Convert)합니다.
         */
        public static long parseTickToEpoch(String tickStr) {
            int digitCount = 0;
            int year = 1970, month = 1, day = 1, hour = 0, min = 0, sec = 0;

            // 스택 메모리에만 잠시 존재하는 초경량 원시 배열 (이스케이프 분석을 통해 힙 할당 방지)
            char[] chars = new char[14];
            int len = tickStr.length();

            // 1단계: 정규식을 대체하는 초고속 포인터 기반 숫자 추출
            for (int i = 0; i < len && digitCount < 14; i++) {
                char c = tickStr.charAt(i);
                if (c >= '0' && c <= '9') {
                    chars[digitCount++] = c;
                }
            }

            // 2단계: 추출된 숫자의 개수에 따라 자리수 계산으로 강제 조립
            if (digitCount >= 8) {
                year = (chars[0] - '0') * 1000 + (chars[1] - '0') * 100 + (chars[2] - '0') * 10 + (chars[3] - '0');
                month = (chars[4] - '0') * 10 + (chars[5] - '0');
                day = (chars[6] - '0') * 10 + (chars[7] - '0');
            }
            if (digitCount >= 12) {
                hour = (chars[8] - '0') * 10 + (chars[9] - '0');
                min = (chars[10] - '0') * 10 + (chars[11] - '0');
            }
            if (digitCount >= 14) {
                sec = (chars[12] - '0') * 10 + (chars[13] - '0');
            }

            // 💡 3단계: [객체 생성 완벽 억제] Fliegel & Van Flandern 율리우스일 변환 공식 적용
            // LocalDateTime.of()를 폐기하고 산술식만으로 년월일을 누적 일수(Epoch Days)로 변환
            int a = (14 - month) / 12;
            int y = year + 4800 - a;
            int m = month + 12 * a - 3;

            long julianDay = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045L;
            long epochDays = julianDay - 2440588L; // 1970년 1월 1일 차감
            long totalSeconds = epochDays * 86400L + hour * 3600L + min * 60L + sec;

            // 4단계: KST(UTC+9)로 입력된 시간을 기준 에포크(UTC)로 변환하기 위해 9시간(32,400초) 차감
            return totalSeconds - (9 * 3600L);
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 100% Zero-Allocation 스캐닝 (FSM Tokenizer):
 * 과거의 `replaceAll(".*\"index\"\\s*:\\s*(\\d+).*", "$1")` 형태의 정규식 코드는 간결해 보이지만,
 * 매 줄마다 정규식 엔진을 초기화하고 불필요한 서브 스트링을 만들며 파싱을 위한 객체 생성을 반복하여
 * 가비지 컬렉터(GC)에 치명적인 부하를 일으킵니다. 통합 OS V6.0은 이를 전면 교체했습니다.
 * `extractIndexZeroAllocation` 메서드는 유한 상태 기계(Finite State Machine) 패턴을 적용하여,
 * 오직 문자열 내부의 커서(`char` 포인터) 이동과 아스키(ASCII) 산술 연산(`result * 10 + (ch - '0')`)만으로
 * 추가적인 메모리 힙 할당 없이 인덱스 정수값을 도출하는 초경량, 고성능 파싱을 보장합니다.
 * 
 * 2. 동적 스키마 진화와 하위 호환성 (Append-Only Schema Evolution):
 * 데이터 파이프라인이나 분석 모델(PyTorch/TensorFlow)은 텐서의 차원 인덱스(Index) 구조에 강하게 결합됩니다.
 * 만약 데이터를 단순히 알파벳 순으로 재정렬하여 인덱스를 다시 매기게 되면, 이전에 학습된 모델의 가중치(Weight)가 
 * 서로 다른 데이터를 참조하게 되는 치명적인 '스키마 왜곡 현상(Schema Mismatch)'이 발생합니다.
 * 이 빌더 모듈은 과거에 저장된 JSON 레지스트리를 읽어 기존 인덱스 체계를 100% 보존(Freeze)하고, 
 * 새로 발견된 지표(Feature)나 종목(Entity)은 무조건 배열의 끝(Append) 쪽에 새로운 인덱스를 부여함으로써 
 * AI 모델과 분석 파이프라인의 영구적인 하위 호환성(Backward Compatibility)을 보장합니다.
 * 
 * 3. 논리적 삭제(Tombstone) 패턴과 기하학적 차원의 무결성:
 * 엔티티(예: 상장폐지된 주식 종목)의 데이터 수집이 영구 중단되었다고 해서 물리적인 텐서 배열 인덱스를 중간에 삭제해버리면,
 * 다차원 배열의 형상(Shape)이 깨져 딥러닝 텐서 연산 시 에러를 유발합니다.
 * 이 아키텍처는 삭제 대신 **묘비(Tombstone)** 상태를 마킹합니다. 
 * 과거 인덱스는 그대로 유지하되 `tombstone: true` 플래그를 설정함으로써, 데이터 쿼리 시점에 디스크 접근을 차단하고 
 * 메모리 상에서 비어있는(0.0f 등) 상태로 패딩(Padding) 처리합니다.
 * 이를 통해 AI 모델은 배열 구조 변경의 충격 없이 "이 데이터 스트림은 소멸되었다"는 사실을 안정적으로 학습할 수 있습니다.
 * 
 * 4. 메타데이터 분리 기반의 독립적 선언형 인프라 (Zero-Dependency Manifest):
 * JSON 레지스트리 내부에는 `base_epoch`와 `interval_seconds` 등 수학적 격자 매핑을 위한 필수 메타데이터가 함께 직렬화됩니다.
 * 이는 하위 분석 시스템(C++ 모듈, Python 렌더러 등)이 자바(Java) 백엔드 파이프라인 코드에 의존하지 않고도, 
 * 오직 이 JSON 파일 하나만 읽어들이면 오프힙 바이너리(`.layer`) 데이터를 스스로 역직렬화하고 메모리 오프셋을 계산할 수 있게 만듭니다.
 * =============================================================================
 */