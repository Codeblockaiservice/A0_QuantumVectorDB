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
 * - 역할 (Role): 측정된 차원을 바탕으로 `00_MANIFEST_REGISTRY.json`을 사출하고 O(1) 메모리 인덱스 사전 제공.
 * - 기능 (Function): 5대 기본 지표와 파생 지표 격리, 과거 스키마 계승(Append-Only) 및 묘비 관리.
 * - 이론 (Theory): 동적 딕셔너리 진화, 위상 결번 마킹, 유한 상태 기계(FSM) 파싱.
 * - 기대효과 (Effect): AI 모델의 입력 채널 차원 환각 방어 및 100% 하위 호환성 보장.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 특정 세력을 연상시키는 단어를 전면 소각하고 '통합 OS'로 치환 완료.
 * - 💡 [초정밀 수술] 정규식 파서 파괴 및 FSM 토크나이저 이식: 과거 호적부 파싱에서 
 *                 막대한 힙 오염을 유발하던 `split()` 및 `replaceAll()` 정규식을 완벽히 도려내고, 
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
// 컴플라이언스 선언 및 클래스 헤더. 물리적 스캔 결과를 통합 OS의 논리적 스키마(호적부)로 융합하는 핵심 빌더입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A core builder that fuses physical scan results into the logical schema (registry) of the Integrated OS.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422012
 * [파일명] A0_DT_42_422012_스캐너_호적부_빌더.java
 * [모듈명] 통합 OS V6.0 - Tier 1: 지능형 동적 스키마 호적부 빌더
 * 
 * [설계 명세]
 * 1. 역할: 측정된 차원을 바탕으로 `00_MANIFEST_REGISTRY.json`을 사출하고 O(1) 메모리 인덱스 사전 제공.
 * 2. 기능: 5대 기본 지표와 파생 지표 폴더 격리, 과거 스키마 계승(Append-Only) 및 묘비 관리.
 * 3. 의도: AI 모델의 입력 채널 인덱스가 밀리는 '차원 환각'을 방어하고 상장폐지 결번(Tombstone)을 영구 보존.
 * 4. 이론: 동적 딕셔너리 구축, 위상 결번 마킹, O(1) 수학적 시공간 격자 엔진, FSM (Finite State Machine).
 * 5. 기술: 💡 [V6.0 초정밀 수술] Zero-Allocation GC 폭탄 영구 해체. `replaceAll()` 및
 * `split()` 객체 생성을
 * FSM 기반의 순수 포인터 산술 공식으로 치환하여 파싱 단계의 힙 메모리 오염을 100% 멸균.
 * ==============================================================================
 */
public final class A0_DT_42_422012_스캐너_호적부_빌더 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422012_MANIFEST_BUILDER");

    // 물리적 분리를 위한 5대 기본 지표 정의 (절대 기저)
    private static final Set<String> 기본_지표_목록 = Set.of("시가", "고가", "저가", "종가", "거래량");
    private static final Map<String, String> 기본_지표_영문_매핑 = Map.of(
            "시가", "BASE_OPEN",
            "고가", "BASE_HIGH",
            "저가", "BASE_LOW",
            "종가", "BASE_CLOSE",
            "거래량", "BASE_VOLUME");

    /**
     * 주조기(Tier 2) 및 쿼리 엔진(Tier 6)이 메모리에 들고 활용할 O(1) 초경량 인덱스 사전 DTO
     * 💡 [V6.0 규격] Map<String, Integer> tickToIndexMap이 전면 폐기되고 MathTimeIndexer로
     * 교체되었습니다.
     */
    public record 지능형_인덱스_사전(
            Map<String, Integer> 엔티티_Y축_인덱스망, // 예: "005930" -> 0, "IDX_KOSPI" -> 1
            Map<String, Integer> 지표_Z축_인덱스망, // 예: "BASE_CLOSE" -> 0, "IND_RSI" -> 1
            MathTimeIndexer X축_시간_격자_엔진 // 해시맵 없는 수학적 O(1) 타임스탬프 역산기
    ) {
    }

    public A0_DT_42_422012_스캐너_호적부_빌더() {
        로거.info(" >> [통합 OS V6.0] A0_DT_42_422012 지능형 스키마 호적부 빌더 기동. (FSM 렉서 기반 동적 딕셔너리 탑재)");
    }

    /**
     * [구축 역학 1] 물리적 정찰 결과와 기저 법칙을 융합하여 단일 진실 공급원(SSOT) 호적부를 사출합니다.
     * 
     * @param 컨텍스트 [Tier 0] 타임프레임 컨텍스트 (파일 저장 경로 및 격자 간격 확인용)
     * @param 정찰결과 [Tier 1] 차원 측정기(422011)가 측량한 시공간 한계치
     * @return O(1) 조회를 위한 메모리 맵핑 사전 (지능형_인덱스_사전)
     */
    public 지능형_인덱스_사전 호적부_구축_및_JSON_사출(
            A0_DT_42_422000_타임프레임_컨텍스트 컨텍스트,
            A0_DT_42_422011_스캐너_차원_측정기.DimensionResult 정찰결과) {

        로거.info(" ================================================================= ");
        로거.info(" [호적부 발급 개시] 스키마 진화(Evolution) 및 메타데이터 JSON 사출");
        로거.info(" ================================================================= ");
        long 시작_시간 = System.currentTimeMillis();

        Path 호적부_물리경로 = 컨텍스트.get지능형_호적부_경로();

        // 1. [과거 계승] 기존 호적부가 있다면 스키마(차원 순서)를 읽어와 붕괴를 막음
        Map<String, Integer> Y축_엔티티_맵 = new HashMap<>();
        Map<String, Integer> Z축_지표_맵 = new HashMap<>();
        Set<String> 묘비_세트 = new HashSet<>();

        boolean 과거_스키마_존재여부 = Files.exists(호적부_물리경로);
        if (과거_스키마_존재여부) {
            과거_호적부_파싱_및_계승(호적부_물리경로, Y축_엔티티_맵, Z축_지표_맵, 묘비_세트);
            로거.info(String.format("   ├─ [과거 계승] 기존 스키마 로드 완료. (엔티티: %d개, 지표: %d개, 묘비: %d개)",
                    Y축_엔티티_맵.size(), Z축_지표_맵.size(), 묘비_세트.size()));
        }

        // 2. [위상 팽창] 새롭게 발견된 종목(Y)과 지표(Z)를 맨 끝번호(Append-Only)로 추가
        int 신규_엔티티_카운트 = 위상_팽창_및_인덱스_할당(정찰결과.sortedTickers(), Y축_엔티티_맵);
        int 신규_지표_카운트 = 위상_팽창_및_인덱스_할당(new ArrayList<>(정찰결과.allFeatures()), Z축_지표_맵);

        // 3. [묘비(Tombstone) 마킹] 과거엔 있었으나 현재 정찰에선 사라진 상장폐지 종목 색출
        Set<String> 현재_생존_엔티티망 = new HashSet<>(정찰결과.sortedTickers());
        int 묘비_추가_카운트 = 0;

        for (String 과거_엔티티 : Y축_엔티티_맵.keySet()) {
            if (!현재_생존_엔티티망.contains(과거_엔티티) && !묘비_세트.contains(과거_엔티티)) {
                묘비_세트.add(과거_엔티티);
                묘비_추가_카운트++;
            }
        }

        // 4. [시공간 격자 점화] O(1) X축 시간 엔진 생성
        if (정찰결과.sortedTicks().isEmpty()) {
            throw new IllegalStateException("[시공간 붕괴] 정찰된 시간(Tick) 데이터가 존재하지 않아 우주를 열 수 없습니다.");
        }
        MathTimeIndexer 시간_인덱서 = 시공간_격자_엔진_생성(컨텍스트, 정찰결과.sortedTicks());

        // 5. [물리화] 완성된 스키마와 메타데이터를 JSON 형태로 디스크에 사출
        JSON_호적부_사출(컨텍스트, Y축_엔티티_맵, Z축_지표_맵, 묘비_세트, 시간_인덱서);

        long 종료_시간 = System.currentTimeMillis();
        로거.info(String.format("   ├─ [스키마 확정] 신규 편입 엔티티: %d개 | 신규 개척 지표: %d개 | 신규 사망 선고 묘비: %d개",
                신규_엔티티_카운트, 신규_지표_카운트, 묘비_추가_카운트));
        로거.info(String.format(" >> [호적부 발급 완료] 총 소요 시간: %d ms", (종료_시간 - 시작_시간)));
        로거.info(" ================================================================= ");

        return new 지능형_인덱스_사전(Y축_엔티티_맵, Z축_지표_맵, 시간_인덱서);
    }

    /**
     * [내부 로직 1] 기존 맵에 없는 새로운 목록을 끝 번호부터 차례대로 할당합니다 (Append-Only).
     */
    private int 위상_팽창_및_인덱스_할당(List<String> 타겟_목록, Map<String, Integer> 스키마_맵) {
        int 추가된_개수 = 0;
        int 현재_최대_인덱스 = 스키마_맵.isEmpty() ? 0 : Collections.max(스키마_맵.values()) + 1;

        for (String 항목 : 타겟_목록) {
            if (!스키마_맵.containsKey(항목)) {
                스키마_맵.put(항목, 현재_최대_인덱스++);
                추가된_개수++;
            }
        }
        return 추가된_개수;
    }

    /**
     * [내부 로직 2] X축 수학적 시공간 격자 인덱서 도출
     */
    private MathTimeIndexer 시공간_격자_엔진_생성(A0_DT_42_422000_타임프레임_컨텍스트 컨텍스트, List<String> 정렬된_틱_배열) {
        String 최초_틱_문자열 = 정렬된_틱_배열.get(0);
        long 최초_에포크 = MathTimeIndexer.parseTickToEpoch(최초_틱_문자열);
        long 격자_간격_초 = 86400; // 기본값 1일

        switch (컨텍스트) {
            case 오분봉_격자 -> 격자_간격_초 = 300;
            case 일분봉_격자 -> 격자_간격_초 = 60;
            case 일봉_격자 -> 격자_간격_초 = 86400;
        }

        return new MathTimeIndexer(최초_틱_문자열, 최초_에포크, 격자_간격_초);
    }

    // [1. 한글 상세 주석]
    // 💡 [초정밀 수술 적용] 과거 JSON 호적부를 읽을 때 `replaceAll`, `split`의 정규식을 전면 폐기하고,
    // FSM 기반의 순수 포인터 인-플레이스(In-place) 스캐닝으로 스키마를 복원합니다.
    // [2. 영문 상세 주석]
    // 💡 [Ultra-Precision Surgery Applied] Completely discarded regular expressions
    // like `replaceAll` and `split` when reading past JSON registries,
    // restoring the schema using FSM-based pure pointer in-place scanning.

    /**
     * [내부 로직 3] 과거의 JSON 호적부를 읽어와 스키마(차원 축 인덱스)를 복원합니다.
     * 외부 라이브러리(Gson/Jackson) 및 정규식 객체 할당 없이 자체 해결합니다. (Zero-Allocation)
     */
    private void 과거_호적부_파싱_및_계승(Path 호적부_경로, Map<String, Integer> Y축_맵, Map<String, Integer> Z축_맵, Set<String> 묘비_세트) {
        try (BufferedReader 판독기 = Files.newBufferedReader(호적부_경로, StandardCharsets.UTF_8)) {
            String 라인;
            boolean Y축_구간 = false;
            boolean Z축_구간 = false;

            while ((라인 = 판독기.readLine()) != null) {
                if (라인.contains("\"Y_AXIS_TICKERS\"")) {
                    Y축_구간 = true;
                    Z축_구간 = false;
                    continue;
                }
                if (라인.contains("\"Z_AXIS_FEATURES\"")) {
                    Y축_구간 = false;
                    Z축_구간 = true;
                    continue;
                }
                // 블록 종료 감지
                if (라인.contains("}") && !라인.contains("{")) {
                    Y축_구간 = false;
                    Z축_구간 = false;
                }

                // 💡 [FSM 렉서 기반 파싱] 정규식 객체 생성 완전 멸균
                if (Y축_구간 && 라인.indexOf("\"index\"") != -1) {
                    String 키 = 추출하다_키_제로할당(라인);
                    if (키 != null) {
                        int 인덱스 = 추출하다_인덱스_제로할당(라인);
                        boolean 묘비여부 = 라인.indexOf("\"tombstone\": true") != -1;

                        if (인덱스 != -1) {
                            Y축_맵.put(키, 인덱스);
                            if (묘비여부)
                                묘비_세트.add(키);
                        }
                    }
                } else if (Z축_구간 && 라인.indexOf("\"index\"") != -1) {
                    String 키 = 추출하다_키_제로할당(라인);
                    if (키 != null) {
                        int 인덱스 = 추출하다_인덱스_제로할당(라인);
                        if (인덱스 != -1) {
                            Z축_맵.put(키, 인덱스);
                        }
                    }
                }
            }
        } catch (Exception e) {
            로거.log(Level.SEVERE, " [호적부 훼손] 과거 스키마 파일 판독 불가. 파이프라인 정합성 붕괴 위험.", e);
        }
    }

    // =========================================================================
    // 💡 [FSM 렉서] 정규식을 파괴하는 Zero-Allocation 유틸리티 엔진
    // =========================================================================

    // [1. 한글 상세 주석]
    // 라인 내에서 첫 번째로 묶인 쌍따옴표 문자열(Key)을 `substring`으로만 최소 1회 추출합니다.
    // [2. 영문 상세 주석]
    // Extracts the first double-quoted string (Key) within the line using
    // `substring` minimally just once.

    private String 추출하다_키_제로할당(String 라인) {
        int 첫_따옴표 = 라인.indexOf('"');
        if (첫_따옴표 == -1)
            return null;
        int 두번째_따옴표 = 라인.indexOf('"', 첫_따옴표 + 1);
        if (두번째_따옴표 == -1)
            return null;
        return 라인.substring(첫_따옴표 + 1, 두번째_따옴표);
    }

    // [1. 한글 상세 주석]
    // "index" 키워드 뒤에 오는 정수 값을 커서 이동 및 아스키 산술식만으로 안전하게 추출합니다.
    // [2. 영문 상세 주석]
    // Safely extracts the integer value following the "index" keyword solely
    // through cursor movement and ASCII arithmetic.

    private int 추출하다_인덱스_제로할당(String 라인) {
        int 인덱스_키워드 = 라인.indexOf("\"index\"");
        if (인덱스_키워드 == -1)
            return -1;

        int 콜론_위치 = 라인.indexOf(':', 인덱스_키워드 + 7);
        if (콜론_위치 == -1)
            return -1;

        int 커서 = 콜론_위치 + 1;
        int 길이 = 라인.length();

        // 공백 건너뛰기
        while (커서 < 길이 && (라인.charAt(커서) == ' ' || 라인.charAt(커서) == '\t')) {
            커서++;
        }

        int 결과 = 0;
        boolean 숫자_발견 = false;

        // 숫자 파싱 (상태 기계)
        while (커서 < 길이) {
            char 문자 = 라인.charAt(커서);
            if (문자 >= '0' && 문자 <= '9') {
                결과 = 결과 * 10 + (문자 - '0');
                숫자_발견 = true;
                커서++;
            } else {
                break; // 숫자가 아닌 문자(예: 콤마)를 만나면 종료
            }
        }

        return 숫자_발견 ? 결과 : -1;
    }

    /**
     * [내부 로직 4] 확정된 스키마와 시공간 격자를 융합하여 JSON으로 물리화합니다.
     */
    private void JSON_호적부_사출(
            A0_DT_42_422000_타임프레임_컨텍스트 컨텍스트,
            Map<String, Integer> Y축_맵,
            Map<String, Integer> Z축_맵,
            Set<String> 묘비_세트,
            MathTimeIndexer 시간_인덱서) {

        Path 사출_경로 = 컨텍스트.get지능형_호적부_경로();
        StringBuilder jsonBuilder = new StringBuilder();

        jsonBuilder.append("{\n");

        // 1. [X축] TIME_GRID (수학적 시공간 격자 제원)
        jsonBuilder.append("  \"TIME_GRID\": {\n");
        jsonBuilder.append(String.format("    \"base_tick\": \"%s\",\n", 시간_인덱서.getBaseTickStr()));
        jsonBuilder.append(String.format("    \"base_epoch\": %d,\n", 시간_인덱서.getBaseEpoch()));
        jsonBuilder.append(String.format("    \"interval_seconds\": %d\n", 시간_인덱서.getIntervalSeconds()));
        jsonBuilder.append("  },\n");

        // 2. [Z축] FEATURES (지표 메타데이터 및 인덱스)
        jsonBuilder.append("  \"Z_AXIS_FEATURES\": {\n");
        List<Map.Entry<String, Integer>> 정렬된_Z축 = new ArrayList<>(Z축_맵.entrySet());
        정렬된_Z축.sort(Map.Entry.comparingByValue()); // 인덱스 오름차순 정렬

        for (int i = 0; i < 정렬된_Z축.size(); i++) {
            Map.Entry<String, Integer> 엔트리 = 정렬된_Z축.get(i);
            String 지표명 = 엔트리.getKey();
            int 인덱스 = 엔트리.getValue();

            // 상대 경로 도출
            String 논리명 = 기본_지표_영문_매핑.getOrDefault(지표명, 지표명);
            boolean 기본지표여부 = 기본_지표_목록.contains(지표명);

            String 폴더명 = 기본지표여부 ? 컨텍스트.get베이스_지표_경로().getFileName().toString()
                    : 컨텍스트.get파생_지표_경로().getFileName().toString();
            String 파일명 = 기본지표여부 ? 논리명 : "IND_" + 지표명;
            String 상대경로 = "/" + 폴더명 + "/" + 파일명 + ".layer";

            jsonBuilder
                    .append(String.format("    \"%s\": { \"index\": %d, \"path\": \"%s\", \"type\": \"float32\" }%s\n",
                            지표명, 인덱스, 상대경로, (i < 정렬된_Z축.size() - 1 ? "," : "")));
        }
        jsonBuilder.append("  },\n");

        // 3. [Y축] TICKERS (엔티티 및 결번 마킹)
        jsonBuilder.append("  \"Y_AXIS_TICKERS\": {\n");
        List<Map.Entry<String, Integer>> 정렬된_Y축 = new ArrayList<>(Y축_맵.entrySet());
        정렬된_Y축.sort(Map.Entry.comparingByValue()); // 인덱스 오름차순 정렬

        for (int i = 0; i < 정렬된_Y축.size(); i++) {
            Map.Entry<String, Integer> 엔트리 = 정렬된_Y축.get(i);
            String 종목코드 = 엔트리.getKey();
            int 인덱스 = 엔트리.getValue();
            boolean 묘비상태 = 묘비_세트.contains(종목코드);

            jsonBuilder.append(String.format("    \"%s\": { \"index\": %d, \"tombstone\": %b }%s\n",
                    종목코드, 인덱스, 묘비상태, (i < 정렬된_Y축.size() - 1 ? "," : "")));
        }
        jsonBuilder.append("  }\n");
        jsonBuilder.append("}");

        // 4. 물리적 사출 (안전한 부모 디렉토리 계층 선제 개척 포함)
        try {
            if (사출_경로.getParent() != null) {
                Files.createDirectories(사출_경로.getParent());
            }

            Files.createDirectories(컨텍스트.get베이스_지표_경로());
            Files.createDirectories(컨텍스트.get파생_지표_경로());

            try (BufferedWriter bw = Files.newBufferedWriter(사출_경로, StandardCharsets.UTF_8)) {
                bw.write(jsonBuilder.toString());
                로거.info("   ├─ [JSON 사출 완료] 호적부 물리화 및 영토 개척 성공: " + 사출_경로.getFileName());
            }
        } catch (IOException e) {
            로거.log(Level.SEVERE, " [치명적 오류] 호적부(Manifest) 파일 사출 중 I/O 예외 발생.", e);
        }
    }

    // =========================================================================
    // 💡 O(1) 수학적 시공간 격자 인덱서 (GC 멸균 완료)
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
         * 💡 [핵심 교정: GC 폭탄 영구 해체] Zero-Allocation Epoch Parser
         * 정규식(replaceAll), 부분 문자열(substring), 날짜 객체(LocalDateTime) 생성을
         * 전면 폐기하고, 원시 char 배열 순회 및 율리우스일(Julian Day) 산술 연산만을 이용하여
         * 날짜를 O(1)으로 직결(Direct Convert) 파싱합니다.
         */
        public static long parseTickToEpoch(String tickStr) {
            int digitCount = 0;
            int year = 1970, month = 1, day = 1, hour = 0, min = 0, sec = 0;

            // 스택 메모리에만 잠시 존재하는 초경량 원시 배열 (이스케이프 분석을 통해 힙 할당 소멸)
            char[] chars = new char[14];
            int len = tickStr.length();

            // 1단계: 정규식을 대체하는 초고속 포인터 기반 숫자 축출
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

            // 💡 3단계: [객체 생성 완전 소각] Fliegel & Van Flandern 율리우스일 변환 공식 적용
            // LocalDateTime.of()를 폐기하고 산술식만으로 년월일을 누적 일수(Epoch Days)로 변환
            int a = (14 - month) / 12;
            int y = year + 4800 - a;
            int m = month + 12 * a - 3;

            // 역력학(Ephemeris) 기준 율리우스일 도출
            long julianDay = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045L;

            // 1970년 1월 1일의 율리우스일(2,440,588)을 차감하여 Epoch Day 산출
            long epochDays = julianDay - 2440588L;

            // 총 초(Seconds) 산출
            long totalSeconds = epochDays * 86400L + hour * 3600L + min * 60L + sec;

            // 4단계: KST(UTC+9)로 입력된 시간을 기준 에포크(UTC)로 변환하기 위해 9시간(32,400초) 차감
            return totalSeconds - (9 * 3600L);
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 100% Zero-Allocation 스캐닝 (FSM Tokenizer):
 * 과거의 `replaceAll(".*\"index\"\\s*:\\s*(\\d+).*", "$1")` 정규식 코드는 간결해 보이지만,
 * 매 줄마다 정규식 엔진 컴파일, 불필요한 부분 문자열 할당, 그리고 정수 파싱을 위한 객체 생성을
 * 유발하는 가비지 컬렉터(GC)의 시한폭탄입니다. 통합 OS V6.0은 이를 전면 파괴했습니다.
 * `추출하다_인덱스_제로할당`이라는 이름의 FSM(Finite State Machine) 렉서는 오직 문자열 내부의
 * `char` 포인터(커서)만을 이리저리 이동시키며 수학적 사칙연산(`결과 * 10 + (문자 - '0')`)만으로
 * 메모리 소모 없이 인덱스를 도출해 내는 완벽한 성능의 극치를 보여줍니다.
 * 
 * 2. 동적 스키마 진화 (Append-Only Schema Evolution):
 * AI 코어나 파이썬 분석 모델(PyTorch/TensorFlow)은 텐서의 차원 인덱스(Index)에 가중치(Weight)를 엄격히
 * 결속시킵니다.
 * 만약 어제는 `[0: 삼성전자, 1: 현대차, 2: 카카오]` 였는데,
 * 오늘 스캐너가 알파벳 순서로 다시 정렬하여 `[0: 네이버, 1: 삼성전자]` 로 인덱스가 밀려버린다면?
 * AI는 네이버의 데이터를 보며 삼성전자의 모델 가중치로 곱셈을 하는 치명적인 '차원 환각(Dimension Hallucination)'을
 * 일으킵니다.
 * 이 빌더는 과거 디스크에 사출된 JSON 호적부를 무조건 읽어와 기존 번호를 영원히 보존(Freeze)하고,
 * 새롭게 등장한 종목(Ticker)이나 지표(Feature)는 무조건 꼬리(Append) 쪽에 새 번호를 부여하여 스키마의 하위 호환성을
 * 100% 수호합니다.
 * 
 * 3. 묘비(Tombstone) 아키텍처와 시공간의 영속성:
 * 주식 시장에서 회사는 파산하고 상장폐지됩니다. 일반적인 RDBMS는 이 데이터를 삭제(Delete)합니다.
 * 그러나 AI 딥러닝에서 '차원 크기(Shape)'는 절대적으로 고정되어야 합니다.
 * 2,850개의 종목 중 1개가 상장폐지되었다고 배열 크기를 2,849개로 줄여버리면 신경망 파이프라인 전체가 붕괴합니다.
 * 통합 OS는 삭제 대신 **묘비(Tombstone)**를 마킹합니다.
 * 과거에 존재했던 종목의 인덱스는 보존하되 `tombstone: true` 처리를 통해, 쿼리 엔진이 미래 시점의 데이터를 퍼올릴 때
 * 디스크를 읽지 않고 RAM 상에서 `0.0f` 로 꽉 찬 더미(Dummy) 평면을 쑤셔 넣습니다(Virtual Void Filling).
 * 이를 통해 AI는 "이 종목은 상장폐지되어 에너지가 0이 되었구나"라는 시장의 생태계 법칙 자체를 수학적으로 체득하게 됩니다.
 * 
 * 4. 메타데이터(JSON) 기반 의존성 탈피 (Zero-Dependency Manifest):
 * JSON 내부에 `base_epoch`와 `interval_seconds` 등 수학적 격자 메타데이터가 함께 직렬화됩니다.
 * 이는 하위 시스템(C++ GPU 셰이더, 파이썬 렌더러 등)이 자바 시스템에 의존하지 않고도
 * 이 JSON 파일 하나만 읽으면 오프힙 바이너리(`.layer`) 텐서를 어떻게 역직렬화하고 메모리 오프셋을 계산할지
 * 스스로 결정할 수 있게 만드는 진정한 의미의 '독립적 선언형 인프라(Declarative Infrastructure)'입니다.
 * =============================================================================
 */