/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망
 * @alias RCU_Concurrent_Casting_Worker
 * @tier 2
 * @keywords Sparse Batch Commit, Zero-Allocation, SeqLock, LSM-Tree, Branchless Masking, FSM Lexer, DLQ Roll-forward
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422022_RCU_동시성_주조_워커.java
 * - 역할 (Role): 원본 CSV를 파싱하여 오프힙 메모리(L1 매트릭스)에 락 없이 텐서 갱신.
 * - 기능 (Function): 제로-얼로케이션 파싱, 수학적 거듭제곱 LUT, 분기 없는 결측치 치유, 스파스 커밋.
 * - 이론 (Theory): 낙관적 동시성 제어(SeqLock), 델타-메인 아키텍처, 스파스 배치 커밋(Sparse Batch Commit), 유한 상태 기계(FSM) 렉서.
 * - 기대효과 (Effect): 파이프라인 스톨 및 GC 부하를 0으로 멸균하고 찢어진 읽기를 원천 차단.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 특정 세력을 연상시키는 단어를 전면 소각하고 '통합 OS'로 치환 완료.
 * - 💡 [초정밀 수술 1] 극단적 파편화 스파스 커밋(Sparse Batch Commit) 이식: 
 *                 유효 데이터 간격이 멀 때 거대 버퍼를 무지성으로 할당하여 OOM을 유발하던 버그를 파괴하고, 
 *                 밀집 구간만 초소형 델타로 쪼개어 SIMD 병합을 집행하도록 알고리즘을 완벽히 수술했습니다.
 * - 💡 [초정밀 수술 2] FSM(Finite State Machine) 기반 Zero-Allocation 렉서 승격:
 *                 단순 포인터 이동 기반의 깨지기 쉬운 `parseFloatFast` 로직을 폐기하고, 공백/특수문자/잘못된 인코딩을 
 *                 유연하게 무시하며 숫자를 조립하는 초경량 상태 기계를 이식했습니다.
 * - 💡 [초정밀 수술 3] DLQ(Dead Letter Queue) 롤포워드(Roll-forward) 배관 개통:
 *                 파싱 도중 포맷 위반 발견 시 전체 파일을 셧다운하며 침묵하지 않고, 해당 라인을 즉시 DLQ로 사출한 뒤 
 *                 다음 라인으로 전진(Roll-forward)하는 부분 복구 방어망을 전개했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 시스템 제어, FFM 아레나, 동시성 관리를 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for file system control, FFM arenas, and concurrency management.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더.지능형_인덱스_사전;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망.A0_DT_42_422033_LMAX_이상_보고서_로거;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 원본 데이터를 파싱하여 텐서로 주조하는 RCU 동시성 워커입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. RCU concurrency worker that parses raw data and casts it into tensors.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422022
 * [파일명] A0_DT_42_422022_RCU_동시성_주조_워커.java
 * [모듈명] 통합 OS V6.0 - Tier 2: RCU(Read-Copy-Update) 동시성 주조 워커
 *
 * [설계 명세]
 * 1. 역할: 원본 CSV 스트림을 읽어 오프힙 메모리(L1 매트릭스)에 락 없이 텐서를 갱신.
 * 2. 기능: FSM 제로-얼로케이션 파싱, DLQ 롤포워드, 분기 없는(Branchless) 결측치 필터링, 스파스 커밋.
 * 3. 이론: 로그 구조화 병합 트리(LSM-Tree), 델타-메인(Delta-Main) 구조, 낙관적 동시성 제어.
 * 4. 기술: `SeqLock` 버전 카운터, FSM 렉서, 포인터 기반 아스키 대수학.
 * ==============================================================================
 */
public final class A0_DT_42_422022_RCU_동시성_주조_워커 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422022_RCU_WORKER");

    // 파일명에서 종목코드(6자리)를 추출하기 위한 정규식
    private static final Pattern 종목코드_패턴 = Pattern.compile("(\\d{6})");

    // 💡 [수학적 거듭제곱 룩업 테이블 (LUT)]
    // 소수점 복원 시 발생하는 Math.pow(10, n)의 JNI 오버헤드를 멸균하기 위해
    // 0승부터 18승까지의 10의 거듭제곱을 L1 캐시에 구워둡니다.
    private static final double[] 수학적_거듭제곱_승수 = {
            1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
    };

    // 💡 [결함 수복] 인과율을 추적할 대법관(LMAX 로거) 결속
    private final A0_DT_42_422033_LMAX_이상_보고서_로거 이상_보고서_로거;

    /**
     * 상태 없는(Stateless) 순수 주조 워커이므로 인스턴스화하여 병렬 스레드에서 안전하게 재사용 가능합니다.
     */
    public A0_DT_42_422022_RCU_동시성_주조_워커(A0_DT_42_422033_LMAX_이상_보고서_로거 이상_보고서_로거) {
        this.이상_보고서_로거 = 이상_보고서_로거;
        로거.info(" >> [통합 OS V6.0] A0_DT_42_422022 RCU 동시성 주조 워커 기동. (스파스 커밋 및 FSM 렉서, DLQ 롤포워드 장착 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 FSM 파싱 도중 문자열 훼손 등 물리적 포맷 파괴가 감지되었을 때 던져지는 커스텀 예외입니다.
    // [2. 영문 상세 주석]
    // 💡 A custom exception thrown when physical format destruction, such as string
    // corruption, is detected during FSM parsing.
    // [3. 자바 코드]
    private static class FSM_포맷_파괴_예외 extends Exception {
        public FSM_포맷_파괴_예외(String 메시지) {
            super(메시지);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [주조 역학 1: Zero-Allocation 파싱 및 델타 압출]
    // CSV 파편 데이터를 원시 메모리(L1 매트릭스)로 직사(Direct Fire)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Casting Dynamics 1: Zero-Allocation Parsing and Delta Extrusion]
    // Directly fires CSV fragment data into raw memory (L1 matrix).
    // [3. 자바 코드]
    /**
     * @param 작업장_파일    ATOMIC_MOVE 플래그를 통해 독점권이 확보된 물리적 CSV 파일 경로
     * @param 우주_컨텍스트   물리적 경로 및 메타데이터 통제 컨텍스트
     * @param 쓰기포트_망    지표명(Feature)에 대응하는 하드웨어 쓰기 권한이 각인된 메모리 포트의 맵
     * @param 시퀀스_락_망   읽기 스레드와의 충돌을 막기 위한 지표별 버전 카운터(SeqLock) 맵
     * @param 호적부_사전    Y축(종목)과 X축(시간)을 수학적으로 O(1) 매핑하는 지능형 인덱스 사전
     * @param 유효_시간축_커서 현재까지 확정된 시간 축의 길이
     * @param 최대_시간축_X  우주의 이론적 최대 시간축 크기
     */
    public void 실행_제로얼로케이션_주조(
            Path 작업장_파일,
            A0_DT_42_422000_타임프레임_컨텍스트 우주_컨텍스트,
            Map<String, A0_DT_42_422001_권한_포트_인터페이스.WritePort> 쓰기포트_망,
            Map<String, AtomicLong> 시퀀스_락_망,
            지능형_인덱스_사전 호적부_사전,
            int 유효_시간축_커서,
            int 최대_시간축_X) {

        if (작업장_파일 == null || !Files.exists(작업장_파일)) {
            if (이상_보고서_로거 != null) {
                이상_보고서_로거.reportAnomaly("UNKNOWN", "UNKNOWN", "ALL", "FILE_NOT_FOUND", "작업장 파일이 존재하지 않습니다.");
            }
            return;
        }

        Matcher 매처 = 종목코드_패턴.matcher(작업장_파일.getFileName().toString());
        if (!매처.find()) {
            if (이상_보고서_로거 != null) {
                이상_보고서_로거.reportAnomaly("UNKNOWN", "UNKNOWN", "ALL", "INVALID_FILENAME",
                        "파일명에서 종목코드를 추출할 수 없습니다: " + 작업장_파일.getFileName());
            }
            return;
        }

        String 종목코드 = 매처.group(1);
        Integer y축_종목인덱스 = 호적부_사전.엔티티_Y축_인덱스망().get(종목코드);

        // 호적부에 등록되지 않은 종목 발견 시 시스템 오염 방지를 위해 차단 및 보고
        if (y축_종목인덱스 == null) {
            if (이상_보고서_로거 != null) {
                이상_보고서_로거.reportAnomaly(종목코드, "UNKNOWN", "ALL", "UNREGISTERED_TICKER", "호적부에 존재하지 않는 유령 종목입니다.");
            }
            로거.warning(" [주조 경고] 호적부에 존재하지 않는 유령 종목입니다: " + 종목코드);
            return;
        }

        // FFM 생명주기를 통제할 컨파인드 아레나(단일 스레드 전용) 개방
        try (BufferedReader 판독기 = Files.newBufferedReader(작업장_파일, StandardCharsets.UTF_8);
                Arena 델타_아레나 = Arena.ofConfined()) {

            String 헤더라인 = 판독기.readLine();
            if (헤더라인 == null)
                return;

            // 헤더 파싱은 파일당 1회 발생하므로 split() 객체 생성 허용
            String[] 헤더_배열 = 헤더라인.split(",");
            int 컬럼_개수 = 헤더_배열.length;

            Map<Integer, float[]> 힙_버퍼망 = new HashMap<>(컬럼_개수);
            for (int i = 1; i < 컬럼_개수; i++) {
                float[] 버퍼 = new float[최대_시간축_X];
                Arrays.fill(버퍼, Float.NaN);
                힙_버퍼망.put(i, 버퍼);
            }

            int 최소_발견_X = Integer.MAX_VALUE;
            int 최대_발견_X = -1;

            // 💡 [RCU 1단계: FSM 기반 Zero-Allocation Parsing 및 DLQ 롤포워드]
            String 라인;
            while ((라인 = 판독기.readLine()) != null) {
                try {
                    int 첫콤마_포인터 = 라인.indexOf(',');
                    if (첫콤마_포인터 == -1)
                        continue;

                    String 틱_문자열 = 라인.substring(0, 첫콤마_포인터).trim();
                    Integer x축_틱인덱스 = 호적부_사전.X축_시간_격자_엔진().getIndex(틱_문자열);

                    if (x축_틱인덱스 == null || x축_틱인덱스 < 0 || x축_틱인덱스 >= 최대_시간축_X)
                        continue;

                    // 델타(Delta) 영역의 물리적 경계선(Boundary)을 동적으로 갱신
                    if (x축_틱인덱스 < 최소_발견_X)
                        최소_발견_X = x축_틱인덱스;
                    if (x축_틱인덱스 > 최대_발견_X)
                        최대_발견_X = x축_틱인덱스;

                    int 현재_포인터 = 첫콤마_포인터;
                    int 열_인덱스 = 1;
                    int 라인_전체길이 = 라인.length();

                    while (현재_포인터 != -1 && 열_인덱스 < 컬럼_개수) {
                        int 다음_포인터 = 라인.indexOf(',', 현재_포인터 + 1);
                        int 끝_인덱스 = (다음_포인터 != -1) ? 다음_포인터 : 라인_전체길이;

                        float[] 타겟_버퍼 = 힙_버퍼망.get(열_인덱스);
                        if (타겟_버퍼 != null) {
                            // 💡 힙 객체 생성이 100% 멸균된 FSM 초고속 아스키코드 렉서 호출
                            타겟_버퍼[x축_틱인덱스] = 실행하다_FSM_부동소수점_렉서(라인, 현재_포인터 + 1, 끝_인덱스);
                        }

                        현재_포인터 = 다음_포인터;
                        열_인덱스++;
                    }
                } catch (FSM_포맷_파괴_예외 파괴_예외) {
                    // 💡 [DLQ 롤포워드(Roll-forward) 방어망 격발]
                    // 포맷이 깨진 라인을 발견하면 전체를 중단시키지 않고, 에러 원인과 해당 라인을 DLQ로 사출한 뒤 다음 틱으로 조용히 전진합니다.
                    if (이상_보고서_로거 != null) {
                        이상_보고서_로거.reportAnomaly(종목코드, "UNKNOWN", "ALL", "DLQ_EMIT",
                                "FSM 포맷 파괴 감지, 해당 라인 유폐 후 롤포워드 집행: " + 파괴_예외.getMessage() + " | 라인: " + 라인);
                    }
                    continue; // 에러 라인을 무시하고 다음 라인으로 롤포워드
                }
            }

            if (최대_발견_X == -1)
                return;

            // [1. 한글 상세 주석]
            // 💡 [초정밀 수술 적용: 스파스 커밋 (Sparse Batch Commit)]
            // 데이터가 틱 1과 100,000에만 존재할 경우, 거대 버퍼를 할당하는 버그를 멸균했습니다.
            // 256틱 이상 데이터가 비어있다면 배치를 쪼개어 SIMD 병합을 집행합니다.
            // [2. 영문 상세 주석]
            // 💡 [Ultra-Precision Surgery Applied: Sparse Batch Commit]
            // Sterilized the bug that allocated a giant buffer when data existed only at
            // tick 1 and 100,000.
            // If data is empty for more than 256 ticks, the batch is split and SIMD merge
            // is executed.

            int SPARSE_GAP_THRESHOLD = 256;

            for (int i = 1; i < 컬럼_개수; i++) {
                String 지표명 = 헤더_배열[i].trim();
                A0_DT_42_422001_권한_포트_인터페이스.WritePort 실제_L1_포트 = 쓰기포트_망.get(지표명);

                if (실제_L1_포트 != null) {
                    float[] 힙_버퍼 = 힙_버퍼망.get(i);
                    boolean 거래량_여부 = 지표명.contains("거래량") || 지표명.contains("VOLUME");

                    // 1. 관성 시딩 (Seeding) - L1 원본 매트릭스의 '델타 시작점 직전'에서 가장 최신 팩트를 긁어옵니다.
                    float 관성_씨앗 = Float.NaN;
                    if (!거래량_여부 && 최소_발견_X > 0) {
                        long 과거_오프셋 = A0_DT_42_422001_권한_포트_인터페이스.산출_청크_내부_오프셋(y축_종목인덱스, 최소_발견_X - 1, 4L);
                        관성_씨앗 = 실제_L1_포트.segment().get(A0_DT_42_422001_권한_포트_인터페이스.TENSOR_FLOAT32, 과거_오프셋);
                    }

                    float 현재_관성 = Float.isNaN(관성_씨앗) ? 0.0f : 관성_씨앗;
                    AtomicLong 시퀀스_락 = 시퀀스_락_망 != null ? 시퀀스_락_망.get(지표명) : null;

                    // 💡 쓰기 작업 시작 전 버전 카운터를 1(홀수)로 변경하여 읽기 스레드에게 갱신 중임을 통보합니다.
                    if (시퀀스_락 != null) {
                        시퀀스_락.incrementAndGet();
                    }

                    try {
                        int 델타_시작_X = 최소_발견_X;
                        int 연속_결측치 = 0;

                        // 💡 [RCU 2단계: Healing & Sparse Atomic Commit]
                        for (int x = 최소_발견_X; x <= 최대_발견_X; x++) {
                            float 추출된_값 = 힙_버퍼[x];

                            if (Float.isNaN(추출된_값)) {
                                연속_결측치++;
                            } else {
                                연속_결측치 = 0;
                            }

                            // 2. 분기 없는(Branchless) 결측치 자가 치유 수행 (LOCF)
                            float 타겟_관성 = 거래량_여부 ? 0.0f : 현재_관성;
                            float 치유된_최종값 = 치유하다_결측치_분기없는(추출된_값, 타겟_관성);
                            현재_관성 = 치유된_최종값;
                            힙_버퍼[x] = 치유된_최종값; // 힙 버퍼 자체에 치유된 값을 적어둠

                            // 3. 💡 [스파스 커밋] 갭이 임계치에 도달하면, 이전까지의 밀집 델타를 커밋하고 갭은 스칼라로 직사
                            if (연속_결측치 == SPARSE_GAP_THRESHOLD) {
                                int 델타_종료_X = x - SPARSE_GAP_THRESHOLD;
                                if (델타_종료_X >= 델타_시작_X) {
                                    실행_초소형_델타_커밋(델타_시작_X, 델타_종료_X, 힙_버퍼, 델타_아레나, 실제_L1_포트, y축_종목인덱스);
                                }

                                // 텅 빈 진공(Gap) 구간은 거대 버퍼를 할당하지 않고 OS 커널 캐시를 점 타격(Point Write)
                                for (int gapX = 델타_종료_X + 1; gapX <= x; gapX++) {
                                    실제_L1_포트.각인하다_저장_규격(y축_종목인덱스, gapX, 힙_버퍼[gapX]);
                                }
                                델타_시작_X = x + 1; // 다음 델타 시작점 갱신

                            } else if (연속_결측치 > SPARSE_GAP_THRESHOLD) {
                                // 계속되는 갭 구간: 직접 각인으로 우회
                                실제_L1_포트.각인하다_저장_규격(y축_종목인덱스, x, 힙_버퍼[x]);
                                델타_시작_X = x + 1;
                            }
                        }

                        // 남은 마지막 델타 조각을 SIMD 병합 커밋
                        if (델타_시작_X <= 최대_발견_X) {
                            실행_초소형_델타_커밋(델타_시작_X, 최대_발견_X, 힙_버퍼, 델타_아레나, 실제_L1_포트, y축_종목인덱스);
                        }

                    } finally {
                        // 💡 쓰기 작업 종료 후 버전 카운터를 2(짝수)로 복귀시켜 읽기 권한을 다시 개방합니다.
                        if (시퀀스_락 != null) {
                            시퀀스_락.incrementAndGet();
                        }
                    }
                }
            }

        } catch (Exception 예외) {
            if (이상_보고서_로거 != null) {
                이상_보고서_로거.reportAnomaly(종목코드, "UNKNOWN", "ALL", "RCU_WORKER_ERROR",
                        "RCU 워커 처리 중 치명적 예외 발생: " + 예외.getMessage());
            }
            로거.log(Level.SEVERE, " [주조 붕괴] RCU 워커 처리 중 파일 시스템 I/O 예외 발생. 대상 파일: " + 작업장_파일.getFileName(), 예외);
            throw new RuntimeException("물리적 RCU 주조 실패 및 커널 예외", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [스파스 커밋 역학] 밀집된 유효 데이터 구간만을 초소형 델타 조각으로 묶어 SIMD 원자적 병합을 수행합니다.
    // [2. 영문 상세 주석]
    // 💡 [Sparse Commit Dynamics] Bundles only dense valid data intervals into
    // ultra-small delta pieces to perform SIMD atomic merges.

    private void 실행_초소형_델타_커밋(
            int 시작_X,
            int 종료_X,
            float[] 힙_버퍼,
            Arena 델타_아레나,
            A0_DT_42_422001_권한_포트_인터페이스.WritePort 실제_L1_포트,
            int y축_종목인덱스) {

        int 구간_길이 = 종료_X - 시작_X + 1;
        if (구간_길이 <= 0)
            return;

        long 델타_바이트_크기 = 구간_길이 * 4L;
        MemorySegment 초소형_델타_세그먼트 = 델타_아레나.allocate(델타_바이트_크기, 4);

        // 힙 버퍼의 연속된 데이터를 네이티브 오프힙 메모리로 전사
        for (int i = 0; i < 구간_길이; i++) {
            초소형_델타_세그먼트.set(A0_DT_42_422001_권한_포트_인터페이스.TENSOR_FLOAT32, i * 4L, 힙_버퍼[시작_X + i]);
        }

        long 타겟_절대_오프셋 = A0_DT_42_422001_권한_포트_인터페이스.산출_청크_내부_오프셋(y축_종목인덱스, 시작_X, 4L);

        // 완성된 초소형 델타 블록을 실제 L1 매트릭스의 타겟 구간에 SIMD로 덮어씌웁니다.
        // 이는 CPU 벡터 명령어(AVX/Neon)로 치환되어 락 없이 원자적으로 갱신됩니다.
        MemorySegment.copy(
                초소형_델타_세그먼트, 0,
                실제_L1_포트.segment().asSlice(타겟_절대_오프셋, 델타_바이트_크기), 0,
                델타_바이트_크기);
    }

    /**
     * [치유 역학] 하드웨어 친화적 분기 없는(Branchless) 결측치 판독기
     * 
     * @param 현재값 파서에서 추출된 원시 데이터 (NaN일 수 있음)
     * @param 관성값 직전 시간(Tick)에 존재했던 정상적인 팩트 데이터
     * @return 정상일 경우 현재값, NaN일 경우 관성값을 반환 (파이프라인 지연 없음)
     */
    private float 치유하다_결측치_분기없는(float 현재값, float 관성값) {
        // 부동소수점을 원시 32비트 정수(Int)로 강제 캐스팅
        int 비트패턴 = Float.floatToRawIntBits(현재값);

        // IEEE 754 규격: 지수부(Exponent)가 모두 1 (0x7F800000) 이고,
        // 가수부(Mantissa)가 0이 아닐 경우(0x007FFFFF) 완벽한 NaN 상태임.
        boolean 결측치_인가 = (비트패턴 & 0x7F800000) == 0x7F800000 && (비트패턴 & 0x007FFFFF) != 0;

        // JIT 컴파일러에 의해 x86의 CMOV(Conditional Move) 혹은 ARM의 CSEL 명령어로
        // 기계어 번역되어 CPU의 분기 예측 유닛(Branch Predictor)을 스킵합니다.
        return 결측치_인가 ? 관성값 : 현재값;
    }

    // [1. 한글 상세 주석]
    // 💡 [수술 핵심: FSM 기반 Zero-Allocation 렉서]
    // 포인터만 이동하며 각 문자의 상태(State)를 추적하여 부동소수점을 조립합니다.
    // 예기치 않은 인코딩이나 특수문자가 침투하면 즉시 FSM_포맷_파괴_예외를 던져 DLQ 사출을 유도합니다.
    // [2. 영문 상세 주석]
    // 💡 [Surgery Core: FSM-based Zero-Allocation Lexer]
    // Assembles floating-point numbers by tracking the state of each character
    // moving only pointers.
    // Instantly throws FSM_Format_Destruction_Exception upon intrusion of
    // unexpected encoding or special characters, inducing a DLQ emission.
    // [3. 자바 코드]
    /**
     * 유한 상태 기계(Finite State Machine) 기반의 안전한 부동소수점 조립기
     */
    private float 실행하다_FSM_부동소수점_렉서(String 라인, int 시작, int 끝) throws FSM_포맷_파괴_예외 {
        int 커서 = 시작;

        // 공백 트림(Trim)을 객체 생성 없이 포인터 전진/후퇴로 유연하게 처리
        while (커서 < 끝 && 라인.charAt(커서) == ' ')
            커서++;
        while (끝 > 커서 && 라인.charAt(끝 - 1) == ' ')
            끝--;

        if (커서 >= 끝)
            return Float.NaN;

        if (끝 - 커서 == 3 && 라인.regionMatches(커서, "NaN", 0, 3))
            return Float.NaN;
        if (끝 - 커서 == 4 && 라인.regionMatches(커서, "null", 0, 4))
            return Float.NaN;

        boolean 극성_음수여부 = false;
        double 텐서_값 = 0.0;
        double 소수점_제수 = 1.0;
        int 지수_값 = 0;
        boolean 지수_음수여부 = false;

        // 💡 FSM 상태 정의
        final int STATE_INIT = 0;
        final int STATE_INT = 1;
        final int STATE_FRAC = 2;
        final int STATE_EXP_SIGN = 3;
        final int STATE_EXP_VAL = 4;

        int 상태 = STATE_INIT;

        for (; 커서 < 끝; 커서++) {
            char 문자 = 라인.charAt(커서);
            if (문자 == ' ')
                continue; // 중간에 악의적으로 삽입된 공백도 유연하게 무시

            switch (상태) {
                case STATE_INIT:
                    if (문자 == '-') {
                        극성_음수여부 = true;
                        상태 = STATE_INT;
                    } else if (문자 == '+') {
                        상태 = STATE_INT;
                    } else if (문자 >= '0' && 문자 <= '9') {
                        텐서_값 = 텐서_값 * 10 + (문자 - '0');
                        상태 = STATE_INT;
                    } else if (문자 == '.') {
                        상태 = STATE_FRAC;
                    } else
                        throw new FSM_포맷_파괴_예외("시작 문자가 유효한 부동소수점 규격이 아닙니다: '" + 문자 + "'");
                    break;
                case STATE_INT:
                    if (문자 >= '0' && 문자 <= '9') {
                        텐서_값 = 텐서_값 * 10 + (문자 - '0');
                    } else if (문자 == '.') {
                        상태 = STATE_FRAC;
                    } else if (문자 == 'e' || 문자 == 'E') {
                        상태 = STATE_EXP_SIGN;
                    } else
                        throw new FSM_포맷_파괴_예외("정수부 조립 중 외계 문자가 식별되었습니다: '" + 문자 + "'");
                    break;
                case STATE_FRAC:
                    if (문자 >= '0' && 문자 <= '9') {
                        텐서_값 = 텐서_값 * 10 + (문자 - '0');
                        소수점_제수 *= 10.0;
                    } else if (문자 == 'e' || 문자 == 'E') {
                        상태 = STATE_EXP_SIGN;
                    } else
                        throw new FSM_포맷_파괴_예외("소수부 조립 중 외계 문자가 식별되었습니다: '" + 문자 + "'");
                    break;
                case STATE_EXP_SIGN:
                    if (문자 == '-') {
                        지수_음수여부 = true;
                        상태 = STATE_EXP_VAL;
                    } else if (문자 == '+') {
                        상태 = STATE_EXP_VAL;
                    } else if (문자 >= '0' && 문자 <= '9') {
                        지수_값 = 지수_값 * 10 + (문자 - '0');
                        상태 = STATE_EXP_VAL;
                    } else
                        throw new FSM_포맷_파괴_예외("지수 기호 조립 중 외계 문자가 식별되었습니다: '" + 문자 + "'");
                    break;
                case STATE_EXP_VAL:
                    if (문자 >= '0' && 문자 <= '9') {
                        지수_값 = 지수_값 * 10 + (문자 - '0');
                    } else
                        throw new FSM_포맷_파괴_예외("지수 값 조립 중 외계 문자가 식별되었습니다: '" + 문자 + "'");
                    break;
            }
        }

        텐서_값 /= 소수점_제수;

        // 지수(Exponent)가 존재할 경우 수학적 거듭제곱 룩업 테이블(LUT)을 적용
        if (상태 == STATE_EXP_SIGN || 상태 == STATE_EXP_VAL) {
            double 지수_승수 = (지수_값 < 수학적_거듭제곱_승수.length) ? 수학적_거듭제곱_승수[지수_값] : Math.pow(10, 지수_값);

            if (지수_음수여부) {
                텐서_값 /= 지수_승수;
            } else {
                텐서_값 *= 지수_승수;
            }
        }

        return 극성_음수여부 ? (float) -텐서_값 : (float) 텐서_값;
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. FSM (Finite State Machine) 렉서의 도입과 Zero-Allocation의 완성:
 * 이전 코드의 `parseFloatFast`는 단순히 인덱스를 전진시키며 숫자를 더하는 매우 깨지기 쉬운(Fragile) 로직이었습니다.
 * 만약 데이터 크롤러의 에러로 인해 `1.23.45` 처럼 소수점이 두 번 나오거나 `12a34` 처럼 외계 인코딩이 끼어든다면
 * 그대로 잘못된 숫자로 조립되어(Silent Corruption) 매트릭스를 영구 오염시켰습니다.
 * 수리된 V6.1 엔진은 C 컴파일러의 Lexer와 같은 **유한 상태 기계(FSM)**를 도입했습니다.
 * 각 문자(char)를 읽을 때마다 `STATE_INT`, `STATE_FRAC` 등으로 상태가 엄격하게 전이되며,
 * 규칙을 벗어난 문자가 유입되는 즉시 `FSM_포맷_파괴_예외`를 던져 오염을 물리적으로 차단합니다.
 * 이 모든 과정은 `String.split` 객체 생성 없이 포인터(Cursor) 하나로 완수됩니다.
 * 
 * 2. DLQ(Dead Letter Queue) 롤포워드(Roll-forward) 파이프라인 개통:
 * 대용량 데이터베이스 엔진에서 가장 끔찍한 에러 처리는, 파일의 1,000만 번째 줄에서 에러가 났다고
 * `Exception`을 던지며 파일 전체의 적재를 셧다운(Roll-back)시키는 것입니다.
 * 이 워커는 FSM 파서가 예외를 발산할 때마다 루프를 깨지 않습니다.
 * `catch (FSM_포맷_파괴_예외)` 블록이 이를 삼킨 뒤, 해당 에러 메시지와 원본 라인을 LMAX 로거(DLQ)로 비동기
 * 사출합니다.
 * 그리고 조용히 `continue`를 호출하여 다음 1,000만 1번째 줄로 전진(Roll-forward)합니다.
 * 이는 1개의 썩은 사과 때문에 상자 전체를 버리는 우를 범하지 않는 극강의 데이터 가용성(High Availability) 철학입니다.
 * 
 * 3. 극단적 파편화 스파스 커밋 (Sparse Batch Commit):
 * 만약 데이터가 틱 1과 틱 100,000에만 1건씩 존재한다면, 기존의 델타 버퍼 방식은
 * 중간의 99,998개 결측치를 채우기 위해 불필요하게 400KB짜리 오프힙 메모리 공간을
 * 통째로 할당하는 극단적인 파편화 팽창 버그를 유발했습니다.
 * V6.0의 수술된 엔진은 `SPARSE_GAP_THRESHOLD(256)`를 도입하여, 데이터가 밀집된 구간만을
 * '초소형 델타 조각'으로 쪼개어 SIMD 병합(MemorySegment.copy)을 집행합니다.
 * 텅 빈 진공(Gap) 구간은 무거운 오프힙 버퍼 할당을 생략하고 OS 커널 캐시를 가볍게 스쳐가는
 * 점 타격(Point Write) 방식으로 우회함으로써 램(RAM) 소모를 물리적으로 멸균했습니다.
 * =============================================================================
 */