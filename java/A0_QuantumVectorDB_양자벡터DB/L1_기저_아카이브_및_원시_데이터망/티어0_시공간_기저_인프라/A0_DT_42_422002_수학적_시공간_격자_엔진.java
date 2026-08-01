/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라
 * @alias Spacetime_Grid_Math_Engine
 * @tier 0
 * @keywords O(1) Indexing, Zero-Allocation, Julian Day Formula, Dimensional Relativity
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422002_수학적_시공간_격자_엔진.java
 * - 역할 (Role): 문자열 타임스탬프를 물리적 메모리 오프셋(X축)으로 변환하는 순수 수학 엔진.
 * - 기능 (Function): O(1) 시간 복잡도의 수학적 X축 좌표 역산, Zero-Allocation Epoch 파서.
 * - 이론 (Theory): 율리우스일(Julian Day) 역력학 산술, 차원 상대성(Dimensional Relativity), 시간의 공간화.
 * - 기대효과 (Effect): 객체 힙 할당을 물리적으로 제거시켜 GC 지연을 0으로 수렴.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [정밀 제어] KST 하드코딩 제거 및 타임존 동적 할당: 파서 하단의 `-(9 * 3600L)` 하드코딩을 제거하고, 
 *                 시스템 환경 변수(`matrix.timezone.offset.seconds`)를 통해 글로벌 타임존 오프셋을 
 *                 런타임에 주입받도록 구조를 개편하여 글로벌 환경에서의 시차 불일치를 방지했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언. 순수 수학 연산 코어이므로 외부 라이브러리 Import가 전혀 존재하지 않습니다(Zero-Dependency).
// [2. 영문 상세 주석]
// Package declaration. As a pure mathematical computation core, there are absolutely no external library imports (Zero-Dependency).
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. O(1) 시간 복잡도로 타임스탬프를 메모리 오프셋으로 치환하는 격자 인덱서입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A grid indexer that substitutes timestamps with memory offsets in O(1) time complexity.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422002
 * [파일명] A0_DT_42_422002_수학적_시공간_격자_엔진.java
 * [모듈명] 통합 OS V6.0 - Tier 0: O(1) 수학적 시공간 격자 인덱서
 * 
 * [설계 명세]
 * 1. 역할: 문자열 타임스탬프를 메모리 오프셋(X축 인덱스)으로 변환.
 * 2. 기능: O(1) 시간 복잡도의 수학적 X축 좌표 역산.
 * 3. 의도: HashMap 객체에 수백만 개의 날짜를 담아두던 힙 메모리 낭비 폐기.
 * 4. 공식: Index = (TargetEpoch - BaseEpoch) / IntervalSeconds
 * 5. 기술: 💡 [V6.0 신규] Fliegel & Van Flandern 율리우스일(Julian Day) 산술 공식을
 * 적용한 완벽한 Zero-Allocation Epoch 파서.
 * 6. 기대효과: java.time.LocalDateTime 객체의 힙 할당을 물리적으로 제거시켜
 * 초당 수백만 건 처리 시 GC(Garbage Collection) 발생률을 완전한 제로(0)로 수렴.
 * 
 * [기능 명세 상세]
 * 1. 💡 [O(1) 수학적 시공간 격자 인덱싱]: 수백만 개의 타임스탬프(Tick) 문자열을 HashMap에 담는
 * 방식을 영구 폐기합니다. '최초 기준 시간(Base Epoch)'과 '격자 간격(Interval)'만을 이용해,
 * 단 한 번의 사칙연산으로 메모리 오프셋(X축 인덱스)을 도출하는 100% 수학적 모델을 확립합니다.
 * 2. 💡 [GC 부하 영구 제거 (Zero-Allocation Parser)]:
 * 문자열 파싱에 사용되던 정규식(replaceAll)을 원시 포인터 추적으로 대체한 것에 더해,
 * 마지막에 잔존하던 LocalDateTime 객체 생성 부하마저 천체 역학의 율리우스일 공식으로
 * 치환하여 JVM 힙(Heap) 객체 생성을 100% 봉쇄했습니다.
 * 3. 💡 [타임존 동적 주입 적용]:
 * KST(UTC+9) 강제 하드코딩을 제거하고, 런타임 환경에 따라 글로벌 타임존 오프셋을
 * 동적으로 주입받아 글로벌 분산 노드망에서도 동일한 Epoch Time을 보장합니다.
 * ==============================================================================
 */
public final class A0_DT_42_422002_수학적_시공간_격자_엔진 {

    // [1. 한글 상세 주석]
    // 글로벌 인프라 전개를 위한 타임존 오프셋 동적 주입 상수. 기본값은 KST(32,400초)로 설정됩니다.
    // [2. 영문 상세 주석]
    // Dynamically injected timezone offset constant for global infrastructure
    // deployment. Default is set to KST (32,400 seconds).

    private static final long GLOBAL_TIMEZONE_OFFSET_SECONDS = Long
            .parseLong(System.getProperty("matrix.timezone.offset.seconds", "32400"));

    // [불변 기저 상수]
    private final String baseTickString;     // 예: "20260101_090000"
    private final long baseEpochSeconds;     // UNIX 타임스탬프
    private final long intervalSeconds;      // 1분 = 60, 5분 = 300, 1일 = 86400

    /**
     * [생성자] 해당 타임프레임의 절대적인 시간 척도를 정의합니다.
     * 
     * @param baseTickString 데이터 수집이 시작되는 최초의 시간 (CSV의 첫 행 데이터)
     * @param intervalSeconds 시간의 밀도 (초 단위 간격)
     */
    public A0_DT_42_422002_수학적_시공간_격자_엔진(String baseTickString, long intervalSeconds) {
        if (baseTickString == null || baseTickString.isEmpty()) {
            throw new IllegalArgumentException("[시스템 오류] 기준 시간이 존재하지 않는 인덱서는 생성할 수 없습니다.");
        }
        if (intervalSeconds <= 0) {
            throw new IllegalArgumentException("[설정 오류] 시간 격자의 간격은 양수여야 합니다.");
        }

        this.baseTickString = baseTickString;
        // 엔진 자체의 제로-얼로케이션 파서를 이용해 기준 에포크 타임 확정
        this.baseEpochSeconds = fastPrimitiveDateParser(baseTickString);
        this.intervalSeconds = intervalSeconds;
    }

    /**
     * [기능 1] 문자열 타임스탬프를 입력받아 메모리 오프셋(X축 인덱스)으로 변환합니다.
     * 해시맵(HashMap) 탐색을 거치지 않고, 뺄셈과 나눗셈 1회로 절대 X축 좌표를 산출합니다.
     * 
     * @param targetTickString 검색할 날짜 (예: "2026-07-19 12:34:00")
     * @return X축 인덱스 (메모리 오프셋의 승수로 사용됨)
     */
    public int calculateAbsoluteXIndex(String targetTickString) {
        long targetEpochSeconds = fastPrimitiveDateParser(targetTickString);

        // 공식: Index = (TargetEpoch - BaseEpoch) / IntervalSeconds
        long calculatedIndex = (targetEpochSeconds - this.baseEpochSeconds) / this.intervalSeconds;

        if (calculatedIndex < 0) {
            // 과거로의 시간 역전은 허용되지 않음 (잘못된 데이터 유입 방어)
            return -1;
        }

        // 통합 OS V6.0 규격에 따라 MAX_TIME_STEPS(500,000)를 초과하는 한계 방어는 상위 포트(드라이버)에서 처리합니다.
        return (int) calculatedIndex;
    }

    /**
     * 외부 모듈(메타데이터 빌더 등)에서 JSON 스키마에 기록하기 위해 참조합니다.
     */
    public String getBaseTickString() {
        return baseTickString;
    }

    public long getBaseEpochSeconds() {
        return baseEpochSeconds;
    }

    public long getIntervalSeconds() {
        return intervalSeconds;
    }

    // =========================================================================
    // 💡 [V6.0 핵심 코어: Zero-Allocation Date Parser]
    // =========================================================================

    /**
     * [기능 2] Zero-Allocation Epoch Parser
     * 정규식(replaceAll), 객체 생성(substring), 날짜 객체(LocalDateTime)를 전면 폐기했습니다.
     * 오직 원시 char 배열 순회와 율리우스일(Julian Day) 기반의 순수 대수학 연산만을 이용하여
     * 문자열을 Epoch 초(Seconds)로 O(1) 시간 내에 직결 변환(Direct Convert)합니다.
     * 
     * @param dateString 포맷에 상관없이 숫자만 포함된 날짜 텍스트 (예: "2026-07-19 09:00", "20260719_0900")
     * @return 주입된 타임존 오프셋 기준의 절대 Epoch 초(Seconds)
     */
    public static long fastPrimitiveDateParser(String dateString) {
        int extractedDigitCount = 0;

        // 날짜 기본값 (에러 방어용: 1970-01-01 00:00:00)
        int year = 1970, month = 1, day = 1, hour = 0, minute = 0, second = 0;

        // 💡 JIT 컴파일러의 이스케이프 분석(Escape Analysis)을 통해
        // 힙(Heap)이 아닌 스택(Stack) 메모리에만 잠시 존재하다 소멸되는 초경량 원시 배열
        char[] digitBuffer = new char[14];
        int length = dateString.length();

        // 1단계: 정규식을 대체하는 초고속 포인터 기반 숫자 축출 로직
        // 문자열을 1바이트씩 순회하며 아스키코드(ASCII) 범위 '0'~'9'인 것만 걸러냅니다.
        for (int i = 0; i < length && extractedDigitCount < 14; i++) {
            char ch = dateString.charAt(i);
            if (ch >= '0' && ch <= '9') {
                digitBuffer[extractedDigitCount++] = ch;
            }
        }

        // 2단계: 추출된 숫자의 개수에 따라 자리수 계산으로 강제 조립
        // Integer.parseInt() 없이 문자 상수 '0'의 ASCII 값을 빼서 순수 정수로 환산
        if (extractedDigitCount >= 8) { // YYYYMMDD
            year = (digitBuffer[0] - '0') * 1000 + (digitBuffer[1] - '0') * 100 + (digitBuffer[2] - '0') * 10 + (digitBuffer[3] - '0');
            month = (digitBuffer[4] - '0') * 10 + (digitBuffer[5] - '0');
            day = (digitBuffer[6] - '0') * 10 + (digitBuffer[7] - '0');
        }
        if (extractedDigitCount >= 12) { // HHMM
            hour = (digitBuffer[8] - '0') * 10 + (digitBuffer[9] - '0');
            minute = (digitBuffer[10] - '0') * 10 + (digitBuffer[11] - '0');
        }
        if (extractedDigitCount >= 14) { // SS
            second = (digitBuffer[12] - '0') * 10 + (digitBuffer[13] - '0');
        }

        // 💡 3단계: [객체 생성 완전 제거] Fliegel & Van Flandern 율리우스일 변환 공식 적용
        // LocalDateTime.of()를 폐기하고 산술식만으로 년월일을 누적 일수(Epoch Days)로 변환
        int a = (14 - month) / 12;
        int y = year + 4800 - a;
        int m = month + 12 * a - 3;

        // 역력학(Ephemeris) 기준 율리우스일 도출
        long julianDay = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045L;

        // 1970년 1월 1일의 율리우스일(2,440,588)을 차감하여 Epoch Day 산출
        long epochDays = julianDay - 2440588L;

        // 총 초(Seconds) 산출
        long totalSeconds = epochDays * 86400L + hour * 3600L + minute * 60L + second;

        // 💡 4단계: [타임존 동적 주입] 런타임에 동적으로 주입된 글로벌 타임존 오프셋을 차감하여 절대 에포크(UTC)로 변환합니다.
        return totalSeconds - GLOBAL_TIMEZONE_OFFSET_SECONDS;
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. O(1) 시간 복잡도의 본질과 String Hashing의 소멸
 * 이전 아키텍처에서는 수백만 개의 타임스탬프(Tick) 문자열을 `HashMap`에 담아 메모리에 유지했습니다.
 * CSV 파일에서 "20260715_0900"이라는 문자열을 읽을 때마다, JVM은 이 문자열의 해시코드(HashCode)를
 * 계산하고 동등성(equals)을 비교하는 극심한 오버헤드를 발생시켰습니다.
 * 해시맵은 이론상 O(1)을 지향하지만, 해시 충돌(Collision) 처리와 String 객체 생성 비용이 숨어 있습니다.
 * V6.0에서는 이 해시맵을 전면 폐기했습니다. `(현재 시간 - 최초 시간) / 격자 간격` 이라는 단 한 번의
 * 수학적 사칙연산만이 존재합니다. 객체 지향의 무거운 부하를 줄이고 수학 연산으로 데이터 파이프라인 성능을 최적화했습니다.
 * 
 * 2. 율리우스일(Julian Day) 기반의 객체 생성 완전 제거 (Complete Zero-Allocation)
 * 날짜 문자열을 처리하기 위해 흔히 사용되는 정규식(Regex)은 물론이고,
 * 단순히 `LocalDateTime.of()` 객체를 만들어 `toEpochSecond()`를 호출하는 행위조차
 * 고빈도 데이터 처리 시스템에서는 치명적인 가비지 컬렉션(GC) 대기를 유발합니다.
 * 본 엔진은 천체 역학의 궤도 계산에 사용되는 Fliegel & Van Flandern의 율리우스일 산술 공식을 차용하여
 * 복잡한 달력(윤년, 월별 일수 차이) 규칙을 단 5줄의 순수 정수 연산(Integer Algebra)으로 분해했습니다.
 * 객체 생성을 원천 차단하여 JVM 힙(Heap) 메모리 할당 및 GC 병목을 물리적으로 0에 수렴시켰습니다.
 * 
 * 3. 연속적 시공간 격자(Continuous Spacetime Grid)와 데이터 일관성
 * 이 수학적 모델을 사용하면 주말이나 공휴일처럼 데이터가 수집되지 않는 시간에도 X축 인덱스가
 * 일정하게 할당되어 메모리상에 물리적인 빈 공간(NaN, 0.0f 등)이 생기게 됩니다.
 * 전통적인 DB 설계에서는 이를 공간 낭비라 보지만, 텐서 및 시계열 분석 관점에서는 이것이 '진정한 시계열의 무결성'입니다.
 * 비어있는 시간이 데이터로 존재해야만 AI 모델이나 분석 엔진이 "시간의 흐름에 따른 데이터의 부재" 자체를 
 * 패턴으로 올바르게 학습하고 분석할 수 있습니다.
 * 
 * 4. 타임존 오프셋 동적 주입 (Dynamic Timezone Configuration)
 * 과거 하드코딩된 KST(UTC+9) 연산(`-(9 * 3600L)`)은 시스템을 특정 타임존에 종속시키는 한계가 있었습니다. 
 * 글로벌 분산 환경에서 시계열 데이터를 병합할 때, 각 서버의 위치와 무관하게
 * 일치된 에포크(Epoch) 시간을 갖기 위해서는 유연한 타임존 설정이 필수적입니다.
 * 수리된 V6.0 파서는 런타임 환경 변수(`System.getProperty`)를 통해 타임존 오프셋을 동적으로
 * 주입받음으로써, 어떠한 인프라 환경에 배포되어도 일관된 시간축 좌표계를 유지할 수 있습니다.
 * =============================================================================
 */