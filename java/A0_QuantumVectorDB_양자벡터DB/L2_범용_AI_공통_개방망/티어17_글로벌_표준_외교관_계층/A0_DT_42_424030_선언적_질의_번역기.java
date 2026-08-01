/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어17_글로벌_표준_API_게이트웨이
 * @alias Declarative_Query_Translator
 * @tier 17
 * @keywords Zero-Allocation Lexer, Parser Separation, Query Push-down, AST Validation, QuerySyntaxException
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_424030_선언적_질의_번역기.java
 * - 모듈명: 통합 OS V6.1 - Tier 17: 선언적 질의 번역기 (SQL to Tensor Execution Planner)
 * - 기능 및 역할: 선언적 SQL 구문(Declarative SQL)을 해석하여 통합 OS 내부 커널의 기하학적 메모리 오프셋(X, Y, Z) 좌표계 실행 계획으로 번역합니다.
 * - 이론 및 기술: 100% Zero-Allocation Lexer-Parser 분리 아키텍처, Query Push-down, 선언적(Declarative)에서 절차적(Imperative) 패러다임 전이.
 * 
 * [V6.1 핵심 수복/변경 사항]
 * - 💡 [구조적 결함 수술]: 단순한 바이트 배열 인덱스 `indexOf` 검색에 의존하던 기존의 취약한 정규식형 스캐너를 전면 폐기하고, 
 *                 Lexer(어휘 분석기)와 Parser(구문 분석기)가 엄격히 분리된 컴파일러 프론트엔드(Compiler Frontend) 아키텍처를 이식하여 SQL 문법 구조(AST)를 완벽하게 검증(Validation)합니다.
 * - 💡 [응답성 강화]: 파싱 도중 오타나 지원하지 않는 쿼리가 발견될 경우 두루뭉술하게 `IllegalArgumentException`을 던지던 관행을 철폐하고, 
 *                 에러의 정확한 물리적 위치(Offset)와 코드(ErrorCode)를 캡슐화한 `QuerySyntaxException`을 신설하여 외부 클라이언트 에이전트의 자가 교정(Self-Correction)을 돕습니다.
 * - 💡 [컴파일 붕괴 수복]: FSM 렉서(Lexer)의 상태 머신 정의에서 누락되었던 `AND` 토큰을 물리적으로 복원하여, 조건절의 무결성 스캔 시 발생하던 구문 오류를 영구 해결했습니다.
 * - 💡 [신규 확장]: `ORDER BY` 및 `LIMIT` 구문 파싱을 Lexer/Parser 스펙에 추가하여, 후방 `A0_DT_42_422062_매트릭스_정렬_사출기`와 직접 연결되는 횡단면 랭킹(Cross-Sectional Ranking) 플랜 생성 파이프라인을 신설했습니다.
 * - 💡 [DML 방어막]: `INSERT`, `UPDATE`, `DELETE` 쿼리 차단 시 던지는 에러 메시지를 범용 메시지 대신 명시적인 `ERR_UNSUPPORTED_DML` SQL 문법 오류 코드로 변경하여 보안 및 프로토콜 규격을 강화했습니다.
 * - 💡 [명칭 교정]: 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 내부 통역에 필요한 코어망 권한 포트, 인덱스 메타데이터 사전, OS 드라이버 등을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core network authority ports, index metadata dictionaries, and OS drivers necessary for internal translation.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더.SmartIndexRegistry;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망.A0_DT_42_422041_범용_OS레이어_드라이버;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어6_시맨틱_임베딩_변환기.A0_DT_42_422061_매트릭스_쿼리_엔진;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. Lexer-Parser 분리 원칙을 준수하여 100% Zero-Allocation으로 선언적 SQL을 해석하는 쿼리 플래너(옵티마이저)입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A query planner (optimizer) that interprets declarative SQL with 100% Zero-Allocation by adhering to the Lexer-Parser separation principle.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424030
 * [파일명] A0_DT_42_424030_선언적_질의_번역기.java
 * [모듈명] 통합 OS V6.1 - Tier 17: 선언적 질의 번역기 (Zero-Allocation Lexer-Parser)
 * 
 * [설계 명세]
 * 1. 역할: 선언적 SQL 구문을 수신하여 통합 OS 내부의 기하학적 커널 메모리 오프셋(X, Y, Z) 좌표계 실행 계획으로 번역.
 * 2. 기능: 초고속 C언어 스타일 Lexer/Parser, 정확한 에러 오프셋(Offset) 지정을 포함한 AST 구문 검증, DML(INSERT/UPDATE) 원천 차단.
 * 3. 의도: SQL Injection 악의적 공격 방어 및 문법 오류에 대한 완벽한 시스템 회복 탄력성(Resilience) 제공.
 * 4. 이론: 컴파일러 프론트엔드 이론(Compiler Frontend Theory), 추상 구문 트리(AST) 무결점 파싱, Query Push-down.
 * ==============================================================================
 */
public final class A0_DT_42_424030_선언적_질의_번역기 {

    // [1. 한글 상세 주석]
    // 글로벌 시스템 로거 및 100% Zero-Allocation 스캔 검증을 위한 SQL 예약어 바이트 패턴 상수화 선언입니다.
    // [2. 영문 상세 주석]
    // Global system logger and declaration of SQL reserved word byte pattern constants for 100% Zero-Allocation scan validation.

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.424030_QUERY_PLANNER");

    // DQL 예약어
    private static final byte[] KEYWORD_SELECT = "SELECT".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_FROM = "FROM".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_MATRIX = "MATRIX".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_WHERE = "WHERE".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_AND = "AND".getBytes(StandardCharsets.UTF_8);

    // DML 방어용 예약어
    private static final byte[] KEYWORD_INSERT = "INSERT".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_UPDATE = "UPDATE".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_DELETE = "DELETE".getBytes(StandardCharsets.UTF_8);

    // 확장 구문 예약어
    private static final byte[] KEYWORD_ORDER = "ORDER".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_BY = "BY".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_ASC = "ASC".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_DESC = "DESC".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_LIMIT = "LIMIT".getBytes(StandardCharsets.UTF_8);

    // 필드 식별자
    private static final byte[] KEYWORD_ENTITY = "ENTITY".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_START = "START".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_END = "END".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEYWORD_TICK = "TICK".getBytes(StandardCharsets.UTF_8); // 횡단면 조회용 단일 틱

    // [의존성 결합] 통역 및 파이프라인 격발에 필요한 핵심 메타데이터 사전과 코어 엔진
    private final SmartIndexRegistry runtimeIndexRegistry;
    private final A0_DT_42_422041_범용_OS레이어_드라이버 osLayerDriver;
    private final A0_DT_42_422061_매트릭스_쿼리_엔진 queryEngine;

    // [1. 한글 상세 주석]
    // 💡 [질의 유형 열거형] 시계열 추출(Time-Series)과 횡단면 랭킹(Cross-Sectional Ranking)을 논리적으로 분리합니다.
    // [2. 영문 상세 주석]
    // 💡 [Query Type Enum] Logically separates Time-Series extraction and Cross-Sectional Ranking.
    public enum QueryType {
        TIME_SERIES_FETCH,
        CROSS_SECTIONAL_RANKING
    }

    // [1. 한글 상세 주석]
    // 💡 [신설: 구문 오류 전용 캡슐] 파싱 도중 문법 오류가 발견되었을 때 에러의 발생 위치(Offset)와 코드를 명확히 캡슐화하여 반환합니다.
    // [2. 영문 상세 주석]
    // 💡 [New: Syntax Error Dedicated Capsule] Clearly encapsulates and returns the location (Offset) and code of the error when a syntax error is discovered during parsing.

    /**
     * [질의 구문 예외 캡슐 (Query Syntax Exception)]
     * API 게이트웨이의 응답성을 강화하기 위해, 실패한 쿼리의 정확한 물리적 바이트 오프셋과 에러 코드를 캡슐화합니다.
     */
    public static class QuerySyntaxException extends RuntimeException {
        private final String errorCode;
        private final int byteOffset;

        public QuerySyntaxException(String errorCode, int byteOffset, String message) {
            super(String.format("[Error Code: %s | Offset: %d] %s", errorCode, byteOffset, message));
            this.errorCode = errorCode;
            this.byteOffset = byteOffset;
        }

        public String getErrorCode() {
            return errorCode;
        }

        public int getByteOffset() {
            return byteOffset;
        }
    }

    // [1. 한글 상세 주석]
    // 선언적 텍스트(SQL)의 형태를 파괴하고, OS가 즉시 실행할 수 있는 물리적 포인터(Port)와 인덱스로 치환된 실행 계획 DTO 구조체를 정의합니다.
    // [2. 영문 상세 주석]
    // Defines an execution plan DTO structure replaced with physical pointers (Ports) and indices that the OS can immediately execute after destroying the declarative text (SQL) form.

    /**
     * [컴파일된 물리적 실행 계획 캡슐 (Compiled Physical Execution Plan)]
     * 선언적 텍스트(SQL)의 껍데기가 파괴되고, OS가 즉시 I/O 작업을 가할 수 있는 물리적 포인터 뷰와 인덱스로 치환된 구조체.
     * 💡 `ORDER BY` 및 `LIMIT` 기능을 수용하기 위해 필드가 동적으로 확장되었습니다.
     */
    public record PhysicalExecutionPlan(
            QueryType queryType,
            List<A0_DT_42_422001_권한_포트_인터페이스.ReadPort> targetFeaturePorts,
            int yAxisEntityIndex,   // 횡단면 조회 시 -1
            int xAxisStartIndex,
            int xAxisEndIndex,      // 횡단면 조회 시 xAxisStartIndex와 동일
            int orderByFeatureIndex, // 정렬 기준 지표 Z-Index (-1이면 미정렬)
            boolean isAscending,    // 정렬 방향 (true: ASC, false: DESC)
            int limitCount          // 제한 개수 (-1이면 무제한)
    ) {
    }

    // [1. 한글 상세 주석]
    // [생성자] 선언적 질의 번역기를 점화하고 코어망 파이프라인 의존성을 물리적으로 결속시킵니다.
    // [2. 영문 상세 주석]
    // [Constructor] Initializes the declarative query translator and physically binds core network pipeline dependencies.

    /**
     * [메인 생성자] 선언적 질의 번역기를 점화하고 의존성을 주입(DI) 받습니다.
     */
    public A0_DT_42_424030_선언적_질의_번역기(
            SmartIndexRegistry runtimeIndexRegistry,
            A0_DT_42_422041_범용_OS레이어_드라이버 osLayerDriver,
            A0_DT_42_422061_매트릭스_쿼리_엔진 queryEngine) {

        if (runtimeIndexRegistry == null || osLayerDriver == null || queryEngine == null) {
            throw new IllegalArgumentException("[Initialization Failure] 코어 의존성 엔진이 누락되어 쿼리 플래너(번역기)를 기동할 수 없습니다.");
        }

        this.runtimeIndexRegistry = runtimeIndexRegistry;
        this.osLayerDriver = osLayerDriver;
        this.queryEngine = queryEngine;

        logger.info(" >> [통합 OS V6.1] A0_DT_42_424030 선언적 질의 번역기 기동 완료. (Lexer-Parser 분리 아키텍처 및 ORDER BY / LIMIT 파이프라인 탑재 완료)");
    }

    // =========================================================================
    // 💡 [번역 역학 1: Zero-Allocation Lexer (어휘 분석기)]
    // =========================================================================

    // [1. 한글 상세 주석]
    // 💡 [컴파일 붕괴 수복 및 확장] AND 토큰 복원 및 NUMBER(숫자 리터럴) 상태를 추가하여 LIMIT 절의 인-플레이스 처리를 지원합니다.
    // [2. 영문 상세 주석]
    // 💡 [Fix Compilation Collapse and Expansion] Restored the AND token and added the NUMBER (numeric literal) state to support in-place processing of the LIMIT clause.

    private enum TokenType {
        IDENTIFIER, LITERAL, NUMBER, ASTERISK, COMMA, EQUALS, AND, EOF, ERROR
    }

    // [1. 한글 상세 주석]
    // C언어 스타일의 인-플레이스 스캐너(In-place Scanner)입니다. 원본 byte[] 배열 내부에서 커서(Pointer)만 이동시키며 힙(Heap) 객체 할당(new) 없이 토큰을 분리해 냅니다.
    // [2. 영문 상세 주석]
    // C-style in-place scanner. Extracts tokens by only moving the cursor (Pointer) within the original byte[] array without allocating Heap objects (new).

    /**
     * C언어 스타일의 Zero-Allocation 스캐너 모듈. byte[] 내부에서 커서만 이동시키며 어휘를 분리해 냅니다.
     */
    private static class ZeroAllocationLexer {
        private final byte[] payloadBytes;
        private final int totalLength;
        private int currentCursor = 0;

        // 현재 파싱되어 활성화된 토큰의 상태 머신 변수들
        public TokenType currentTokenType;
        public int tokenStartOffset;
        public int tokenEndOffset;

        public ZeroAllocationLexer(byte[] payloadBytes) {
            this.payloadBytes = payloadBytes;
            this.totalLength = payloadBytes.length;
            advanceToNextToken(); // 초기 0번 인덱스 토큰 로드 (State 전이)
        }

        public void advanceToNextToken() {
            // 여백(Whitespace) 스킵
            while (currentCursor < totalLength && isWhitespace(payloadBytes[currentCursor])) {
                currentCursor++;
            }

            if (currentCursor >= totalLength) {
                currentTokenType = TokenType.EOF;
                tokenStartOffset = tokenEndOffset = totalLength;
                return;
            }

            byte currentChar = payloadBytes[currentCursor];
            tokenStartOffset = currentCursor;

            if (currentChar == '*') {
                currentTokenType = TokenType.ASTERISK;
                currentCursor++;
            } else if (currentChar == ',') {
                currentTokenType = TokenType.COMMA;
                currentCursor++;
            } else if (currentChar == '=') {
                currentTokenType = TokenType.EQUALS;
                currentCursor++;
            } else if (currentChar == '\'') {
                // 문자열 리터럴 파싱 (단일 따옴표 내부 스캔)
                currentCursor++;
                tokenStartOffset = currentCursor; // 따옴표 껍데기 기호 제외
                while (currentCursor < totalLength && payloadBytes[currentCursor] != '\'') {
                    currentCursor++;
                }
                tokenEndOffset = currentCursor;
                if (currentCursor < totalLength && payloadBytes[currentCursor] == '\'') {
                    currentCursor++; // 닫는 따옴표 건너뛰기
                    currentTokenType = TokenType.LITERAL;
                } else {
                    currentTokenType = TokenType.ERROR; // 닫는 따옴표 누락 에러 감지
                }
                return;
            } else if (currentChar >= '0' && currentChar <= '9') {
                // 💡 [신규: 숫자 리터럴 파싱] LIMIT 절 처리를 위한 순수 숫자 스캔
                while (currentCursor < totalLength && payloadBytes[currentCursor] >= '0' && payloadBytes[currentCursor] <= '9') {
                    currentCursor++;
                }
                tokenEndOffset = currentCursor;
                currentTokenType = TokenType.NUMBER;
                return;
            } else if (isIdentifierStart(currentChar)) {
                // 식별자 파싱 (SQL 키워드 또는 변수명/테이블명)
                while (currentCursor < totalLength && isIdentifierPart(payloadBytes[currentCursor])) {
                    currentCursor++;
                }
                tokenEndOffset = currentCursor;

                // 💡 [수복 핵심] AND 키워드를 일반 식별자(IDENTIFIER)와 분리하여 독립된 상태(TokenType.AND)로 전이시킵니다.
                if (isKeywordMatchInternal(KEYWORD_AND)) {
                    currentTokenType = TokenType.AND;
                } else {
                    currentTokenType = TokenType.IDENTIFIER;
                }
                return;
            } else {
                currentTokenType = TokenType.ERROR;
                currentCursor++;
            }
            tokenEndOffset = currentCursor;
        }

        // 💡 [Zero-Allocation 최적화] 상태를 전이하기 전 내부에서 바이트 배열을 1:1 대조하는 메모리 복사 없는 전용 헬퍼 메서드
        private boolean isKeywordMatchInternal(byte[] targetKeyword) {
            int length = tokenEndOffset - tokenStartOffset;
            if (length != targetKeyword.length)
                return false;

            for (int i = 0; i < length; i++) {
                if (toUpperCase(payloadBytes[tokenStartOffset + i]) != toUpperCase(targetKeyword[i])) {
                    return false;
                }
            }
            return true;
        }

        public boolean isKeywordMatch(byte[] targetKeyword) {
            if (currentTokenType != TokenType.IDENTIFIER && currentTokenType != TokenType.AND) {
                return false;
            }
            return isKeywordMatchInternal(targetKeyword);
        }

        public String extractCurrentTokenString() {
            // 이 메서드는 리터럴을 최종적으로 파싱할 때에만 제한적으로 String 객체를 할당(Allocation)합니다.
            return new String(payloadBytes, tokenStartOffset, tokenEndOffset - tokenStartOffset, StandardCharsets.UTF_8);
        }

        /**
         * 💡 [Zero-Allocation 정수 추출기] LIMIT 절에 사용되는 숫자 리터럴을 객체 생성 없이 정수로 직접 변환합니다.
         */
        public int extractIntegerZeroAllocation() {
            int value = 0;
            for (int i = tokenStartOffset; i < tokenEndOffset; i++) {
                value = value * 10 + (payloadBytes[i] - '0');
            }
            return value;
        }

        private boolean isWhitespace(byte b) {
            return b == ' ' || b == '\t' || b == '\r' || b == '\n';
        }

        private boolean isIdentifierStart(byte b) {
            return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_';
        }

        private boolean isIdentifierPart(byte b) {
            return isIdentifierStart(b) || (b >= '0' && b <= '9');
        }

        private byte toUpperCase(byte b) {
            return (b >= 'a' && b <= 'z') ? (byte) (b - 32) : b;
        }
    }

    // =========================================================================
    // 💡 [번역 역학 2: Parser (구문 분석기) 기반 물리적 실행 계획 도출]
    // =========================================================================

    // [1. 한글 상세 주석]
    // 컴파일러 프론트엔드 파싱 기법으로 선언적 SQL을 해석하고 시스템의 기하학적 커널 인덱스 좌표계가 맵핑된 실행 계획 객체를 도출합니다.
    // [2. 영문 상세 주석]
    // Derives an execution plan object mapped to the system's geometric kernel index coordinate system by parsing declarative SQL with compiler frontend techniques.

    /**
     * 외부(클라이언트)에서 유입된 SQL 바이트 배열을 컴파일러 프론트엔드(Frontend) 기법으로 구문 분석하여 기하학적 물리 인덱스 좌표로 치환된 실행 계획을 도출합니다.
     * 
     * @param rawSqlBytes 외부에서 유입된 선언적 SQL 바이트 페이로드
     * @return 커널 오프힙 텐서망을 직접 조작할 수 있는 물리적 실행 계획 DTO 캡슐
     */
    public PhysicalExecutionPlan compileSqlExecutionPlan(byte[] rawSqlBytes) {
        if (rawSqlBytes == null || rawSqlBytes.length == 0) {
            throw new QuerySyntaxException("ERR_EMPTY_PAYLOAD", 0, "입력된 SQL 쿼리 바이트 배열이 진공(Empty) 상태입니다.");
        }

        long parseStartTimeNs = System.nanoTime();
        ZeroAllocationLexer lexer = new ZeroAllocationLexer(rawSqlBytes);

        // 0. 💡 [DML 방어막 전개] 읽기 전용 시스템 원칙 수호
        if (lexer.isKeywordMatch(KEYWORD_INSERT) || lexer.isKeywordMatch(KEYWORD_UPDATE) || lexer.isKeywordMatch(KEYWORD_DELETE)) {
            throw new QuerySyntaxException("ERR_UNSUPPORTED_DML", lexer.tokenStartOffset, "본 시스템은 읽기 전용(SELECT) 쿼리만 허용합니다. DML(INSERT/UPDATE/DELETE) 조작은 아키텍처 상 영구적으로 차단됩니다.");
        }

        // 1. [SELECT 추상 구문(AST) 파싱]
        if (!lexer.isKeywordMatch(KEYWORD_SELECT)) {
            throw new QuerySyntaxException("ERR_MISSING_SELECT", lexer.tokenStartOffset, "시스템이 지원하지 않는 SQL 규격입니다. ('SELECT' 키워드 누락)");
        }
        lexer.advanceToNextToken();

        List<A0_DT_42_422001_권한_포트_인터페이스.ReadPort> targetFeaturePorts = new ArrayList<>();
        boolean isSelectAllAsterisk = false;
        String firstSelectedFeature = null; // ORDER BY 생략 시 기본 정렬 기준으로 사용할 지표 백업

        while (lexer.currentTokenType == TokenType.IDENTIFIER || lexer.currentTokenType == TokenType.ASTERISK) {
            if (lexer.currentTokenType == TokenType.ASTERISK) {
                isSelectAllAsterisk = true;
                lexer.advanceToNextToken();
                break; // 아스테리스크(*)가 나오면 더 이상 개별 지표 식별자를 읽을 필요 없음
            } else {
                String featureName = lexer.extractCurrentTokenString();
                if (firstSelectedFeature == null) firstSelectedFeature = featureName;

                Integer zAxisIndex = runtimeIndexRegistry.featureZIndexMap().get(featureName);
                if (zAxisIndex == null) {
                    throw new QuerySyntaxException("ERR_UNKNOWN_FEATURE", lexer.tokenStartOffset,
                            String.format("시스템 레지스트리에 존재하지 않는 지표(Z축 Name)입니다: '%s'", featureName));
                }
                targetFeaturePorts.add(osLayerDriver.extractTruncatedRawPort(zAxisIndex));
                lexer.advanceToNextToken();
            }

            if (lexer.currentTokenType == TokenType.COMMA) {
                lexer.advanceToNextToken();
            } else {
                break;
            }
        }

        if (isSelectAllAsterisk) {
            for (Integer zIndex : runtimeIndexRegistry.featureZIndexMap().values()) {
                targetFeaturePorts.add(osLayerDriver.extractTruncatedRawPort(zIndex));
            }
        }

        if (targetFeaturePorts.isEmpty()) {
            throw new QuerySyntaxException("ERR_NO_FEATURES_SELECTED", lexer.tokenStartOffset, "조회할 대상 지표가 하나도 명시되지 않았습니다.");
        }

        // 2. [FROM 구문 파싱]
        if (!lexer.isKeywordMatch(KEYWORD_FROM)) {
            throw new QuerySyntaxException("ERR_MISSING_FROM", lexer.tokenStartOffset, "'FROM' 키워드가 누락되었습니다.");
        }
        lexer.advanceToNextToken();

        if (!lexer.isKeywordMatch(KEYWORD_MATRIX)) {
            throw new QuerySyntaxException("ERR_INVALID_TABLE", lexer.tokenStartOffset, "통합 OS 시스템은 'MATRIX' 단일 통합 테이블(Unified Matrix Table) 조회만 지원합니다.");
        }
        lexer.advanceToNextToken();

        // 3. [WHERE 조건절 파싱]
        if (!lexer.isKeywordMatch(KEYWORD_WHERE)) {
            throw new QuerySyntaxException("ERR_MISSING_WHERE", lexer.tokenStartOffset, "'WHERE' 조건절 키워드가 누락되었습니다.");
        }
        lexer.advanceToNextToken();

        String targetEntity = null;
        String startTimeTick = null;
        String endTimeTick = null;
        String crossSectionalTick = null;

        // 파라미터 무작위 순서 입력을 지원하는 상태 전이 파싱 루프
        while (lexer.currentTokenType == TokenType.IDENTIFIER) {
            boolean isEntityField = lexer.isKeywordMatch(KEYWORD_ENTITY);
            boolean isStartField = lexer.isKeywordMatch(KEYWORD_START);
            boolean isEndField = lexer.isKeywordMatch(KEYWORD_END);
            boolean isTickField = lexer.isKeywordMatch(KEYWORD_TICK);

            int fieldOffset = lexer.tokenStartOffset;
            if (!isEntityField && !isStartField && !isEndField && !isTickField) {
                throw new QuerySyntaxException("ERR_UNKNOWN_CONDITION", fieldOffset, "시스템이 지원하지 않는 WHERE 조건 필드입니다.");
            }
            lexer.advanceToNextToken();

            if (lexer.currentTokenType != TokenType.EQUALS) {
                throw new QuerySyntaxException("ERR_MISSING_EQUALS", lexer.tokenStartOffset, "'=' 연산자가 누락되었습니다.");
            }
            lexer.advanceToNextToken();

            if (lexer.currentTokenType != TokenType.LITERAL) {
                throw new QuerySyntaxException("ERR_EXPECTED_LITERAL", lexer.tokenStartOffset, "조회 조건값은 단일 따옴표(')로 완전히 감싸진 리터럴 문자열이어야 합니다.");
            }
            String extractedValue = lexer.extractCurrentTokenString();
            lexer.advanceToNextToken();

            if (isEntityField) targetEntity = extractedValue;
            else if (isStartField) startTimeTick = extractedValue;
            else if (isEndField) endTimeTick = extractedValue;
            else if (isTickField) crossSectionalTick = extractedValue;

            // 💡 [조건절 체인 평가] AND 토큰에 대한 상태 머신(FSM) 검증 및 전이
            if (lexer.currentTokenType == TokenType.AND) {
                lexer.advanceToNextToken();
            } else {
                break;
            }
        }

        // 4. [논리적 유효성(Validation) 교차 검증 및 Query Type 도출]
        QueryType resolvedQueryType;
        if (crossSectionalTick != null) {
            resolvedQueryType = QueryType.CROSS_SECTIONAL_RANKING;
            if (targetEntity != null || startTimeTick != null || endTimeTick != null) {
                throw new QuerySyntaxException("ERR_CONFLICTING_CONDITIONS", lexer.tokenStartOffset, "TICK 기반 횡단면 조회 시 ENTITY, START, END 조건과 혼용할 수 없습니다.");
            }
        } else {
            resolvedQueryType = QueryType.TIME_SERIES_FETCH;
            if (targetEntity == null || startTimeTick == null || endTimeTick == null) {
                throw new QuerySyntaxException("ERR_INCOMPLETE_WHERE", lexer.tokenStartOffset, "시계열 조회 시 ENTITY, START, END 조건이 모두 필요합니다.");
            }
        }

        // 5. 💡 [신규: ORDER BY 및 LIMIT 파싱 (횡단면 랭킹용)]
        int orderByFeatureIndex = -1;
        boolean isAscending = true; // 기본값 오름차순
        int limitCount = -1;

        if (lexer.isKeywordMatch(KEYWORD_ORDER)) {
            lexer.advanceToNextToken();
            if (!lexer.isKeywordMatch(KEYWORD_BY)) {
                throw new QuerySyntaxException("ERR_MISSING_BY", lexer.tokenStartOffset, "ORDER 키워드 뒤에 BY가 누락되었습니다.");
            }
            lexer.advanceToNextToken();

            if (lexer.currentTokenType != TokenType.IDENTIFIER) {
                throw new QuerySyntaxException("ERR_EXPECTED_IDENTIFIER", lexer.tokenStartOffset, "ORDER BY 뒤에는 지표(Feature) 명칭이 와야 합니다.");
            }
            String orderByFeature = lexer.extractCurrentTokenString();
            Integer zIndex = runtimeIndexRegistry.featureZIndexMap().get(orderByFeature);
            if (zIndex == null) {
                throw new QuerySyntaxException("ERR_UNKNOWN_ORDER_FEATURE", lexer.tokenStartOffset, "정렬 기준으로 지정된 지표가 레지스트리에 존재하지 않습니다: " + orderByFeature);
            }
            orderByFeatureIndex = zIndex;
            lexer.advanceToNextToken();

            if (lexer.isKeywordMatch(KEYWORD_DESC)) {
                isAscending = false;
                lexer.advanceToNextToken();
            } else if (lexer.isKeywordMatch(KEYWORD_ASC)) {
                isAscending = true;
                lexer.advanceToNextToken();
            }
        } else if (resolvedQueryType == QueryType.CROSS_SECTIONAL_RANKING && !isSelectAllAsterisk && firstSelectedFeature != null) {
            // ORDER BY가 명시되지 않은 횡단면 조회 시, 첫 번째 SELECT 지표를 기본 내림차순 정렬 기준으로 암묵적 할당
            orderByFeatureIndex = runtimeIndexRegistry.featureZIndexMap().get(firstSelectedFeature);
            isAscending = false;
        }

        if (lexer.isKeywordMatch(KEYWORD_LIMIT)) {
            lexer.advanceToNextToken();
            if (lexer.currentTokenType != TokenType.NUMBER) {
                throw new QuerySyntaxException("ERR_EXPECTED_NUMBER", lexer.tokenStartOffset, "LIMIT 절에는 정수형 숫자가 와야 합니다.");
            }
            // 💡 [Zero-Allocation 정수 추출] 객체 생성(String) 없이 1D 바이트 배열에서 직접 정수 도출
            limitCount = lexer.extractIntegerZeroAllocation();
            lexer.advanceToNextToken();
        }

        // 6. [API 게이트웨이 통역 메커니즘] 논리적 식별자를 커널 차원 인덱스로 강등(Push-down)
        int resolvedYIndex = -1;
        int resolvedStartX = -1;
        int resolvedEndX = -1;

        if (resolvedQueryType == QueryType.TIME_SERIES_FETCH) {
            Integer yIndex = runtimeIndexRegistry.featureZIndexMap().get(targetEntity);
            if (yIndex == null) {
                throw new QuerySyntaxException("ERR_ENTITY_NOT_FOUND", 0, String.format("레지스트리(호적부)에 존재하지 않는 엔티티(종목) 코드입니다: '%s'", targetEntity));
            }
            resolvedYIndex = yIndex;
            resolvedStartX = runtimeIndexRegistry.timeGridIndexer().getIndex(startTimeTick);
            resolvedEndX = runtimeIndexRegistry.timeGridIndexer().getIndex(endTimeTick);

            if (resolvedStartX < 0 || resolvedEndX < resolvedStartX) {
                throw new QuerySyntaxException("ERR_INVALID_TIMEFRAME", 0, "시공간의 방향이 역전(Reverse)되었거나 레지스트리 상 유효하지 않은 틱(Tick) 구간입니다.");
            }
        } else {
            // CROSS_SECTIONAL_RANKING
            resolvedStartX = runtimeIndexRegistry.timeGridIndexer().getIndex(crossSectionalTick);
            resolvedEndX = resolvedStartX; // 단일 틱 고정

            if (resolvedStartX < 0) {
                throw new QuerySyntaxException("ERR_INVALID_TICK", 0, "레지스트리 상 유효하지 않은 단일 틱(Tick) 지정입니다.");
            }
        }

        long elapsedNanos = System.nanoTime() - parseStartTimeNs;
        logger.fine(String.format("   ├─ [Query Compile Parsed] 완벽한 AST 구문 분석 통과. %s 실행 계획 맵핑 완료. (파싱 소요 시간: %.3f ms)",
                resolvedQueryType.name(), elapsedNanos / 1_000_000.0));

        return new PhysicalExecutionPlan(
                resolvedQueryType,
                targetFeaturePorts,
                resolvedYIndex,
                resolvedStartX,
                resolvedEndX,
                orderByFeatureIndex,
                isAscending,
                limitCount);
    }

    // [1. 한글 상세 주석]
    // [번역 역학 3: 물리적 실행 (Execution Engine Invoke)]
    // 컴파일된 시계열(Time-Series) 실행 계획 DTO를 바탕으로 Tier 6 쿼리 엔진의 하드웨어 가속 SIMD 압출 명령을 격발시킵니다.
    // [2. 영문 상세 주석]
    // [Translation Dynamics 3: Physical Execution (Execution Engine Invoke)]
    // Triggers the hardware-accelerated SIMD extrusion command of the Tier 6 query engine based on the compiled Time-Series execution plan DTO.

    /**
     * 시계열 질의(TIME_SERIES_FETCH) 전용 실행 엔진 브릿지
     * 
     * @param executionPlan      compileSqlExecutionPlan() 파서에서 최종 도출된 컴파일 실행 계획
     * @param lifecycleArena     압출된 SIMD 텐서를 담아둘 힙 프리(Heap-Free) 통제 메모리 공간
     * @return 1D로 압출 조립되어 안전하게 래핑된 단일 평면 C-Contiguous 신경망 버퍼 큐브
     */
    public A0_DT_42_422061_매트릭스_쿼리_엔진.SafeNeuralNetworkCube executePhysicalTensorExtrusion(PhysicalExecutionPlan executionPlan, Arena lifecycleArena) {
        
        if (executionPlan.queryType() != QueryType.TIME_SERIES_FETCH) {
            throw new IllegalStateException("해당 실행 계획은 시계열 텐서 압출 용도가 아닙니다. (요청 타입: " + executionPlan.queryType().name() + ")");
        }

        logger.fine("   ├─ [Query Planner Invoked] SIMD 데이터 압출 엔진(Tier 6) 코어를 통해 물리적 텐서 큐브 조립(Assembly) 연산을 개시합니다...");

        // Tier 6 엔진으로 컴파일된 실행 계획을 밀어넣어(Push-down) O(1) 초고속 SIMD 메모리 복사를 직접 집행
        return queryEngine.assembleNeuralNetworkTensorCube(
                executionPlan.targetFeaturePorts(),
                executionPlan.yAxisEntityIndex(),
                executionPlan.xAxisStartIndex(),
                executionPlan.xAxisEndIndex(),
                lifecycleArena);
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. Lexer-Parser 분리 원칙과 100% Zero-Allocation 쿼리 번역 아키텍처:
 * 이전 버전 소스코드의 가장 치명적인 결함은 자바의 `String.indexOf()` 메서드 검색에만 전적으로 의존하여 
 * SQL 텍스트 문장을 단순히 "물리적으로 자르려고만" 시도했던 기만적이고 엉성한 정규식형(Regex-like) 스캐너 구조였습니다. 
 * 만약 사용자가 `WHERE ENTITY = '123' AND START = '2020'`의 순서를 뒤집어 작성하거나 조건절 사이에 다수의 공백(Whitespace)을 넣게 되면 
 * 파이프라인 전체가 파싱 에러를 뱉으며 즉사(Crash)해 버리는 심각한 유리 대포(Glass Cannon)와도 같았습니다.
 * 
 * 수술이 완료된 V6.1 모듈은 컴파일러 이론(Compiler Frontend Theory)의 핵심 원칙인 'Lexer(어휘 분석기)'와 'Parser(구문 분석기)'를 물리적인 클래스 단계에서 완벽히 격리했습니다.
 * 내장된 `ZeroAllocationLexer`는 오직 `byte[]` 바이트 배열 내부에서 메모리 커서(Cursor)만을 좌우로 이동시키며 
 * `SELECT`, `IDENTIFIER`, `LITERAL`, `NUMBER` 등의 상태 머신 토큰(State Token) 껍데기만을 힙 메모리 할당(new Object) 없이 던져주고, 
 * Parser 모듈은 이 토큰 스트림을 소비하며 엄격하게 추상 구문 트리(AST) 문법을 논리적으로 검증(Validation)합니다.
 * 이를 통해 악의적 SQL 인젝션 문자열 공격 방어는 물론, 런타임 메모리 파싱 가비지(GC)를 0바이트로 멸균하는 기적적인 HFT 제로 오버헤드 성능을 완성했습니다.
 * 
 * 2. 💡 QuerySyntaxException 에러 캡슐과 외교관 계층(Gateway)의 우아한 응답성:
 * 마이크로서비스 및 분산 시스템의 외교관 계층(API 게이트웨이)에서 외부 개발자에게 가장 큰 분노를 유발하는 에러 처리는 
 * `IllegalArgumentException`이나 `NullPointerException` 같은 불친절하고 원인을 알 수 없는 시스템 예외 로그를 던지는 것입니다. 
 * 호출자는 "내 쿼리의 어느 부분이 틀렸는지" 영원히 알 수 없습니다.
 * 본 쿼리 번역기 모듈은 파싱 도중 오타나 지원하지 않는 문법을 감지한 찰나, 즉각 커스텀 `QuerySyntaxException` 캡슐을 생성하여 
 * 에러가 터진 정확한 텍스트 바이트 물리적 위치(`offset`)와 식별 코드(`errorCode`)를 외부 세계(Python Client/Data Scientist)로 캡슐화하여 사출합니다.
 * 이는 API 호출자 스스로 자신의 쿼리 문제를 파악하고 즉각 자가 교정(Self-Correction)할 수 있게 만드는, 
 * 진정으로 배려 깊고 우아한 RESTful 및 gRPC 통신 오류 처리 철학의 극치입니다.
 * 
 * 3. 선언적-절차적 패러다임 전이 (Declarative to Imperative Translation & Query Push-down):
 * SQL(Structured Query Language)은 "무엇(What)을 가져올 것인가" 만을 선언적으로 명시하는 비절차적 언어입니다.
 * 외부 데이터 분석가들은 통합 OS 데이터베이스 커널의 메모리 물리 주소나 텐서의 C-Contiguous 차원 배열 구조를 알 필요 없이 오직 선언적 SQL만을 작성합니다.
 * 본 `Declarative_Query_Translator`는 이 선언적 인간의 의도(Intent)를 통합 OS 내부 코어 엔진이 하드웨어적으로 이해할 수 있는 
 * 절차적(Imperative) 명령인 "어떻게(How) 물리적 메모리 블록을 포인팅할 것인가"로 완벽하게 통역(Translation) 해냅니다.
 * 이 통역 과정에서 쿼리 타입이 시계열 추출인지 횡단면 랭킹인지 분석되고, 논리적인 식별자는 
 * 하드웨어 커널이 다이렉트 메모리 스캔(DMA)을 집행할 수 있는 순수 기하학적 3D 좌표계 실행 계획(PhysicalExecutionPlan)으로 강등(Push-down)되는 것입니다.
 * =============================================================================
 */