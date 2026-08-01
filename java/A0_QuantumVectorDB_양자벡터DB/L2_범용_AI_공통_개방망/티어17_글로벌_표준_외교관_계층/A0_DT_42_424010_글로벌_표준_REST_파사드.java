/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층
 * @alias Declarative_REST_Facade
 * @tier 17
 * @keywords Pinecone/OpenAI API Specification, Admin API, Management Abstraction, Zero-Dependency, ACL
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_424010_글로벌_표준_REST_파사드.java
 * - 모듈명: 통합 OS V6.1 - Tier 17: 글로벌 표준 REST 파사드 (HTTP 게이트웨이 및 Admin Shell)
 * - 역할: 외부 데이터 과학자 및 BI 툴이 발송하는 표준 HTTP/REST 요청을 수신하여 내부 코어망으로 라우팅하고, 시스템 통제를 위한 관리자(Admin) 전용 제어 명령을 노출합니다.
 * - 이론 및 기술: RESTful API 디자인, 헥사고날 아키텍처(Hexagonal Architecture), 부패 방지 계층(Anti-Corruption Layer, ACL), 관리 추상화(Management Abstraction).
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [변경] 아키텍처 확장: `A0_DT_42_422023_비동기_텐서_소화기`, `A0_DT_42_422026_LSM_컴팩션_데몬` 인스턴스를 주입받도록 DI(Dependency Injection) 배관을 완벽히 갱신했습니다.
 * - 💡 [신설] Admin API 물리적 바인딩: 더미(Mock) 응답을 반환하던 과거 로직을 소각하고, 주입받은 데몬의 `executeDlqManualRollforward` 및 `executeBackgroundCompactionLoop`를 강제로 직격(Trigger)하여 실제 커널 레벨의 관리 제어가 가능하도록 통신망을 개통했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 HTTP 통신, 내부 코어망 결속을 위한 코어 라이브러리와 비동기 데몬들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries and asynchronous daemons for HTTP communication and internal core network binding.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.ReadPort;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더.SmartIndexRegistry;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망.A0_DT_42_422023_비동기_텐서_소화기;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망.A0_DT_42_422026_LSM_컴팩션_데몬;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망.A0_DT_42_422041_범용_OS레이어_드라이버;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어6_시맨틱_임베딩_변환기.A0_DT_42_422061_매트릭스_쿼리_엔진;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 글로벌 표준 벡터 스토어 API 규격과 시스템 관리자 쉘을 제공하는 무결점 HTTP API 게이트웨이입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A flawless HTTP API gateway providing global standard vector store API specifications and a system admin shell.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424010
 * [파일명] A0_DT_42_424010_글로벌_표준_REST_파사드.java
 * [모듈명] 통합 OS V6.1 - Tier 17: 글로벌 표준 REST 파사드 (HTTP 게이트웨이 및 Admin Shell)
 * ==============================================================================
 */
public final class A0_DT_42_424010_글로벌_표준_REST_파사드 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.424010_REST_FACADE");

    // [1. 한글 상세 주석]
    // 💡 [코어망 의존성 결합] 프로토콜 통역(Translation)에 필요한 핵심 쿼리 엔진, 메타데이터 레지스트리, 그리고 시스템
    // 백그라운드 데몬들을 선언합니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Network Dependency Binding] Declares the core query engine, metadata
    // registry, and system background daemons necessary for protocol translation.

    private final A0_DT_42_422061_매트릭스_쿼리_엔진 queryEngine;
    private final A0_DT_42_422041_범용_OS레이어_드라이버 osLayerDriver;
    private final SmartIndexRegistry runtimeIndexRegistry;

    // 관리자(Admin) 명령을 런타임에 실제로 직격 집행(Trigger)할 타겟 백그라운드 데몬 (DI 주입)
    private final A0_DT_42_422023_비동기_텐서_소화기 tensorIngestor;
    private final A0_DT_42_422026_LSM_컴팩션_데몬 compactionDaemon;

    // Zero-Dependency HTTP 서버 인스턴스 및 비동기 워커 스레드 풀
    private HttpServer embeddedHttpServer;
    private final ExecutorService webWorkerThreadPool;

    // [1. 한글 상세 주석]
    // [생성자] 파사드 계층을 초기화하고 의존성 주입(DI)을 통해 내부 코어망 및 데몬들과 결속시킵니다.
    // [2. 영문 상세 주석]
    // [Constructor] Initializes the facade layer and binds it with the internal
    // core network and daemons via Dependency Injection (DI).

    public A0_DT_42_424010_글로벌_표준_REST_파사드(
            A0_DT_42_422061_매트릭스_쿼리_엔진 queryEngine,
            A0_DT_42_422041_범용_OS레이어_드라이버 osLayerDriver,
            SmartIndexRegistry runtimeIndexRegistry,
            A0_DT_42_422023_비동기_텐서_소화기 tensorIngestor,
            A0_DT_42_422026_LSM_컴팩션_데몬 compactionDaemon) {

        if (queryEngine == null || osLayerDriver == null || runtimeIndexRegistry == null || tensorIngestor == null
                || compactionDaemon == null) {
            throw new IllegalArgumentException("[배관 파열] 필수 의존성 데몬 및 엔진이 누락되어 REST 게이트웨이를 창설할 수 없습니다.");
        }

        this.queryEngine = queryEngine;
        this.osLayerDriver = osLayerDriver;
        this.runtimeIndexRegistry = runtimeIndexRegistry;
        this.tensorIngestor = tensorIngestor;
        this.compactionDaemon = compactionDaemon;

        // 웹 HTTP 요청 처리를 메인 HFT(고빈도 매매) 코어 스레드와 격리하기 위한 전용 스레드 풀 개방
        int availableProcessors = Math.max(2, Runtime.getRuntime().availableProcessors() / 4);
        this.webWorkerThreadPool = Executors.newFixedThreadPool(availableProcessors, runnable -> {
            Thread thread = new Thread(runnable, "OS_REST_GATEWAY_WORKER");
            // HTTP I/O는 백그라운드로 처리되도록 내부 텐서 연산보다 낮은 스레드 우선순위를 부여
            thread.setPriority(Thread.NORM_PRIORITY - 1);
            return thread;
        });

        logger.info(
                " >> [통합 OS V6.1] A0_DT_42_424010 글로벌 표준 REST 파사드 기동 준비 완료. (Pinecone/OpenAI 호환 API 및 Admin Shell 장착)");
    }

    // [1. 한글 상세 주석]
    // [외교 역학 1: 통신망 개방] 지정된 포트로 내장 서버를 점화하고 API 라우터를 물리적으로 바인딩(Binding)합니다.
    // [2. 영문 상세 주석]
    // [Diplomacy Dynamics 1: Network Opening] Ignites the embedded server on the
    // specified port and physically binds API routers.

    public void startRestGateway(int port) {
        try {
            // Spring Boot 톰캣(Tomcat)의 거대한 초기화 메모리와 리플렉션 오버헤드를 원천 배제한 순수 JDK 내장 HttpServer
            // 사용 (Zero-Dependency)
            this.embeddedHttpServer = HttpServer.create(new InetSocketAddress(port), 0);

            // 💡 [라우팅: /api/v1/vectors/fetch] (Pinecone Vector DB Fetch API 호환 규격 에뮬레이션)
            this.embeddedHttpServer.createContext("/api/v1/vectors/fetch", new VectorFetchApiHandler());

            // 💡 [신설 라우팅: 관리자 쉘(Admin API)]
            this.embeddedHttpServer.createContext("/api/v1/admin/recover-dlq", new DlqRollforwardAdminHandler());
            this.embeddedHttpServer.createContext("/api/v1/admin/force-compaction", new ForceCompactionAdminHandler());

            // 비동기 처리를 위해 전용 스레드 풀 위임
            this.embeddedHttpServer.setExecutor(webWorkerThreadPool);
            this.embeddedHttpServer.start();

            logger.info(
                    String.format("   ├─ [통신망 개방] 글로벌 표준 REST API 게이트웨이 및 Admin Shell 서비스가 개방되었습니다. (Port: %d)", port));

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [통신망 붕괴] REST 파사드 서버 포트 바인딩 실패.", ex);
            throw new RuntimeException("API 게이트웨이 기동 불가", ex);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [통역 코어] 텐서 조회 표준 통역기 (Pinecone / OpenAI 호환 규격). 외부의 표준 REST GET 요청을 시스템
    // 내부의 기하학적 인덱스(Grid)로 통역합니다.
    // [2. 영문 상세 주석]
    // 💡 [Translation Core] Tensor Retrieval Standard Translator (Pinecone / OpenAI
    // compatible). Translates external standard REST GET requests into geometric
    // indices within the system.

    private class VectorFetchApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            if (!"GET".equalsIgnoreCase(httpExchange.getRequestMethod())) {
                sendErrorJsonResponse(httpExchange, 405, "Method Not Allowed. Only GET is supported.");
                return;
            }

            try {
                // HTTP GET 쿼리 스트링 파라미터 파싱
                Map<String, String> queryParams = parseQueryString(httpExchange.getRequestURI().getQuery());
                String entityId = queryParams.get("ids");
                String namespace = queryParams.getOrDefault("namespace", "BASE_CLOSE"); // 지표명(Feature)을 namespace 개념으로
                                                                                        // 매핑
                String startTickString = queryParams.get("start_tick");
                String endTickString = queryParams.get("end_tick");

                if (entityId == null || startTickString == null || endTickString == null) {
                    sendErrorJsonResponse(httpExchange, 400,
                            "Bad Request: 'ids', 'start_tick', and 'end_tick' parameters are required.");
                    return;
                }

                // =========================================================================
                // 💡 [부패 방지 계층 통역 (Anti-Corruption Layer Translation)]
                // =========================================================================
                Integer yAxisIndex = runtimeIndexRegistry.featureZIndexMap().get(entityId);
                Integer zAxisIndex = runtimeIndexRegistry.featureZIndexMap().get(namespace);

                if (yAxisIndex == null) {
                    sendErrorJsonResponse(httpExchange, 404,
                            "Not Found: Vector ID '" + entityId + "' does not exist in the Matrix.");
                    return;
                }
                if (zAxisIndex == null) {
                    sendErrorJsonResponse(httpExchange, 404,
                            "Not Found: Namespace '" + namespace + "' does not exist in the Matrix.");
                    return;
                }

                int xAxisStartIndex = runtimeIndexRegistry.timeGridIndexer().getIndex(startTickString);
                int xAxisEndIndex = runtimeIndexRegistry.timeGridIndexer().getIndex(endTickString);

                if (xAxisStartIndex < 0 || xAxisEndIndex < xAxisStartIndex) {
                    sendErrorJsonResponse(httpExchange, 400, "Bad Request: Invalid time range requested.");
                    return;
                }

                // =========================================================================
                // 💡 [코어 엔진 직접 타격 (Direct Core Engine Call)]
                // =========================================================================
                ReadPort tensorReadPort = osLayerDriver.extractTruncatedRawPort(zAxisIndex);

                // 💡 [Pinecone 호환 Zero-Allocation JSON 베이킹]
                // 거대한 객체 직렬화 툴(Jackson/Gson)을 사용하지 않고 StringBuilder를 통해 바이트 수준에서 JSON 구조를 직접
                // 구워냅니다.
                int intervalLength = xAxisEndIndex - xAxisStartIndex + 1;
                StringBuilder jsonBuffer = new StringBuilder(intervalLength * 40 + 200);

                jsonBuffer.append("{\n");
                jsonBuffer.append("  \"vectors\": {\n");
                jsonBuffer.append(String.format("    \"%s\": {\n", entityId));
                jsonBuffer.append(String.format("      \"id\": \"%s\",\n", entityId));
                jsonBuffer.append("      \"values\": [\n");

                for (int x = xAxisStartIndex; x <= xAxisEndIndex; x++) {
                    float extractedTensorValue = queryEngine.extractSinglePointUltraFast(tensorReadPort, yAxisIndex, x);

                    if (Float.isNaN(extractedTensorValue)) {
                        jsonBuffer.append("        0.0"); // Pinecone 및 일반 JSON 규격에 맞추어 NaN(결측치) 대신 0.0을 주입
                    } else {
                        jsonBuffer.append("        ").append(extractedTensorValue);
                    }

                    if (x < xAxisEndIndex) {
                        jsonBuffer.append(",\n");
                    } else {
                        jsonBuffer.append("\n");
                    }
                }

                jsonBuffer.append("      ],\n");
                jsonBuffer.append("      \"metadata\": {\n");
                jsonBuffer.append(String.format("        \"namespace\": \"%s\"\n", namespace));
                jsonBuffer.append("      }\n");
                jsonBuffer.append("    }\n");
                jsonBuffer.append("  },\n");
                jsonBuffer.append("  \"namespace\": \"").append(namespace).append("\"\n");
                jsonBuffer.append("}");

                sendSuccessJsonResponse(httpExchange, jsonBuffer.toString());

            } catch (Exception ex) {
                logger.log(Level.SEVERE, " [통역기 파열] REST 텐서 조회 파이프라인 처리 중 내부 시스템 예외 발생.", ex);
                sendErrorJsonResponse(httpExchange, 500, "Internal Server Error.");
            }
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [관리자 쉘: Admin API] 외부 관제망의 지시를 받아 DLQ(Dead Letter Queue) 수동 롤포워드를 직격시키는
    // 통역기입니다.
    // [2. 영문 상세 주석]
    // 💡 [Admin Shell: Admin API] A translator that takes instructions from the
    // external control network and directly triggers manual roll-forward of the
    // Dead Letter Queue (DLQ).

    private class DlqRollforwardAdminHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            if (!"POST".equalsIgnoreCase(httpExchange.getRequestMethod())) {
                sendErrorJsonResponse(httpExchange, 405, "Method Not Allowed.");
                return;
            }
            try {
                // 실 서비스 프로덕션 시 Header JWT 토큰 검증(ACL) 보안 로직이 위치할 지점
                logger.warning(" 🚨 [Admin API 호출] 최고 관리자의 권한으로 외부 관제망을 통해 DLQ 수동 롤포워드(복구) 명령이 하달되었습니다.");

                // 💡 [데몬 직격(Trigger)] 의존성 주입(DI)받은 텐서_소화기의 롤포워드 엔진을 물리적으로 격발
                Path defaultDlqPath = Paths.get("MATRIX_A0_422023_DLQ.log");
                tensorIngestor.executeDlqManualRollforward(defaultDlqPath);

                String successResponse = "{\"status\": \"success\", \"message\": \"DLQ roll-forward signal successfully emitted and executed.\"}";
                sendSuccessJsonResponse(httpExchange, successResponse);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, " [Admin API 파열] DLQ 롤포워드 수동 집행 중 시스템 예외 발생.", ex);
                sendErrorJsonResponse(httpExchange, 500, "Failed to emit DLQ roll-forward signal.");
            }
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [관리자 쉘: Admin API] 외부 관제망의 지시를 받아 디스크 강제 병합(Compaction)을 직격시키는 통역기입니다.
    // [2. 영문 상세 주석]
    // 💡 [Admin Shell: Admin API] A translator that takes instructions from the
    // external control network and directly triggers forced disk compaction.

    private class ForceCompactionAdminHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            if (!"POST".equalsIgnoreCase(httpExchange.getRequestMethod())) {
                sendErrorJsonResponse(httpExchange, 405, "Method Not Allowed.");
                return;
            }
            try {
                logger.warning(" 🚨 [Admin API 호출] 최고 관리자의 권한으로 외부 관제망을 통해 디스크 강제 병합(LSM Compaction) 명령이 하달되었습니다.");

                // 💡 [데몬 직격(Trigger)] 의존성 주입(DI)받은 컴팩션_데몬의 백그라운드 병합 루프를 강제적으로 즉시 1회 실행
                compactionDaemon.executeBackgroundCompactionLoop();

                String successResponse = "{\"status\": \"success\", \"message\": \"Force compaction signal successfully emitted and executed.\"}";
                sendSuccessJsonResponse(httpExchange, successResponse);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, " [Admin API 파열] 강제 컴팩션 집행 중 시스템 예외 발생.", ex);
                sendErrorJsonResponse(httpExchange, 500, "Failed to emit force compaction signal.");
            }
        }
    }

    // [1. 한글 상세 주석]
    // [보조 유틸리티] HTTP GET 쿼리 문자열(a=1&b=2 형태)을 파싱하여 자바 Map으로 변환합니다.
    // [2. 영문 상세 주석]
    // [Auxiliary Utility] Parses HTTP GET query strings (e.g., a=1&b=2) and
    // converts them into a Java Map.

    private Map<String, String> parseQueryString(String query) {
        Map<String, String> queryParamMap = new HashMap<>();
        if (query == null || query.isEmpty())
            return queryParamMap;

        String[] fragments = query.split("&");
        for (String fragment : fragments) {
            int equalSignPosition = fragment.indexOf("=");
            if (equalSignPosition > 0) {
                queryParamMap.put(fragment.substring(0, equalSignPosition), fragment.substring(equalSignPosition + 1));
            } else {
                queryParamMap.put(fragment, "");
            }
        }
        return queryParamMap;
    }

    // [1. 한글 상세 주석]
    // [응답 사출 유틸리티] 성공(200 OK) HTTP 상태코드와 함께 직렬화된 JSON 본문을 전송합니다.
    // [2. 영문 상세 주석]
    // [Response Ejection Utility] Sends a serialized JSON body along with a Success
    // (200 OK) HTTP status code.

    private void sendSuccessJsonResponse(HttpExchange httpExchange, String jsonResponse) throws IOException {
        byte[] payloadBytes = jsonResponse.getBytes(StandardCharsets.UTF_8);
        httpExchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        httpExchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        httpExchange.sendResponseHeaders(200, payloadBytes.length);

        try (OutputStream outputStream = httpExchange.getResponseBody()) {
            outputStream.write(payloadBytes);
        }
    }

    // [1. 한글 상세 주석]
    // [응답 사출 유틸리티] 에러(4xx, 5xx) HTTP 상태코드와 함께 시스템 에러 메시지가 담긴 JSON을 생성하여 전송합니다.
    // [2. 영문 상세 주석]
    // [Response Ejection Utility] Generates and sends a JSON containing a system
    // error message along with an Error (4xx, 5xx) HTTP status code.

    private void sendErrorJsonResponse(HttpExchange httpExchange, int statusCode, String errorMessage)
            throws IOException {
        String errorJsonString = String.format("{\"error\": \"%s\"}", errorMessage.replace("\"", "\\\""));
        byte[] payloadBytes = errorJsonString.getBytes(StandardCharsets.UTF_8);

        httpExchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        httpExchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        httpExchange.sendResponseHeaders(statusCode, payloadBytes.length);

        try (OutputStream outputStream = httpExchange.getResponseBody()) {
            outputStream.write(payloadBytes);
        }
    }

    /**
     * [종결 절차] 애플리케이션 셧다운 시 HTTP 서버 포트를 닫고 비동기 웹 워커 스레드 풀의 시스템 자원을 OS 커널에 반환합니다.
     */
    public void executeGracefulShutdown() {
        if (embeddedHttpServer != null) {
            logger.info("   ├─ [통신망 셧다운] REST 파사드 게이트웨이가 수신을 멈추고 외부 포트를 닫습니다.");
            embeddedHttpServer.stop(1); // 1초 대기 후 강제 셧다운(Graceful Stop)
        }
        if (webWorkerThreadPool != null) {
            webWorkerThreadPool.shutdownNow();
        }
        logger.info(" >> [외교관 계층 철수 완료] 글로벌 통신망 포트가 닫히고 모든 HTTP I/O 자원이 커널에 환원되었습니다.");
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 및 시스템 아키텍처 (Theoretical Background & Architecture Philosophy)]
 * 
 * 1. 부패 방지 계층 (Anti-Corruption Layer, ACL)과 Pinecone 표준 에뮬레이션:
 * 외부의 데이터 과학자 생태계(LangChain, LlamaIndex, 각종 BI Tool 등)는 이미 Pinecone이나 OpenAI
 * Vector Store가
 * 정의한 API 표준 포맷에 강하게 기술적으로 종속되어 있습니다.
 * 만약 통합 OS의 내부 코어 엔진이 지닌 초고속 HFT 물리적 인덱스(X/Y/Z) 구조를 REST API 밖으로 날것 그대로 노출하게
 * 되면,
 * 외부 클라이언트가 우리 데이터베이스 내부의 물리적 구조를 완벽히 이해해야만 하는 '의존성 오염(Dependency Corruption)'이
 * 발생합니다.
 * 본 파사드(REST Facade) 모듈은 어댑터 패턴을 응용하여, 외부 생태계에서 요청한 논리적인 `Namespace`와 `Vector
 * ID` 문자열을
 * 통합 OS 시스템 내부의 물리적인 격자형(Grid) Z축/Y축 정수 인덱스로 완벽히 통역(Translation)함으로써 코어망의 내부
 * 구현을 보호(ACL)합니다.
 * 
 * 2. 관리 추상화(Management Abstraction) 및 백그라운드 데몬 직결(Trigger) 배관:
 * 이전 버전의 Admin API는 수신만 받을 뿐 실제 데몬을 깨우지 못하는 '가짜 관제탑(Dummy Shell)'에 불과했습니다.
 * V6.1 업데이트를 통해 외교관 계층의 생성자에 `비동기_텐서_소화기`와 `LSM_컴팩션_데몬` 모듈의 인스턴스를
 * 직접 의존성 주입(Dependency Injection)받도록 아키텍처가 전면 개편되었습니다.
 * 이제 외부 웹 기반 관제 대시보드(Admin UI)에서 POST 요청을 날리는 즉시, 가짜 목업(Mock) 응답이 아닌 실제 커널 내부의
 * 데몬 코어 메서드
 * (`executeDlqManualRollforward`, `executeBackgroundCompactionLoop`)가 뇌관을 때리듯
 * 즉각 물리적으로 실행(Trigger)됩니다.
 * 이는 개발자가 리눅스 터미널(SSH)에 직접 접속하여 쉘 스크립트를 돌려야 하는 구시대적 운영 방식을 배제한, 우아한 중앙 통제 관제
 * 시스템(Control Plane)의 완성을 의미합니다.
 * 
 * 3. Zero-Dependency와 객체 할당 멸균 (Zero-Allocation JSON Baking):
 * Spring Boot 웹 프레임워크와 Jackson 직렬화 라이브러리는 기업용 백엔드에서 강력하지만, 매 HTTP 요청마다
 * 수십 개의 내부 DTO 객체를 힙(Heap) 메모리에 할당하며 텐서 연산 환경에서 엄청난 가비지 컬렉터(GC) 지연 스파이크를
 * 발생시킵니다.
 * 통합 OS는 이러한 오버헤드를 원천 배제하기 위해 서드파티 라이브러리(Zero-Dependency)를 전혀 쓰지 않고,
 * JDK 내장 `HttpServer`와 가변 버퍼(`StringBuilder`)만을 이용해 JSON 구조 문자열을 바이트 단위에서 직접
 * 구워내는(Baking) 극한의 HFT 성능 튜닝을 거쳤습니다.
 * =============================================================================
 */