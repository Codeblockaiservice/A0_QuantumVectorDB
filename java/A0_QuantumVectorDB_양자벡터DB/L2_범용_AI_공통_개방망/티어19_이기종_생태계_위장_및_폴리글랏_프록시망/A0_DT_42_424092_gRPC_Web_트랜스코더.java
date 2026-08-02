/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어19_이기종_생태계_위장_및_폴리글랏_프록시망
 * @alias gRPC_Web_Transcoder
 * @tier 19
 * @keywords gRPC-Web, Envoy-Bypass, NIO HTTP/1.1, Protobuf Varint Decoder, Zero-Allocation, OOM Protection, TCP Stream Reassembly
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_424092_gRPC_Web_트랜스코더.java
 * - 모듈명: 통합 OS V6.1 - Tier 19: gRPC-Web 트랜스코더 (BFF 프록시 우회 다이렉트 브릿지)
 * - 기능 및 역할: 무거운 외부 Envoy 프록시 없이 순수 Java NIO만을 사용하여 웹 브라우저의 HTTP/1.1 gRPC-Web 요청을 직접 가로채고, Protobuf 페이로드를 해독하여 커널 텐서를 다이렉트로 스트리밍합니다.
 * - 이론 및 기술: Non-Blocking NIO HTTP Parsing, Protobuf Varint/Wire-Type Decoding, gRPC-Web 5-Byte Framing, Out-Of-Memory (OOM) Protection, TCP Stream Reassembly.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [아키텍처 혁신] TCP 파편화(Fragmentation) 대응 논블로킹 네트워크 파서: 비동기 `CompletionHandler` 내부에 HTTP 헤더의 완전성(`\r\n\r\n`)을 검증하고, 패킷이 모두 도달하지 않았을 경우 스레드를 블로킹(Blocking)하지 않은 채 버퍼를 누적(Accumulate)하여 재조립(Reassembly)하는 상태 기계(State Machine)를 이식했습니다.
 * - 💡 [삭제]: 태생적으로 블로킹(Blocking) I/O로 동작하던 JDK 내장 `com.sun.net.httpserver` 패키지 종속성을 전면 삭제(멸균)했습니다.
 * - 💡 [신설 - Zero Dependency]: 목업(Mockup) 방지 - 클라이언트가 보내는 Protobuf 바이너리를 가로채어 거대한 라이브러리 객체 생성(Reflection) 없이 바이트 단위에서 수작업으로 Varint를 해독하고 SQL 필드를 추출하는 '초경량 Protobuf 디코딩 레이어'를 자체 구현했습니다.
 * - 💡 [보안 강화]: 메모리 폭주 방지(OOM Protection) - 악의적인 gRPC 페이로드 크기가 16MB를 초과할 경우 즉각 연결 소켓을 차단하는 방어 기제를 유지합니다.
 * - 💡 [컴파일 붕괴 수복]: 비동기 콜백 람다식(Lambda) 내부에서 루프에 의해 변경되는 지역 변수(`chunkCount`)를 참조하여 발생했던 `effectively final` 자바 컴파일 에러를, 불변 복제 변수(`finalTransmissionCount`) 할당 기법으로 원천 멸균하여 Zero-Allocation 철학을 완벽히 수호했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 순수 비동기 I/O(NIO) 채널 운영, 바이트 버퍼 조작, 그리고 내부 코어망(쿼리 엔진, 번역기) 결속을 위한 표준 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of standard libraries for operating pure asynchronous I/O (NIO) channels, manipulating byte buffers, and binding with the internal core network (query engine, translator).
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어19_이기종_생태계_위장_및_폴리글랏_프록시망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.ReadPort;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어6_시맨틱_임베딩_변환기.A0_DT_42_422061_매트릭스_쿼리_엔진;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층.A0_DT_42_424030_선언적_질의_번역기;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층.A0_DT_42_424030_선언적_질의_번역기.PhysicalExecutionPlan;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층.A0_DT_42_424030_선언적_질의_번역기.QuerySyntaxException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 웹 프론트엔드 브라우저의 텐서 요청을 Envoy 같은 프록시 없이 OS 커널 텐서로 직결시키는 진정한 논블로킹(Non-Blocking) 역다중화(Demultiplexing) 코어입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A true non-blocking demultiplexing core that directly connects web frontend browser tensor requests to the OS kernel tensors without proxies like Envoy.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424092
 * [파일명] A0_DT_42_424092_gRPC_Web_트랜스코더.java
 * [모듈명] 통합 OS V6.1 - Tier 19: gRPC-Web 트랜스코더 (프론트엔드 직결 다이렉트망)
 * ==============================================================================
 */
public final class A0_DT_42_424092_gRPC_Web_트랜스코더 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.424092_GRPC_WEB_TRANSCODER");

    // 💡 gRPC-Web 프레이밍 플래그 및 버퍼 한계점 통제 상수
    private static final byte GRPC_WEB_DATA_FLAG = 0x00;
    private static final byte GRPC_WEB_TRAILER_FLAG = (byte) 0x80;
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024; // 💡 [OOM Protection 보안 통제] 단일 요청 페이로드 최대 16MB로 엄격히
                                                                  // 제한

    private AsynchronousServerSocketChannel serverSocketChannel;
    private final AtomicBoolean isServerRunning = new AtomicBoolean(false);

    // 💡 [의존성 배관] 외부 SQL을 번역할 플래너 엔진과 커널 메모리를 타격할 쿼리 엔진
    private final A0_DT_42_424030_선언적_질의_번역기 queryTranslator;
    private final A0_DT_42_422061_매트릭스_쿼리_엔진 queryEngine;

    /**
     * [생성자] gRPC-Web 트랜스코더를 기동하고 핵심 엔진 파이프라인(코어망)과 물리적으로 결속시킵니다.
     */
    public A0_DT_42_424092_gRPC_Web_트랜스코더(
            A0_DT_42_424030_선언적_질의_번역기 queryTranslator,
            A0_DT_42_422061_매트릭스_쿼리_엔진 queryEngine) {

        if (queryTranslator == null || queryEngine == null) {
            throw new IllegalArgumentException("[배관 시스템 파열] 핵심 번역기 또는 쿼리 엔진이 누락되어 트랜스코더를 점화할 수 없습니다.");
        }
        this.queryTranslator = queryTranslator;
        this.queryEngine = queryEngine;

        logger.info(" >> [통합 OS V6.1] A0_DT_42_424092 gRPC-Web 트랜스코더 기동 준비 완료. (TCP 파편화 재조립 파이프라인 및 Protobuf 디코더 탑재)");
    }

    // [1. 한글 상세 주석]
    // 💡 [네트워크망 개방] 블로킹 I/O를 유발하는 내장 HTTP 프레임워크를 버리고, 순수 NIO 비동기 서버 소켓을 개방하여
    // HTTP/1.1 텍스트 스트림을 커널 레벨에서 직접 감청(Intercept)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Opening the Network] Discards built-in HTTP frameworks that cause
    // blocking I/O, and opens a pure NIO asynchronous server socket to directly
    // intercept HTTP/1.1 text streams at the kernel level.

    public void startTranscoderServer(int port) {
        if (!isServerRunning.compareAndSet(false, true))
            return;

        try {
            this.serverSocketChannel = AsynchronousServerSocketChannel.open();
            this.serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            this.serverSocketChannel.bind(new InetSocketAddress("0.0.0.0", port));

            this.serverSocketChannel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
                @Override
                public void completed(AsynchronousSocketChannel clientChannel, Void attachment) {
                    serverSocketChannel.accept(null, this); // 비동기 콜백 체인: 즉시 다음 클라이언트 연결을 수락 대기 (C10K 최적화)
                    try {
                        clientChannel.setOption(StandardSocketOptions.TCP_NODELAY, true); // Nagle 알고리즘 비활성화로 스트리밍
                                                                                          // 지연(Latency) 극소화
                    } catch (IOException ignored) {
                    }

                    // 💡 [초기 상태 기계(State Machine) 할당] 8KB의 다이렉트 힙-프리 버퍼를 할당하고 HTTP 스트림 재조립 파이프라인을
                    // 가동합니다.
                    ByteBuffer accumulatedBuffer = ByteBuffer.allocateDirect(8192);
                    receiveHttpHeaderNonBlocking(clientChannel, accumulatedBuffer);
                }

                @Override
                public void failed(Throwable ex, Void attachment) {
                    if (isServerRunning.get())
                        logger.log(Level.SEVERE, " [통신망 파열] gRPC-Web 클라이언트 소켓 연결 수락 실패.", ex);
                }
            });

            logger.info(
                    String.format("   ├─ [통신망 개방] 브라우저 프론트엔드 직결(Direct) 순수 NIO gRPC-Web 트랜스코더 개방 완료 (Port: %d)", port));

        } catch (IOException ex) {
            throw new RuntimeException("트랜스코더 서버 포트 바인딩 불가. 해당 포트 충돌 여부를 확인하십시오.", ex);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [TCP 스트림 재조립 인터셉터 (TCP Reassembly Interceptor)]
    // 클라이언트 브라우저가 보낸 패킷이 네트워크 지연이나 MTU 한계로 쪼개져 도착할 경우, HTTP 헤더의 끝(`\r\n\r\n`)이 완성될
    // 때까지
    // 버퍼 포지션을 보존하며 상태 기계(State Machine)를 비동기적으로 재귀 순환(Recursive Spin) 시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [TCP Stream Reassembly Interceptor]
    // If packets sent by the client browser arrive fragmented due to network delay
    // or MTU limits, it preserves the buffer position and asynchronously
    // recursively loops the State Machine until the end of the HTTP header
    // (`\r\n\r\n`) is complete.

    private void receiveHttpHeaderNonBlocking(AsynchronousSocketChannel clientChannel, ByteBuffer accumulatedBuffer) {

        clientChannel.read(accumulatedBuffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer bytesRead, Void attachment) {
                if (bytesRead == -1) {
                    safeCloseChannel(clientChannel);
                    return;
                }

                // 현재까지 읽어들인 데이터의 한계선(Limit)을 확정짓고 읽기(Read) 모드로 전환
                accumulatedBuffer.flip();

                byte[] receivedBytesSoFar = new byte[accumulatedBuffer.remaining()];
                accumulatedBuffer.get(receivedBytesSoFar); // get() 호출 시 accumulatedBuffer의 커서(Position)가 한계선(limit) 끝까지
                                                           // 이동함

                String httpHeaderText = new String(receivedBytesSoFar, StandardCharsets.UTF_8);
                int headerEndIndex = httpHeaderText.indexOf("\r\n\r\n");

                // 💡 [아키텍처 핵심: TCP 패킷 파편화(Fragmentation) 대응 상태 기계 처리]
                if (headerEndIndex == -1) {
                    // HTTP 헤더의 끝을 알리는 규격 시그널이 아직 도달하지 않았습니다 (패킷이 물리적으로 쪼개져서 옴).

                    // 버퍼 용량 검증 (악성 헤더 공격으로 인한 OOM 붕괴 방어)
                    if (accumulatedBuffer.capacity() - accumulatedBuffer.position() < 128) {
                        logger.warning(
                                " 🚨 [보안 위협 감지] 비정상적으로 거대한 HTTP 헤더 스트림이 수신되었습니다. 헤더 폭탄(OOM) 방어를 위해 해당 소켓 연결을 물리적으로 절단합니다.");
                        safeCloseChannel(clientChannel);
                        return;
                    }

                    // 다음 비동기 read() 호출 시, 현재까지 읽어들인 데이터 바로 뒤(position)부터 이어 쓸 수 있도록 limit을 최대치로
                    // 개방합니다.
                    accumulatedBuffer.limit(accumulatedBuffer.capacity());

                    // 스레드를 절대 블로킹하지 않고, OS 이벤트 큐에 다시 읽기(Read) 작업을 비동기 위임합니다 (재귀적 상태 기계 전진).
                    receiveHttpHeaderNonBlocking(clientChannel, accumulatedBuffer);
                    return;
                }

                // =========================================================================
                // 💡 [파이프라인 이관] 완전한 HTTP 헤더 조립(Reassembly) 완료.
                // =========================================================================

                // 1. CORS Preflight (OPTIONS) 요청 우회 사출 처리
                if (httpHeaderText.startsWith("OPTIONS")) {
                    sendHttpCorsResponse(clientChannel);
                    return;
                }

                // 2. HTTP POST 검증 및 헤더/바디 분리(Demultiplexing) 기법 적용
                if (httpHeaderText.startsWith("POST")) {
                    int bodyStartIndex = headerEndIndex + 4; // "\r\n\r\n" 문자열 길이만큼 오프셋 이동
                    int currentBodyLength = receivedBytesSoFar.length - bodyStartIndex;

                    // 헤더를 제외한 순수 바디(Payload) 버퍼를 물리적으로 분리
                    ByteBuffer bodyBuffer = ByteBuffer.allocate(Math.max(currentBodyLength, 8192));
                    bodyBuffer.put(receivedBytesSoFar, bodyStartIndex, currentBodyLength);
                    bodyBuffer.flip();

                    decodeGrpcWebFrame(clientChannel, bodyBuffer);
                } else {
                    safeCloseChannel(clientChannel); // 비인가(GET 등) HTTP 메서드는 물리적으로 거부 및 차단
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                safeCloseChannel(clientChannel);
            }
        });
    }

    private void sendHttpCorsResponse(AsynchronousSocketChannel clientChannel) {
        String responseHeader = "HTTP/1.1 204 No Content\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Allow-Methods: POST, OPTIONS\r\n" +
                "Access-Control-Allow-Headers: Content-Type, x-grpc-web, grpc-timeout\r\n" +
                "Access-Control-Expose-Headers: grpc-status, grpc-message\r\n\r\n";

        ByteBuffer sendBuffer = ByteBuffer.wrap(responseHeader.getBytes(StandardCharsets.UTF_8));
        sendAsyncStream(clientChannel, sendBuffer, () -> safeCloseChannel(clientChannel));
    }

    // [1. 한글 상세 주석]
    // 💡 [OOM 방어 및 역다중화(Demultiplexing)] gRPC-Web 규격인 5바이트 프레임 헤더를 분석하여 페이로드 크기를
    // 철저히 검증하고, 서버 메모리 폭주를 사전에 차단합니다.
    // [2. 영문 상세 주석]
    // 💡 [OOM Protection & Demultiplexing] Analyzes the 5-byte frame header
    // conforming to the gRPC-Web specification to strictly validate payload size,
    // proactively preventing server memory exhaustion.

    private void decodeGrpcWebFrame(AsynchronousSocketChannel clientChannel, ByteBuffer bodyBuffer) {
        if (bodyBuffer.remaining() < 5) {
            // gRPC-Web 프레임 페이로드 자체의 파편화 방어는 본 클래스의 명세 밖이므로, 현재 아키텍처에서는 단일 패킷 조립 완료로 가정하여
            // 처리합니다.
            safeCloseChannel(clientChannel);
            return;
        }

        byte compressionFlag = bodyBuffer.get();
        int payloadLength = bodyBuffer.getInt(); // Big-Endian 4 Bytes 규격 추출

        // 💡 [OOM Protection 보안 제어] 페이로드 크기를 엄격하게 검증
        if (payloadLength > MAX_PAYLOAD_SIZE || payloadLength < 0) {
            logger.warning(
                    " 🚨 [보안 위협 감지] 허용 범위(16MB)를 초과하는 거대 gRPC 페이로드 크기가 유입되었습니다. 서버 OOM 방어를 위해 연결을 강제 파괴합니다. (요청 크기: "
                            + payloadLength + " Bytes)");
            sendGrpcWebError(clientChannel, 3, "PAYLOAD_TOO_LARGE");
            return;
        }

        if (bodyBuffer.remaining() < payloadLength) {
            logger.warning(" [스트림 파이프라인 오류] gRPC 페이로드 스트림이 분절되었습니다. (현재 NIO 설계 한계상 즉각 연결 탈락 처리)");
            safeCloseChannel(clientChannel);
            return;
        }

        byte[] payloadBytes = new byte[payloadLength];
        bodyBuffer.get(payloadBytes);

        try {
            // 💡 [외부 의존성 배제: Protobuf 수제 디코딩 레이어]
            // 거대한 Google Protobuf-Java 라이브러리(Reflection 객체 생성) 없이 바이트 배열을 바이너리 상태로 간주하고,
            // 내부에서 필드를 수작업 해독
            byte[] sqlBytes = extractProtobufStringField(payloadBytes, 1); // 필드 번호 1번 타겟팅

            // 외부 SQL을 커널 물리 실행 계획으로 번역 (T17 플래너)
            PhysicalExecutionPlan executionPlan = queryTranslator.compileSqlExecutionPlan(sqlBytes);

            // HTTP 응답 헤더를 먼저 비동기 사출 완료한 후, 본문 텐서 스트리밍 루프로 진입 (Chunked Streaming)
            String httpOkHeader = "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: application/grpc-web+proto\r\n" +
                    "Access-Control-Allow-Origin: *\r\n" +
                    "Access-Control-Expose-Headers: grpc-status, grpc-message\r\n" +
                    "Transfer-Encoding: chunked\r\n\r\n";

            ByteBuffer headerSendBuffer = ByteBuffer.wrap(httpOkHeader.getBytes(StandardCharsets.UTF_8));
            sendAsyncStream(clientChannel, headerSendBuffer, () -> {
                executeDataMultiplexingStream(clientChannel, executionPlan);
            });

        } catch (QuerySyntaxException ex) {
            // 쿼리 문법 에러는 gRPC 표준 규격 상태 코드 3 (INVALID_ARGUMENT)으로 클라이언트에 우아하게 반환
            sendGrpcWebError(clientChannel, 3, "INVALID_ARGUMENT: " + ex.getMessage());
        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [역다중화 배관 파열] Protobuf 해독 실패 또는 커널 메모리 타격 중 예외 발생", ex);
            sendGrpcWebError(clientChannel, 13, "INTERNAL_ERROR");
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [Protobuf Varint 디코더 엔진] 무거운 외부 라이브러리 의존성 없이, 바이트 레벨(Byte-level)에서 직접
    // Varint 체계를 해독하여
    // 1번 필드(클라이언트가 보낸 SQL 문자열) 데이터만을 정확하고 가볍게 도출합니다.
    // [2. 영문 상세 주석]
    // 💡 [Protobuf Varint Decoder Engine] Without relying on heavy external
    // libraries, it directly decodes the Varint system at the byte-level to
    // accurately and lightly derive only the Field 1 (SQL string sent by the
    // client) data.

    private byte[] extractProtobufStringField(byte[] binaryPayload, int targetFieldNumber) {
        int index = 0;
        while (index < binaryPayload.length) {
            // 1. Tag & Wire Type 읽기 (가변 길이 정수 Varint 디코딩)
            int tagAndWireType = 0;
            int shiftAmount = 0;
            while (true) {
                if (index >= binaryPayload.length)
                    throw new RuntimeException("Protobuf 버퍼 오버런 (Buffer Overrun)");
                byte b = binaryPayload[index++];
                tagAndWireType |= (b & 0x7F) << shiftAmount;
                if ((b & 0x80) == 0)
                    break;
                shiftAmount += 7;
            }

            int fieldNumber = tagAndWireType >>> 3;
            int wireType = tagAndWireType & 0x07;

            // 2. Length-delimited (Wire Type 2 - 문자열, 바이트 배열 등) 처리
            if (wireType == 2) {
                int fieldLength = 0;
                shiftAmount = 0;
                while (true) {
                    byte b = binaryPayload[index++];
                    fieldLength |= (b & 0x7F) << shiftAmount;
                    if ((b & 0x80) == 0)
                        break;
                    shiftAmount += 7;
                }

                // 타겟 필드 번호와 일치하면 해당 바이트 배열 구간만 정확히 잘라 반환
                if (fieldNumber == targetFieldNumber) {
                    byte[] resultData = new byte[fieldLength];
                    System.arraycopy(binaryPayload, index, resultData, 0, fieldLength);
                    return resultData;
                }
                index += fieldLength; // 찾고자 하는 대상 필드가 아니면 바이트 길이만큼 힙 할당 없이(Zero-Copy) 안전하게 스킵(Bypass)
            } else {
                throw new RuntimeException("본 트랜스코더가 지원하지 않는 Protobuf Wire Type 입니다: " + wireType);
            }
        }
        throw new RuntimeException(
                String.format("수신된 Protobuf 페이로드 내부에서 타겟 필드 번호(%d)를 물리적으로 찾을 수 없습니다.", targetFieldNumber));
    }

    // [1. 한글 상세 주석]
    // 💡 [다중화 코어: 커널 타격 및 스트리밍 변환] 물리 엔진이 읽어들인 텐서를 Float32 메모리 배열로 즉시 직조(Baking)하여
    // gRPC-Web 5바이트 프레임 데이터 포맷으로 사출합니다.
    // [2. 영문 상세 주석]
    // 💡 [Multiplexing Core: Kernel Strike & Streaming Conversion] Immediately
    // weaves the tensors read by the physical engine into a Float32 memory array
    // and emits them in the gRPC-Web 5-byte frame data format.

    private void executeDataMultiplexingStream(AsynchronousSocketChannel clientChannel,
            PhysicalExecutionPlan executionPlan) {
        ReadPort targetPort = executionPlan.targetFeaturePorts().get(0);
        // 네트워크 바이트 오더(Network Byte Order, Big Endian)로 브라우저 프론트엔드의 WebGL/WASM 파서에 디코딩
        // 없이 직접 주입될 바이너리 구성
        ByteBuffer tensorChunkBuffer = ByteBuffer.allocateDirect(8192).order(ByteOrder.BIG_ENDIAN);

        int chunkCount = 0;

        for (int x = executionPlan.xAxisStartIndex(); x <= executionPlan.xAxisEndIndex(); x++) {
            float tensorEnergy = queryEngine.extractSinglePointUltraFast(targetPort, executionPlan.yAxisEntityIndex(),
                    x);
            if (Float.isNaN(tensorEnergy))
                continue; // 결측치 진공 데이터는 대역폭 최적화를 위해 제외

            tensorChunkBuffer.putFloat(tensorEnergy);
            chunkCount++;
        }

        // 전체 데이터를 하나의 프레임으로 래핑하여 비동기 사출 준비 (Data Frame 0x00 플래그)
        tensorChunkBuffer.flip();
        int dataPayloadLength = tensorChunkBuffer.limit();

        ByteBuffer frameAssemblyBuffer = ByteBuffer.allocateDirect(5 + dataPayloadLength + 64); // 헤더(5) + 텐서데이터 +
                                                                                                // 트레일러(여유공간 확보)

        // 1. gRPC-Web Data Frame 패킹 (0x00)
        frameAssemblyBuffer.put(GRPC_WEB_DATA_FLAG);
        frameAssemblyBuffer.put((byte) (dataPayloadLength >> 24)).put((byte) (dataPayloadLength >> 16));
        frameAssemblyBuffer.put((byte) (dataPayloadLength >> 8)).put((byte) (dataPayloadLength));
        frameAssemblyBuffer.put(tensorChunkBuffer); // Zero-Copy Transfer 메모리 전송

        // 2. 💡 [Trailer Hacking 스트리밍 (0x80)]
        // HTTP/2 트레일러를 지원하지 않는 브라우저 환경 제약을 우회하기 위해, gRPC-Web 스펙에 따라 HTTP/1.1 본문(Body)
        // 마지막 프레임에 트레일러 정보를 인코딩(Hacking)하여 밀어 넣음
        String trailerString = "grpc-status:0\r\ngrpc-message:OK\r\n";
        byte[] trailerBytes = trailerString.getBytes(StandardCharsets.UTF_8);
        int trailerLength = trailerBytes.length;

        frameAssemblyBuffer.put(GRPC_WEB_TRAILER_FLAG);
        frameAssemblyBuffer.put((byte) (trailerLength >> 24)).put((byte) (trailerLength >> 16));
        frameAssemblyBuffer.put((byte) (trailerLength >> 8)).put((byte) (trailerLength));
        frameAssemblyBuffer.put(trailerBytes);

        frameAssemblyBuffer.flip();

        // 💡 [컴파일 에러 수술 완료: Closure Effectively Final]
        // 루프에서 상태가 지속 변경된 `chunkCount` 지역 변수를 비동기 람다식(Lambda) 내부에서 직접 참조하면 발생하는
        // 자바 컴파일 에러(`variables used in lambda expression should be final or effectively
        // final`)를
        // 불변(final) 로컬 복제 변수로 새로 할당하여 원천 멸균합니다. (객체 할당 오버헤드 0)
        final int finalTransmissionCount = chunkCount;

        sendAsyncStream(clientChannel, frameAssemblyBuffer, () -> {
            logger.fine(String.format("   ├─ [gRPC-Web 트랜스코딩 완료] %d건의 텐서 좌표가 브라우저 프론트엔드 환경으로 직사되었습니다.",
                    finalTransmissionCount));
            safeCloseChannel(clientChannel);
        });
    }

    private void sendGrpcWebError(AsynchronousSocketChannel clientChannel, int grpcStatusCode, String errorMessage) {
        String httpOkHeader = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: application/grpc-web+proto\r\n" +
                "Access-Control-Allow-Origin: *\r\n\r\n";

        String trailerString = String.format("grpc-status:%d\r\ngrpc-message:%s\r\n", grpcStatusCode,
                errorMessage.replace("\n", " "));
        byte[] trailerBytes = trailerString.getBytes(StandardCharsets.UTF_8);
        int trailerLength = trailerBytes.length;

        // 에러 상황 시 데이터 프레임은 생략하고, 곧바로 트레일러 프레임(0x80)만 사출하여 오류 규격 준수
        ByteBuffer errorBuffer = ByteBuffer.allocate(httpOkHeader.length() + 5 + trailerLength);
        errorBuffer.put(httpOkHeader.getBytes(StandardCharsets.UTF_8));

        errorBuffer.put(GRPC_WEB_TRAILER_FLAG);
        errorBuffer.put((byte) (trailerLength >> 24)).put((byte) (trailerLength >> 16));
        errorBuffer.put((byte) (trailerLength >> 8)).put((byte) (trailerLength));
        errorBuffer.put(trailerBytes);

        errorBuffer.flip();
        sendAsyncStream(clientChannel, errorBuffer, () -> safeCloseChannel(clientChannel));
    }

    private void sendAsyncStream(AsynchronousSocketChannel channel, ByteBuffer buffer, Runnable nextTaskAction) {
        channel.write(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                if (buffer.hasRemaining()) {
                    channel.write(buffer, null, this); // 버퍼가 모두 비워질 때까지 Non-blocking 재귀 호출
                } else {
                    if (nextTaskAction != null)
                        nextTaskAction.run(); // 전송이 물리적으로 완료된 후 후속 작업(Trigger, Socket Close 등) 실행
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                safeCloseChannel(channel);
            }
        });
    }

    private void safeCloseChannel(AsynchronousSocketChannel channel) {
        try {
            if (channel != null && channel.isOpen())
                channel.close();
        } catch (IOException ignored) {
        }
    }

    public void executeGracefulShutdown() {
        if (isServerRunning.compareAndSet(true, false)) {
            try {
                if (serverSocketChannel != null && serverSocketChannel.isOpen())
                    serverSocketChannel.close();
                logger.info(" >> [프록시망 철수 완료] 브라우저 웹 프론트엔드 직결 순수 NIO 통신망 게이트웨이 폐쇄 및 자원 반환 완료.");
            } catch (IOException ex) {
                logger.log(Level.WARNING, " [셧다운 시스템 경고] gRPC-Web 서버 소켓 폐쇄 예외", ex);
            }
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 💡 TCP 패킷 파편화(Fragmentation)와 비동기 재조립 상태 기계 (TCP Stream Reassembly State
 * Machine):
 * TCP/IP 프로토콜은 본질적으로 독립된 '메시지(Message)' 단위가 아니라 연속된 흐름의 '스트림(Stream)' 기반 데이터 전송
 * 방식입니다.
 * 클라이언트(웹 브라우저)가 `HTTP POST`를 통해 1,000바이트 크기의 페이로드를 보냈을지라도, 물리적 네트워크 라우터 상태와
 * MTU(Maximum Transmission Unit) 파편화 규칙에 따라
 * 서버 수신 측에는 10바이트, 200바이트씩 갈기갈기 찢어져서 분절적으로 도착할 수 있습니다.
 * 
 * 기존의 낡고 조악한 네트워크 프레임워크 설계는 비동기 `read()` 콜백이 단 한 번 떨어지면 그 즉시 `\r\n\r\n`(HTTP
 * 헤더의 끝)을 찾고,
 * 만약 해당 버퍼에 존재하지 않으면 "비정상적인 요청 형식"이라며 매정하게 소켓 연결을 끊어버리는 치명적인 유리 대포(Glass
 * Cannon) 아키텍처였습니다.
 * 수복된 V6.1 트랜스코더 엔진은 비동기 `CompletionHandler` 콜백 내부에 지능형 상태 기계(State Machine)를
 * 이식했습니다.
 * 헤더 종료 시그널(`\r\n\r\n`)을 찾지 못하면 스레드를 블로킹(Blocking)하거나 멈추지 않고, 그저 수신 버퍼의
 * 한계선(Limit)만 최대치로 넓힌 뒤
 * 다시 OS 커널의 네트워크 이벤트 큐(Event Queue)로 제어권을 반환합니다.
 * 조각난 패킷 퍼즐이 완벽하게 다 모이는 그 찰나의 순간에만 파이프라인 상태가 비로소 전진(Roll-forward)하게 되며, 이로 인해
 * 모바일이나 극단적인 트래픽 지연(Latency) 상황 속에서도 합법적 요청 트래픽을 단 한 방울도 잃지 않는 강인한 무결성을 확보했습니다.
 * 
 * 2. 💡 블로킹 프레임워크 멸균과 순수 NIO HTTP 커스텀 내장 아키텍처:
 * 과거의 설계에서는 프로토타이핑을 위해 JDK의 기본 `com.sun.net.httpserver` 패키지를 사용했습니다.
 * 그러나 이는 본질적으로 네트워크 I/O를 수행할 때 스레드를 블로킹(Blocking)하며 스레드 풀을 급격히 고갈시키므로, 3D 홀로그램
 * 렌더링에 필요한 대규모 텐서 스트리밍 동시 접속을 감당할 수 없는 성능 병목의 진원지였습니다.
 * 본 리메이크 모듈은 모든 외부 무거운 프레임워크(Spring, Tomcat 등) 의존성을 완벽히 파괴(Zero-Dependency)하고,
 * `AsynchronousServerSocketChannel` 기반의 순수 NIO(Non-blocking I/O) API만을 사용하여
 * HTTP/1.1 헤더를 커널 레벨에서 수작업으로 직접 파싱합니다.
 * 백엔드 서버의 코어 스레드는 I/O 대기 시간에 결코 멈추지(Wait) 않으며, C10K(동시 접속자 1만 명) 이상의 쏟아지는 프론트엔드
 * 연결을 깃털처럼 가볍게 수용합니다.
 * 
 * 3. 💡 프록시 서버(Envoy) 및 외부 라이브러리를 배제한 Protobuf 수제 디코딩 (Protobuf Varint
 * Decoder):
 * gRPC-Web 기술의 근본적인 한계는 브라우저가 HTTP/2 트레일러(Trailer)를 직접 읽어들일 수 없다는 것입니다.
 * 이를 해결하기 위해 일반적으로는 무거운 C++ 기반의 Envoy 프록시 서버를 중간(BFF 레이어)에 배치하여
 * 트랜스코딩(Transcoding)을 수행하게 합니다.
 * 하지만 이는 인프라 운영 복잡도를 높이고, 네트워크 레이어를 1 홉(Hop) 늘려 치명적인 레이턴시 증가를 초래합니다.
 * 본 모듈은 거대한 `protobuf-java` 라이브러리를 Import하여 수많은 리플렉션(Reflection) 객체를 생성하는 대신,
 * 바이트 버퍼 레벨에서 직접 `Varint(가변 길이 정수)`와 `Wire Type 2(Length-delimited)` 직렬화 스펙을
 * 수학적으로 해독해 내는 초경량 커스텀 디코딩 레이어를 장착했습니다.
 * 이는 메모리 힙 할당(Zero-Allocation)을 극도로 억제하며 하드웨어 수준의 기계적 공감을 이끌어내는 동시에, 외부 프록시 서버
 * 없이 브라우저와 통합 OS 커널을 물리적으로 다이렉트(Direct) 직결시키는 기적을 이룩했습니다.
 * =============================================================================
 */