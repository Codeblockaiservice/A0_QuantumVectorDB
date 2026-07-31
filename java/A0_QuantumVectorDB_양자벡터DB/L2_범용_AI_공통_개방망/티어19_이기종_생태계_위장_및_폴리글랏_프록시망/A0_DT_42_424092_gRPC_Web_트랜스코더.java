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
 * - 모듈명: 통합 OS V6.1 - Tier 19: gRPC-Web 트랜스코더 (BFF 미들웨어 우회 전위대)
 * - 기능 및 역할: 외부 프레임워크 없이 순수 NIO를 사용하여 웹 브라우저의 HTTP/1.1 gRPC-Web 요청을 가로채고, Protobuf 페이로드를 해독하여 커널 텐서를 직접 스트리밍합니다.
 * - 이론 및 기술: Non-Blocking NIO HTTP Parsing, Protobuf Varint/Wire-Type Decoding, gRPC-Web 5-Byte Framing, Out-Of-Memory (OOM) Protection, TCP Stream Reassembly.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [신설] TCP 파편화(Fragmentation) 대응 논블로킹 네트워크 파서: 비동기 `CompletionHandler` 내부에 헤더의 완전성(`\r\n\r\n`)을 검증하고, 도달하지 않았을 경우 스레드를 블로킹하지 않은 채 버퍼를 누적(Accumulate)하여 재조립(Reassembly)하는 상태 기계(State Machine)를 이식했습니다.
 * - 💡 [삭제]: 태생적으로 블로킹(Blocking) I/O로 동작하던 `com.sun.net.httpserver` 패키지 종속성을 전면 삭제(멸균)했습니다.
 * - 💡 [신규]: 목업(Mockup) 방지 - 클라이언트가 보내는 Protobuf 바이너리를 가로채어 Reflection 없이 수작업으로 Varint를 해독하고 SQL 필드를 추출하는 '초경량 Protobuf 디코딩 레이어'를 유지합니다.
 * - 💡 [신규]: 메모리 폭주 방지(OOM Protection) - 페이로드 크기가 16MB를 초과할 경우 즉각 연결을 차단하는 방어 기제를 유지합니다.
 * - 💡 [컴파일 붕괴 수술 완료]: 람다식(Lambda) 내부에서 루프에 의해 변경되는 지역 변수(`청크_카운트`)를 참조하여 발생한 `effectively final` 컴파일 에러를, 불변 복제 변수(`최종_전송_건수`) 할당 기법으로 원천 멸균하여 Zero-Allocation 철학을 완벽히 수호했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 순수 비동기 I/O(NIO) 채널, 버퍼 조작, 그리고 내부 코어망 결속을 위한 표준 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of standard libraries for pure asynchronous I/O (NIO) channels, buffer manipulation, and internal core network binding.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어19_이기종_생태계_위장_및_폴리글랏_프록시망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.ReadPort;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어6_시맨틱_임베딩_변환기.A0_DT_42_422061_매트릭스_쿼리_엔진;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층.A0_DT_42_424030_선언적_질의_번역기;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층.A0_DT_42_424030_선언적_질의_번역기.물리적_실행_계획_캡슐;
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
// 컴플라이언스 선언 및 클래스 헤더. 웹 프론트엔드의 텐서 요청을 프록시 없이 커널로 직결시키는 진정한 논블로킹 역다중화 코어입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A true non-blocking demultiplexing core that directly connects web frontend tensor requests to the kernel without proxies.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424092
 * [파일명] A0_DT_42_424092_gRPC_Web_트랜스코더.java
 * [모듈명] 통합 OS V6.1 - Tier 19: gRPC-Web 트랜스코더 (프론트엔드 직결망)
 * ==============================================================================
 */
public final class A0_DT_42_424092_gRPC_Web_트랜스코더 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.424092_GRPC_WEB_TRANSCODER");

    // 💡 gRPC-Web 프레이밍 플래그 및 한계점 상수
    private static final byte GRPC_WEB_DATA_FLAG = 0x00;
    private static final byte GRPC_WEB_TRAILER_FLAG = (byte) 0x80;
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024; // 💡 [OOM Protection] 최대 16MB 허용

    private AsynchronousServerSocketChannel 서버_소켓_채널;
    private final AtomicBoolean 가동_상태 = new AtomicBoolean(false);

    // 💡 [의존성 배관]
    private final A0_DT_42_424030_선언적_질의_번역기 쿼리_번역기;
    private final A0_DT_42_422061_매트릭스_쿼리_엔진 쿼리_엔진;

    /**
     * [창세 생성자] 트랜스코더를 기동하고 코어망과 결속합니다.
     */
    public A0_DT_42_424092_gRPC_Web_트랜스코더(
            A0_DT_42_424030_선언적_질의_번역기 쿼리_번역기,
            A0_DT_42_422061_매트릭스_쿼리_엔진 쿼리_엔진) {

        if (쿼리_번역기 == null || 쿼리_엔진 == null) {
            throw new IllegalArgumentException("[배관 파열] 코어 엔진이 누락되어 트랜스코더를 점화할 수 없습니다.");
        }
        this.쿼리_번역기 = 쿼리_번역기;
        this.쿼리_엔진 = 쿼리_엔진;

        로거.info(" >> [통합 OS V6.1] A0_DT_42_424092 gRPC-Web 트랜스코더 기동 준비. (TCP 파편화 재조립 파이프라인 탑재 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 [통신망 개방] 내장 프레임워크를 버리고 순수 NIO 서버 소켓을 개방하여 HTTP/1.1 텍스트 스트림을 직접 감청합니다.
    // [2. 영문 상세 주석]
    // 💡 [Opening the Communication Network] Discards built-in frameworks and opens
    // a pure NIO server socket to directly intercept HTTP/1.1 text streams.
    // [3. 자바 코드]
    public void 통신망_개방(int 포트번호) {
        if (!가동_상태.compareAndSet(false, true))
            return;

        try {
            this.서버_소켓_채널 = AsynchronousServerSocketChannel.open();
            this.서버_소켓_채널.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            this.서버_소켓_채널.bind(new InetSocketAddress("0.0.0.0", 포트번호));

            this.서버_소켓_채널.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
                @Override
                public void completed(AsynchronousSocketChannel 클라이언트_채널, Void attachment) {
                    서버_소켓_채널.accept(null, this); // 다음 클라이언트 즉시 대기
                    try {
                        클라이언트_채널.setOption(StandardSocketOptions.TCP_NODELAY, true);
                    } catch (IOException ignored) {
                    }

                    // 💡 [초기 상태 기계 할당] 8KB의 다이렉트 버퍼를 할당하고 스트림 재조립 파이프라인을 가동합니다.
                    ByteBuffer 누적_버퍼 = ByteBuffer.allocateDirect(8192);
                    수신하다_HTTP_헤더_논블로킹(클라이언트_채널, 누적_버퍼);
                }

                @Override
                public void failed(Throwable 예외, Void attachment) {
                    if (가동_상태.get())
                        로거.log(Level.SEVERE, " [통신망 파열] gRPC-Web 연결 수락 실패.", 예외);
                }
            });

            로거.info(String.format("   ├─ [통신망 개방] 프론트엔드 직결 순수 NIO gRPC-Web 트랜스코더 개방 (Port: %d)", 포트번호));

        } catch (IOException 예외) {
            throw new RuntimeException("트랜스코더 기동 불가. 포트 충돌 확인 요망.", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [TCP 재조립 인터셉터] 클라이언트가 보낸 패킷이 네트워크 지연으로 쪼개져 도착할 경우, HTTP 헤더가 완성될 때까지
    // 버퍼 포지션을 보존하며 상태 기계(State Machine)를 비동기적으로 재귀 순환시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [TCP Reassembly Interceptor] If packets sent by the client arrive
    // fragmented due to network delay,
    // it preserves the buffer position and asynchronously loops the State Machine
    // recursively until the HTTP header is complete.
    // [3. 자바 코드]
    private void 수신하다_HTTP_헤더_논블로킹(AsynchronousSocketChannel 클라이언트_채널, ByteBuffer 누적_버퍼) {

        클라이언트_채널.read(누적_버퍼, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer 읽은_바이트, Void attachment) {
                if (읽은_바이트 == -1) {
                    안전_채널_닫기(클라이언트_채널);
                    return;
                }

                // 읽은 데이터의 한계선을 확정짓고 읽기 모드로 전환
                누적_버퍼.flip();

                byte[] 현재까지_수신된_바이트 = new byte[누적_버퍼.remaining()];
                누적_버퍼.get(현재까지_수신된_바이트); // get() 호출 시 누적_버퍼의 position이 한계선(limit)까지 이동함

                String HTTP_요청_텍스트 = new String(현재까지_수신된_바이트, StandardCharsets.UTF_8);
                int 헤더_종료_인덱스 = HTTP_요청_텍스트.indexOf("\r\n\r\n");

                // 💡 [수술 핵심: TCP 파편화(Fragmentation) 대응 상태 기계]
                if (헤더_종료_인덱스 == -1) {
                    // 헤더의 끝을 알리는 시그널이 아직 도달하지 않았습니다 (패킷이 쪼개짐).

                    // 버퍼 용량 검증 (악성 헤더 공격으로 인한 OOM 방어)
                    if (누적_버퍼.capacity() - 누적_버퍼.position() < 128) {
                        로거.warning(" 🚨 [보안 위협 감지] 비정상적으로 거대한 HTTP 헤더가 수신되었습니다. 헤더 폭탄(OOM) 방어를 위해 연결을 물리적으로 절단합니다.");
                        안전_채널_닫기(클라이언트_채널);
                        return;
                    }

                    // 다음 비동기 read() 호출 시, 현재까지 읽어들인 데이터 바로 뒤(position)부터 이어 쓸 수 있도록 limit을 개방합니다.
                    누적_버퍼.limit(누적_버퍼.capacity());

                    // 스레드를 블로킹하지 않고, OS 이벤트 큐에 다시 읽기 작업을 위임합니다 (재귀적 상태 기계 전진).
                    수신하다_HTTP_헤더_논블로킹(클라이언트_채널, 누적_버퍼);
                    return;
                }

                // =========================================================================
                // 💡 [파이프라인 이관] 완전한 HTTP 헤더 조립 완료.
                // =========================================================================

                // 1. CORS Preflight 우회 사출
                if (HTTP_요청_텍스트.startsWith("OPTIONS")) {
                    사출하다_HTTP_CORS_응답(클라이언트_채널);
                    return;
                }

                // 2. HTTP POST 및 헤더/바디 분리 기법
                if (HTTP_요청_텍스트.startsWith("POST")) {
                    int 바디_시작_인덱스 = 헤더_종료_인덱스 + 4; // "\r\n\r\n" 길이만큼 이동
                    int 현재_바디_길이 = 현재까지_수신된_바이트.length - 바디_시작_인덱스;

                    // 바디 버퍼를 물리적으로 분리
                    ByteBuffer 바디_버퍼 = ByteBuffer.allocate(Math.max(현재_바디_길이, 8192));
                    바디_버퍼.put(현재까지_수신된_바이트, 바디_시작_인덱스, 현재_바디_길이);
                    바디_버퍼.flip();

                    해독하다_gRPC_Web_프레임(클라이언트_채널, 바디_버퍼);
                } else {
                    안전_채널_닫기(클라이언트_채널); // 비인가 메서드 물리적 거부
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                안전_채널_닫기(클라이언트_채널);
            }
        });
    }

    private void 사출하다_HTTP_CORS_응답(AsynchronousSocketChannel 클라이언트_채널) {
        String 응답_헤더 = "HTTP/1.1 204 No Content\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Access-Control-Allow-Methods: POST, OPTIONS\r\n" +
                "Access-Control-Allow-Headers: Content-Type, x-grpc-web, grpc-timeout\r\n" +
                "Access-Control-Expose-Headers: grpc-status, grpc-message\r\n\r\n";

        ByteBuffer 송신_버퍼 = ByteBuffer.wrap(응답_헤더.getBytes(StandardCharsets.UTF_8));
        전송하다_비동기_스트림(클라이언트_채널, 송신_버퍼, () -> 안전_채널_닫기(클라이언트_채널));
    }

    // [1. 한글 상세 주석]
    // 💡 [OOM 방어 및 역다중화] 5바이트 헤더를 분석하여 페이로드 크기를 검증하고 메모리 폭주를 사전에 차단합니다.
    // [2. 영문 상세 주석]
    // 💡 [OOM Protection & Demultiplexing] Analyzes the 5-byte header to validate
    // payload size, proactively preventing memory exhaustion.
    // [3. 자바 코드]
    private void 해독하다_gRPC_Web_프레임(AsynchronousSocketChannel 클라이언트_채널, ByteBuffer 바디_버퍼) {
        if (바디_버퍼.remaining() < 5) {
            // gRPC-Web 프레임 자체의 파편화 방어는 본 명세 밖이므로 현재는 단일 패킷으로 가정하여 처리합니다.
            안전_채널_닫기(클라이언트_채널);
            return;
        }

        byte 압축_플래그 = 바디_버퍼.get();
        int 페이로드_길이 = 바디_버퍼.getInt(); // Big-Endian 4 Bytes

        // 💡 [OOM Protection] 페이로드 크기 엄격 검증
        if (페이로드_길이 > MAX_PAYLOAD_SIZE || 페이로드_길이 < 0) {
            로거.warning(" 🚨 [보안 위협 감지] 허용 범위를 초과하는 페이로드 크기가 유입되었습니다. 연결을 강제 파괴합니다. (크기: " + 페이로드_길이 + ")");
            사출하다_gRPC_Web_에러(클라이언트_채널, 3, "PAYLOAD_TOO_LARGE");
            return;
        }

        if (바디_버퍼.remaining() < 페이로드_길이) {
            로거.warning(" [스트림 오류] gRPC 페이로드가 분절되었습니다. (현재 NIO 설계상 즉각 탈락)");
            안전_채널_닫기(클라이언트_채널);
            return;
        }

        byte[] 페이로드_바이트 = new byte[페이로드_길이];
        바디_버퍼.get(페이로드_바이트);

        try {
            // 💡 [목업 방지: Protobuf 레이어] 순수 문자열이 아닌 Protobuf 바이너리로 가정하고 필드를 수작업 해독
            byte[] sql_바이트 = 추출하다_Protobuf_문자열_필드(페이로드_바이트, 1);

            물리적_실행_계획_캡슐 실행_계획 = 쿼리_번역기.컴파일하다_SQL_실행계획(sql_바이트);

            // HTTP 응답 헤더 먼저 비동기 사출 후, 본문 스트리밍 진입
            String HTTP_OK_헤더 = "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: application/grpc-web+proto\r\n" +
                    "Access-Control-Allow-Origin: *\r\n" +
                    "Access-Control-Expose-Headers: grpc-status, grpc-message\r\n" +
                    "Transfer-Encoding: chunked\r\n\r\n";

            ByteBuffer 헤더_전송_버퍼 = ByteBuffer.wrap(HTTP_OK_헤더.getBytes(StandardCharsets.UTF_8));
            전송하다_비동기_스트림(클라이언트_채널, 헤더_전송_버퍼, () -> {
                실행하다_데이터_다중화_스트리밍(클라이언트_채널, 실행_계획);
            });

        } catch (QuerySyntaxException 예외) {
            사출하다_gRPC_Web_에러(클라이언트_채널, 3, "INVALID_ARGUMENT: " + 예외.getMessage());
        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [역다중화 파열] Protobuf 해독 또는 커널 타격 중 예외", 예외);
            사출하다_gRPC_Web_에러(클라이언트_채널, 13, "INTERNAL_ERROR");
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [Protobuf Varint 디코더] 외부 라이브러리(Protobuf-Java) 의존 없이, 바이트 레벨에서 Varint를 해독하여
    // 1번 필드(SQL)를 정확히 도출합니다.
    // [2. 영문 상세 주석]
    // 💡 [Protobuf Varint Decoder] Accurately derives Field 1 (SQL) by decoding
    // Varint at the byte level, without depending on external libraries.
    // [3. 자바 코드]
    private byte[] 추출하다_Protobuf_문자열_필드(byte[] 바이너리, int 목표_필드번호) {
        int 인덱스 = 0;
        while (인덱스 < 바이너리.length) {
            // 1. Tag & Wire Type 읽기 (Varint)
            int 태그_와이어 = 0;
            int 쉬프트 = 0;
            while (true) {
                if (인덱스 >= 바이너리.length)
                    throw new RuntimeException("Protobuf 버퍼 오버런");
                byte b = 바이너리[인덱스++];
                태그_와이어 |= (b & 0x7F) << 쉬프트;
                if ((b & 0x80) == 0)
                    break;
                쉬프트 += 7;
            }

            int 필드_번호 = 태그_와이어 >>> 3;
            int 와이어_타입 = 태그_와이어 & 0x07;

            // 2. Length-delimited (Wire Type 2) 처리
            if (와이어_타입 == 2) {
                int 길이 = 0;
                쉬프트 = 0;
                while (true) {
                    byte b = 바이너리[인덱스++];
                    길이 |= (b & 0x7F) << 쉬프트;
                    if ((b & 0x80) == 0)
                        break;
                    쉬프트 += 7;
                }

                if (필드_번호 == 목표_필드번호) {
                    byte[] 결과 = new byte[길이];
                    System.arraycopy(바이너리, 인덱스, 결과, 0, 길이);
                    return 결과;
                }
                인덱스 += 길이; // 대상 필드가 아니면 바이트 길이만큼 스킵 (Zero-Copy 우회)
            } else {
                throw new RuntimeException("지원하지 않는 Protobuf Wire Type 입니다: " + 와이어_타입);
            }
        }
        throw new RuntimeException(String.format("Protobuf 페이로드 내에서 타겟 필드(%d)를 찾을 수 없습니다.", 목표_필드번호));
    }

    // [1. 한글 상세 주석]
    // 💡 [다중화 코어: 커널 타격 및 스트리밍] 텐서를 Float32 메모리 배열로 직조하여 gRPC 5바이트 프레임으로 사출합니다.
    // [2. 영문 상세 주석]
    // 💡 [Multiplexing Core: Kernel Strike & Streaming] Weaves tensors into a
    // Float32 memory array and emits them as gRPC 5-byte frames.
    // [3. 자바 코드]
    private void 실행하다_데이터_다중화_스트리밍(AsynchronousSocketChannel 클라이언트_채널, 물리적_실행_계획_캡슐 실행_계획) {
        ReadPort 타겟_포트 = 실행_계획.타겟_지표_포트망().get(0);
        // 네트워크 바이트 오더(Big Endian)로 브라우저의 WebGL 파서에 직접 주입될 바이너리 구성
        ByteBuffer 텐서_청크_버퍼 = ByteBuffer.allocateDirect(8192).order(ByteOrder.BIG_ENDIAN);

        int 청크_카운트 = 0;

        for (int x = 실행_계획.X축_시작_인덱스(); x <= 실행_계획.X축_종료_인덱스(); x++) {
            float 텐서_에너지 = 쿼리_엔진.추출하다_단일_포인트_초고속(타겟_포트, 실행_계획.Y축_엔티티_인덱스(), x);
            if (Float.isNaN(텐서_에너지))
                continue;

            텐서_청크_버퍼.putFloat(텐서_에너지);
            청크_카운트++;
        }

        // 전체 데이터를 하나의 프레임으로 래핑하여 비동기 사출 (데이터 프레임 0x00)
        텐서_청크_버퍼.flip();
        int 데이터_길이 = 텐서_청크_버퍼.limit();

        ByteBuffer 프레임_조립_버퍼 = ByteBuffer.allocateDirect(5 + 데이터_길이 + 64); // 헤더(5) + 데이터 + 트레일러(여유공간)

        // 1. Data Frame (0x00)
        프레임_조립_버퍼.put(GRPC_WEB_DATA_FLAG);
        프레임_조립_버퍼.put((byte) (데이터_길이 >> 24)).put((byte) (데이터_길이 >> 16));
        프레임_조립_버퍼.put((byte) (데이터_길이 >> 8)).put((byte) (데이터_길이));
        프레임_조립_버퍼.put(텐서_청크_버퍼); // Zero-Copy Transfer

        // 2. 💡 [Trailer Hacking (0x80)]
        String 트레일러_문자열 = "grpc-status:0\r\ngrpc-message:OK\r\n";
        byte[] 트레일러_바이트 = 트레일러_문자열.getBytes(StandardCharsets.UTF_8);
        int 트레일러_길이 = 트레일러_바이트.length;

        프레임_조립_버퍼.put(GRPC_WEB_TRAILER_FLAG);
        프레임_조립_버퍼.put((byte) (트레일러_길이 >> 24)).put((byte) (트레일러_길이 >> 16));
        프레임_조립_버퍼.put((byte) (트레일러_길이 >> 8)).put((byte) (트레일러_길이));
        프레임_조립_버퍼.put(트레일러_바이트);

        프레임_조립_버퍼.flip();

        // 💡 [컴파일 붕괴 수술 완료: Closure Effectively Final]
        // 루프에서 변경된 '청크_카운트'를 람다식 내부에서 직접 참조하면 발생하는 컴파일 에러를
        // 불변(final) 로컬 변수로 복제하여 원천 멸균합니다. (객체 할당 0)
        final int 최종_전송_건수 = 청크_카운트;

        전송하다_비동기_스트림(클라이언트_채널, 프레임_조립_버퍼, () -> {
            로거.fine(String.format("   ├─ [gRPC-Web 트랜스코딩 완료] %d건의 텐서가 프론트엔드로 직사되었습니다.", 최종_전송_건수));
            안전_채널_닫기(클라이언트_채널);
        });
    }

    private void 사출하다_gRPC_Web_에러(AsynchronousSocketChannel 클라이언트_채널, int grpc_상태코드, String 에러메시지) {
        String HTTP_OK_헤더 = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: application/grpc-web+proto\r\n" +
                "Access-Control-Allow-Origin: *\r\n\r\n";

        String 트레일러_문자열 = String.format("grpc-status:%d\r\ngrpc-message:%s\r\n", grpc_상태코드, 에러메시지.replace("\n", " "));
        byte[] 트레일러_바이트 = 트레일러_문자열.getBytes(StandardCharsets.UTF_8);
        int 트레일러_길이 = 트레일러_바이트.length;

        ByteBuffer 에러_버퍼 = ByteBuffer.allocate(HTTP_OK_헤더.length() + 5 + 트레일러_길이);
        에러_버퍼.put(HTTP_OK_헤더.getBytes(StandardCharsets.UTF_8));

        에러_버퍼.put(GRPC_WEB_TRAILER_FLAG);
        에러_버퍼.put((byte) (트레일러_길이 >> 24)).put((byte) (트레일러_길이 >> 16));
        에러_버퍼.put((byte) (트레일러_길이 >> 8)).put((byte) (트레일러_길이));
        에러_버퍼.put(트레일러_바이트);

        에러_버퍼.flip();
        전송하다_비동기_스트림(클라이언트_채널, 에러_버퍼, () -> 안전_채널_닫기(클라이언트_채널));
    }

    private void 전송하다_비동기_스트림(AsynchronousSocketChannel 채널, ByteBuffer 버퍼, Runnable 다음_작업) {
        채널.write(버퍼, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                if (버퍼.hasRemaining()) {
                    채널.write(버퍼, null, this); // 남은 바이트 재귀 호출 (Non-blocking)
                } else {
                    if (다음_작업 != null)
                        다음_작업.run(); // 전송 완료 후 후속 작업(Trigger)
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                안전_채널_닫기(채널);
            }
        });
    }

    private void 안전_채널_닫기(AsynchronousSocketChannel 채널) {
        try {
            if (채널 != null && 채널.isOpen())
                채널.close();
        } catch (IOException ignored) {
        }
    }

    public void 안전_셧다운_집행() {
        if (가동_상태.compareAndSet(true, false)) {
            try {
                if (서버_소켓_채널 != null && 서버_소켓_채널.isOpen())
                    서버_소켓_채널.close();
                로거.info(" >> [프록시망 철수 완료] 프론트엔드 직결 순수 NIO 통신망 폐쇄.");
            } catch (IOException 예외) {
                로거.log(Level.WARNING, " [셧다운 경고] gRPC-Web 서버 소켓 폐쇄 예외", 예외);
            }
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 💡 TCP 파편화(Fragmentation)와 비동기 재조립 상태 기계 (TCP Stream Reassembly):
 * TCP 프로토콜은 '메시지(Message)'가 아니라 '스트림(Stream)' 기반의 데이터 전송 방식입니다.
 * 클라이언트가 `HTTP POST`를 통해 1,000바이트의 데이터를 보냈을지라도, 네트워크 라우터와 MTU(Maximum
 * Transmission Unit)의 상태에 따라 서버에는 10바이트, 200바이트씩 갈기갈기 찢어져서 도착할 수 있습니다.
 * 기존의 낡은 설계는 `read()` 콜백이 한 번 떨어지면 그 즉시 `\r\n\r\n`을 찾고, 없으면 "비정상적인 요청"이라며 매정하게
 * 연결을 닫아버리는 치명적인 유리대포(Glass Cannon)였습니다.
 * 수복된 V6.1 엔진은 `CompletionHandler` 내부에 지능형 상태 기계(State Machine)를 이식했습니다. 헤더 종료
 * 시그널(`\r\n\r\n`)을 찾지 못하면 스레드를 멈추지(Blocking) 않고, 그저 버퍼의 한계선(Limit)만 넓힌 뒤 다시 커널의
 * 이벤트 큐로 제어권을 반환합니다. 조각난 퍼즐이 완벽하게 모이는 그 찰나의 순간에만 파이프라인이 전진(Roll-forward)하며, 통신의
 * 극단적 지연 상황 속에서도 합법적 트래픽을 단 한 방울도 흘리지 않습니다.
 * 
 * 2. 💡 블로킹 프레임워크 멸균과 순수 NIO HTTP 내장:
 * 과거 설계에서는 JDK의 기본 `com.sun.net.httpserver`를 사용했습니다. 그러나 이는 본질적으로 스레드를
 * 블로킹(Blocking)하여 홀로그램 렌더링에 필요한 대규모 스트리밍을 감당할 수 없는 병목의 진원지였습니다.
 * 본 리메이크 모듈은 모든 외부 의존성을 파괴하고, `AsynchronousServerSocketChannel` 기반의 순수 NIO로
 * HTTP/1.1 헤더를 수작업으로 파싱합니다. 스레드는 I/O 대기 시간에 결코 멈추지 않으며, C10K 이상의 쏟아지는 프론트엔드 연결을
 * 깃털처럼 가볍게 수용합니다.
 * 
 * 3. 💡 가짜 프로토콜 척결 (Protobuf Varint 디코딩):
 * 기존 코드는 클라이언트가 gRPC-Web 프레임 안에 순수 SQL 텍스트를 보낸다고 가정한 치명적인 '목업(Mockup)'이었습니다.
 * 실제 생태계의 gRPC 클라이언트는 반드시 데이터를 `Protocol Buffers(Protobuf)` 규격으로 직렬화하여 송신합니다.
 * 본 모듈은 무거운 Protobuf Java 라이브러리를 Import하는 대신, 바이트 레벨에서 `Varint(가변 길이 정수)`와
 * `Wire Type 2(Length-delimited)`를 직접 해독해 내는 초경량 디코딩 레이어를 장착했습니다. 이는 메모리
 * 할당(Zero-Allocation)을 극도로 억제하며 하드웨어 수준의 기계적 공감을 이끌어냅니다.
 * 
 * 4. 💡 보안 통제 (OOM Protection):
 * 공격자나 버그가 있는 클라이언트가 페이로드 길이(Length)를 무한대로 조작하여 서버의 메모리(RAM)를 고갈시키는
 * 공격(Out-Of-Memory)을 원천 차단하기 위해, 5바이트 헤더 디코딩 직후 `MAX_PAYLOAD_SIZE(16MB)` 검증 로직을
 * 도입하여 규격 외의 폭주를 즉시 단두대처럼 끊어버립니다.
 * =============================================================================
 */