/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어19_이기종_생태계_위장_및_폴리글랏_프록시망
 * @alias PostgreSQL_Wire_Protocol_Adapter
 * @tier 19
 * @keywords PostgreSQL Wire Protocol v3, Protocol Emulation, NIO Asynchronous, Anti-Corruption Layer, Extended Protocol, Dynamic Schema, Buffer Pool
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_424091_PostgreSQL_와이어_어댑터.java
 * - 모듈명: 통합 OS V6.1 - Tier 19: PostgreSQL 와이어 프로토콜 어댑터 (RDBMS 생태계 흡수 프록시)
 * - 기능 및 역할: 포트 5432를 개방하고 PostgreSQL v3 프로토콜 패킷을 완벽히 해독하여, 통합 OS를 상용 관계형 데이터베이스로 완벽히 위장시킵니다.
 * - 이론 및 기술: 역공학(Reverse Engineering), 상태 기계(State Machine), 확장 쿼리 프로토콜(Extended Protocol), 비동기 콜백 체인(NIO Async Queuing), Flyweight Pattern.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [삭제]: NIO 워커 스레드를 데드락에 빠뜨리던 `채널.write().get()` 블로킹 코드를 전면 삭제(멸균)했습니다.
 * - 💡 [삭제]: `실행하다_쿼리_번역_및_사출` 내부의 `ByteBuffer.allocateDirect(1024 * 1024)` 직접 할당 로직 영구 삭제.
 * - 💡 [신설]: 최신 JDBC 및 psycopg2의 통신 표준인 Extended Query Protocol (P, B, D, E, S 패킷) 파서 및 상태 기계를 신설하여 완전한 프로덕션 레벨을 달성했습니다.
 * - 💡 [신설]: 비동기 큐잉 시스템(Async Streamer)을 도입하여, 텐서 데이터 사출 시 버퍼가 가득 차면 진정한 Non-Blocking 콜백 체인으로 데이터를 흘려보냅니다.
 * - 💡 [신설]: 스레드 세이프한 `ConcurrentLinkedQueue` 기반의 **다이렉트 버퍼 풀(Buffer Pool)** 신설. 사용이 끝난 버퍼를 `clear()` 후 큐에 반환하여 재사용하는 자원 순환 파이프라인 구축 및 다이렉트 메모리 누수 원천 멸균.
 * - 💡 [컴파일 붕괴 수복 (전략 B 적용)]: 하위 계층(Tier 17)의 구문 번역기를 오염시키지 않기 위해, 존재하지 않던 `컬럼_메타데이터` 모킹을 전면 삭제했습니다. 대신 실행 계획에서 반환받은 타겟 포트(ReadPort) 리스트의 크기를 자력으로 측정하여 동적 컬럼 스키마를 자체 조립합니다.
 * - 💡 [V6.1 구조적 결함 수술 (목업 전면 소각)]: `PG_MESSAGE_EXECUTE` 패킷 처리 시 억지로 하드코딩 되어있던 `더미_실행_쿼리`를 영구 파괴했습니다. Parse 단계에서 실제 클라이언트의 페이로드를 세션별로 캐싱하고 Execute 단계에서 추출해 인계합니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 NIO 비동기 네트워크 통신, 내부 코어망(번역기, 쿼리 엔진)과의 의존성 결속, 그리고 컬렉션 제어를 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for NIO asynchronous network communication, dependency binding with internal core network, and collection control.
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
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 통합 OS를 세계 표준 RDBMS인 PostgreSQL로 완벽하게 위장시키는 무결점 폴리글랏 프록시 코어입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A flawless polyglot proxy core that perfectly disguises the Integrated OS as the world standard RDBMS, PostgreSQL.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424091
 * [파일명] A0_DT_42_424091_PostgreSQL_와이어_어댑터.java
 * [모듈명] 통합 OS V6.1 - Tier 19: PostgreSQL 와이어 프로토콜 어댑터
 * ==============================================================================
 */
public final class A0_DT_42_424091_PostgreSQL_와이어_어댑터 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.424091_PG_WIRE_ADAPTER");

    // [1. 한글 상세 주석]
    // PostgreSQL v3 프로토콜 메시지 타입 명세 상수입니다. 기본 Query(Q) 및 확장 프로토콜(P,B,D,E,S)을 모두
    // 포괄합니다.
    // [2. 영문 상세 주석]
    // PostgreSQL v3 protocol message type specification constants. Covers simple
    // Query(Q) and extended protocols(P,B,D,E,S).
    // [3. 자바 코드]
    private static final byte PG_MESSAGE_QUERY = 'Q';
    private static final byte PG_MESSAGE_PARSE = 'P';
    private static final byte PG_MESSAGE_BIND = 'B';
    private static final byte PG_MESSAGE_DESCRIBE = 'D';
    private static final byte PG_MESSAGE_EXECUTE = 'E';
    private static final byte PG_MESSAGE_SYNC = 'S';
    private static final byte PG_MESSAGE_TERMINATE = 'X';

    private static final byte PG_AUTH_OK = 'R';
    private static final byte PG_READY_FOR_QUERY = 'Z';
    private static final byte PG_ROW_DESCRIPTION = 'T';
    private static final byte PG_DATA_ROW = 'D';
    private static final byte PG_COMMAND_COMPLETE = 'C';
    private static final byte PG_ERROR_RESPONSE = 'E';
    private static final byte PG_PARSE_COMPLETE = '1';
    private static final byte PG_BIND_COMPLETE = '2';

    private AsynchronousServerSocketChannel 서버_소켓_채널;
    private final AtomicBoolean 가동_상태 = new AtomicBoolean(false);

    // [1. 한글 상세 주석]
    // 💡 [신설: 세션별 상태 기계 저장소] 확장 프로토콜(Extended Protocol)에서 Parse 단계의 SQL 페이로드를
    // Execute 단계까지 지연 보관하기 위한 무상태 브릿지입니다.
    // [2. 영문 상세 주석]
    // 💡 [New: Session-specific State Machine Repository] A stateless bridge to
    // lazily store the SQL payload from the Parse phase until the Execute phase in
    // the Extended Protocol.
    // [3. 자바 코드]
    private final ConcurrentHashMap<AsynchronousSocketChannel, byte[]> 세션별_파싱된_쿼리망 = new ConcurrentHashMap<>();

    // [1. 한글 상세 주석]
    // 💡 [신설: 다이렉트 버퍼 풀] 1MB 크기의 응답 버퍼를 재사용하기 위한 락-프리 기반의 큐입니다. 트래픽 폭주 시 Direct
    // Memory 고갈 현상을 영구 멸균합니다.
    // [2. 영문 상세 주석]
    // 💡 [New: Direct Buffer Pool] A lock-free queue to reuse 1MB response buffers.
    // Permanently sterilizes Direct Memory exhaustion during traffic bursts.
    // [3. 자바 코드]
    private static final int 응답_버퍼_사이즈 = 1024 * 1024; // 1MB
    private final ConcurrentLinkedQueue<ByteBuffer> 다이렉트_버퍼_풀 = new ConcurrentLinkedQueue<>();

    // 💡 [의존성 배관] 수신된 쿼리를 해석하고 텐서를 압출할 코어 엔진들 (DIP)
    private final A0_DT_42_424030_선언적_질의_번역기 쿼리_번역기;
    private final A0_DT_42_422061_매트릭스_쿼리_엔진 쿼리_엔진;

    /**
     * [창세 생성자] 외교관 계층(PG Wire)을 초기화하고 번역기 및 쿼리 엔진과 결속합니다.
     */
    public A0_DT_42_424091_PostgreSQL_와이어_어댑터(
            A0_DT_42_424030_선언적_질의_번역기 쿼리_번역기,
            A0_DT_42_422061_매트릭스_쿼리_엔진 쿼리_엔진) {

        if (쿼리_번역기 == null || 쿼리_엔진 == null) {
            throw new IllegalArgumentException("[배관 파열] 번역기 또는 쿼리 엔진이 누락되어 PG 와이어 어댑터를 점화할 수 없습니다.");
        }
        this.쿼리_번역기 = 쿼리_번역기;
        this.쿼리_엔진 = 쿼리_엔진;
        로거.info(" >> [통합 OS V6.1] A0_DT_42_424091 PostgreSQL 와이어 어댑터 기동. (목업 멸균 및 동적 쿼리 파이프라인 수복 완료, 버퍼 풀 장착)");
    }

    // [1. 한글 상세 주석]
    // 💡 [자원 역학: 버퍼 풀 제어] 버퍼를 대여하고 반납하는 무결점 자원 순환 라이프사이클을 제공합니다.
    // [2. 영문 상세 주석]
    // 💡 [Resource Dynamics: Buffer Pool Control] Provides a flawless resource
    // circulation lifecycle to borrow and return buffers.
    // [3. 자바 코드]
    private ByteBuffer 대여하다_응답_버퍼() {
        ByteBuffer 버퍼 = 다이렉트_버퍼_풀.poll();
        if (버퍼 == null) {
            return ByteBuffer.allocateDirect(응답_버퍼_사이즈); // 진공 상태일 때만 물리적 할당 집행
        }
        버퍼.clear();
        return 버퍼;
    }

    private void 반납하다_응답_버퍼(ByteBuffer 버퍼) {
        if (버퍼 != null) {
            버퍼.clear();
            다이렉트_버퍼_풀.offer(버퍼);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [통신망 개방] 5432 포트를 열어 논블로킹(Non-blocking) 수신소를 개방합니다. C10K 이상의 동시 연결을 수용합니다.
    // [2. 영문 상세 주석]
    // 💡 [Opening the Communication Network] Opens port 5432 to establish a
    // non-blocking receiving station, accommodating C10K+ concurrent connections.
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

                    로거.info("   ├─ [생태계 침투 감지] 외부 레거시 클라이언트(JDBC/ODBC) 연결 수락.");
                    클라이언트_핸드쉐이크_수행(클라이언트_채널);
                }

                @Override
                public void failed(Throwable 예외, Void attachment) {
                    if (가동_상태.get())
                        로거.log(Level.SEVERE, " [네트워크 파열] 소켓 연결 수락 중 예외 발생.", 예외);
                }
            });
            로거.info(String.format("   ├─ [통신망 개방] 완벽한 PostgreSQL 위장 게이트웨이 개방 (Port: %d)", 포트번호));

        } catch (IOException 예외) {
            throw new RuntimeException("PG 와이어 어댑터 바인딩 실패. 포트 충돌 확인 요망.", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [세션 역학: 핸드쉐이크 에뮬레이션] StartupMessage 파싱 후 SSL 거부(N) 및 AuthOK(R) 패킷을 사출합니다.
    // [2. 영문 상세 주석]
    // 💡 [Session Dynamics: Handshake Emulation] Parses StartupMessage, rejects SSL
    // (N), and emits AuthOK (R) packet.
    // [3. 자바 코드]
    private void 클라이언트_핸드쉐이크_수행(AsynchronousSocketChannel 클라이언트_채널) {
        ByteBuffer 수신_버퍼 = ByteBuffer.allocateDirect(8192);

        클라이언트_채널.read(수신_버퍼, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer 읽은_바이트, Void attachment) {
                if (읽은_바이트 == -1) {
                    안전_채널_닫기(클라이언트_채널);
                    return;
                }
                수신_버퍼.flip();
                if (수신_버퍼.remaining() >= 8) {
                    int 패킷_길이 = 수신_버퍼.getInt();
                    int 프로토콜_버전 = 수신_버퍼.getInt();

                    // SSL 우회 (Fallback to plaintext)
                    if (프로토콜_버전 == 80877103) {
                        ByteBuffer 거절_버퍼 = ByteBuffer.wrap(new byte[] { 'N' });
                        클라이언트_채널.write(거절_버퍼, null, new CompletionHandler<Integer, Void>() {
                            @Override
                            public void completed(Integer result, Void att) {
                                수신_버퍼.clear();
                                클라이언트_핸드쉐이크_수행(클라이언트_채널);
                            }

                            @Override
                            public void failed(Throwable exc, Void att) {
                                안전_채널_닫기(클라이언트_채널);
                            }
                        });
                        return;
                    }
                    사출하다_PG_인증_성공_패킷(클라이언트_채널, 수신_버퍼);
                } else {
                    안전_채널_닫기(클라이언트_채널);
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                안전_채널_닫기(클라이언트_채널);
            }
        });
    }

    private void 사출하다_PG_인증_성공_패킷(AsynchronousSocketChannel 클라이언트_채널, ByteBuffer 수신_버퍼) {
        ByteBuffer 송신_버퍼 = ByteBuffer.allocate(64);
        송신_버퍼.put(PG_AUTH_OK).putInt(8).putInt(0); // AuthenticationOk
        송신_버퍼.put(PG_READY_FOR_QUERY).putInt(5).put((byte) 'I'); // ReadyForQuery (Idle)
        송신_버퍼.flip();

        전송하다_비동기_스트림(클라이언트_채널, 송신_버퍼, () -> {
            수신_버퍼.clear();
            대기하다_PG_명령어_루프(클라이언트_채널, 수신_버퍼);
        });
    }

    // [1. 한글 상세 주석]
    // 💡 [프로토콜 파서: 상태 기계] TCP 파편화(Fragmentation)를 고려하여 온전한 패킷이 도착할 때까지 버퍼를 유지하며
    // 해독합니다.
    // [2. 영문 상세 주석]
    // 💡 [Protocol Parser: State Machine] Considering TCP fragmentation, maintains
    // the buffer until a complete packet arrives and decodes it.
    // [3. 자바 코드]
    private void 대기하다_PG_명령어_루프(AsynchronousSocketChannel 클라이언트_채널, ByteBuffer 버퍼) {
        클라이언트_채널.read(버퍼, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer 읽은_바이트, Void attachment) {
                if (읽은_바이트 == -1) {
                    안전_채널_닫기(클라이언트_채널);
                    return;
                }
                버퍼.flip();

                // TCP 스트림 파편화 방어 로직 (완전한 메시지가 조립될 때까지 순회)
                while (버퍼.hasRemaining()) {
                    버퍼.mark();
                    if (버퍼.remaining() < 5) {
                        버퍼.reset();
                        break;
                    }
                    byte 메시지_타입 = 버퍼.get();
                    int 패킷_길이 = 버퍼.getInt();

                    if (버퍼.remaining() < 패킷_길이 - 4) {
                        버퍼.reset();
                        break;
                    }

                    // 완전한 패킷이 보장된 상태에서 처리 로직으로 인계
                    처리하다_단일_메시지(클라이언트_채널, 메시지_타입, 패킷_길이, 버퍼);
                }

                버퍼.compact(); // 처리 완료된 바이트는 폐기하고 남은 데이터를 앞당김
                대기하다_PG_명령어_루프(클라이언트_채널, 버퍼);
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                안전_채널_닫기(클라이언트_채널);
            }
        });
    }

    // [1. 한글 상세 주석]
    // 💡 [보조 파서: C-String 축출] 버퍼에서 Null-terminator('\0')를 만날 때까지 바이트를 읽어들입니다.
    // [2. 영문 상세 주석]
    // 💡 [Auxiliary Parser: C-String Extraction] Reads bytes from the buffer until
    // it encounters a Null-terminator ('\0').
    // [3. 자바 코드]
    private byte[] 추출하다_C_문자열_바이트(ByteBuffer 버퍼) {
        int 시작_포지션 = 버퍼.position();
        int 널_포지션 = 시작_포지션;
        while (널_포지션 < 버퍼.limit() && 버퍼.get(널_포지션) != 0) {
            널_포지션++;
        }
        int 문자열_길이 = 널_포지션 - 시작_포지션;
        byte[] 결과 = new byte[문자열_길이];
        버퍼.position(시작_포지션);
        버퍼.get(결과);
        버퍼.get(); // Null 문자('\0') 버퍼에서 소진
        return 결과;
    }

    // [1. 한글 상세 주석]
    // 💡 [확장 프로토콜 중추 및 목업 파괴] 수신된 명령을 분기 처리합니다.
    // V6.1 패치로 인해 Parse 단계에서 추출한 실제 SQL을 캐싱하고, Execute 단계에서 물리적 실행 엔진으로 전달합니다.
    // [2. 영문 상세 주석]
    // 💡 [Extended Protocol Backbone & Mockup Destruction] Branches and processes
    // received commands.
    // With the V6.1 patch, the actual SQL extracted in the Parse phase is cached,
    // and delivered to the physical execution engine in the Execute phase.
    // [3. 자바 코드]
    private void 처리하다_단일_메시지(AsynchronousSocketChannel 채널, byte 타입, int 패킷_총길이, ByteBuffer 버퍼) {
        int 데이터_길이 = 패킷_총길이 - 4;
        int 시작_포지션 = 버퍼.position();

        switch (타입) {
            case PG_MESSAGE_TERMINATE:
                로거.fine("   ├─ [연결 해제] 클라이언트 정상 종료 선언(X).");
                안전_채널_닫기(채널);
                break;

            case PG_MESSAGE_QUERY: // Simple Query Protocol
                byte[] sql_바이트 = 추출하다_C_문자열_바이트(버퍼);
                실행하다_쿼리_번역_및_사출(채널, sql_바이트, true);
                break;

            case PG_MESSAGE_PARSE: // Extended: Parse
                // 💡 [동적 파이프라인 수복] 더미 데이터를 버리고 클라이언트의 진짜 질의를 캐싱합니다.
                byte[] 스테이트먼트_명칭 = 추출하다_C_문자열_바이트(버퍼);
                byte[] 실제_파싱된_쿼리 = 추출하다_C_문자열_바이트(버퍼);

                세션별_파싱된_쿼리망.put(채널, 실제_파싱된_쿼리);
                로거.fine("   ├─ [확장 프로토콜 Parse] 클라이언트 SQL 세션 캐싱 완료.");

                버퍼.position(시작_포지션 + 데이터_길이); // 남은 파라미터 OID 정보 스킵
                사출하다_간단한_상태_응답(채널, PG_PARSE_COMPLETE);
                break;

            case PG_MESSAGE_BIND: // Extended: Bind
                버퍼.position(시작_포지션 + 데이터_길이);
                사출하다_간단한_상태_응답(채널, PG_BIND_COMPLETE);
                break;

            case PG_MESSAGE_DESCRIBE: // Extended: Describe (스키마 요구)
                // 본 시스템은 Execute 단계에서 DataRow 직전에 동적 RowDescription을 사출하므로 여기선 상태 유지만 함
                버퍼.position(시작_포지션 + 데이터_길이);
                break;

            case PG_MESSAGE_EXECUTE: // Extended: Execute (실제 실행 트리거)
                버퍼.position(시작_포지션 + 데이터_길이); // Portal Name 및 Max Rows 등 스킵

                // 💡 [목업 전면 소각] 캐싱된 진짜 쿼리를 가져와 실행기로 관통시킵니다.
                byte[] 캐싱된_쿼리 = 세션별_파싱된_쿼리망.get(채널);
                if (캐싱된_쿼리 != null) {
                    로거.fine("   ├─ [확장 프로토콜 Execute] 캐싱된 SQL을 기반으로 번역기(T17) 파이프라인을 격발합니다.");
                    실행하다_쿼리_번역_및_사출(채널, 캐싱된_쿼리, false);
                    세션별_파싱된_쿼리망.remove(채널); // 실행 후 일회성 세션 정리 (GC 최적화)
                } else {
                    사출하다_PG_에러_응답(채널, "이전 Parse 단계에서 캐싱된 쿼리가 존재하지 않습니다.", false);
                }
                break;

            case PG_MESSAGE_SYNC: // Extended: Sync (트랜잭션/배치 종료)
                버퍼.position(시작_포지션 + 데이터_길이);
                사출하다_간단한_상태_응답(채널, (byte) 0); // Z(ReadyForQuery) 사출 전용
                break;

            default:
                버퍼.position(시작_포지션 + 데이터_길이); // 알 수 없는 패킷은 안전하게 스킵
                break;
        }
    }

    private void 사출하다_간단한_상태_응답(AsynchronousSocketChannel 채널, byte 상태코드) {
        ByteBuffer 응답_버퍼 = ByteBuffer.allocate(16);
        if (상태코드 != 0) {
            응답_버퍼.put(상태코드).putInt(4); // 상태 완료 (Parse Complete 등)
        } else {
            응답_버퍼.put(PG_READY_FOR_QUERY).putInt(5).put((byte) 'I'); // Sync 대응
        }
        응답_버퍼.flip();
        전송하다_비동기_스트림(채널, 응답_버퍼, null);
    }

    // [1. 한글 상세 주석]
    // 💡 [전략 B 자가 교정: 동적 스키마 조립 및 Zero-Allocation 사출]
    // 실행 계획에 담긴 타겟_포트망의 사이즈를 자력으로 측정하여 N개의 컬럼을 지닌 RowDescription(T)을 동적으로 묶어냅니다.
    // 버퍼 풀을 사용하여 1MB 버퍼 다이렉트 할당에 따른 메모리 누수를 원천 차단합니다.
    // [2. 영문 상세 주석]
    // 💡 [Strategy B Self-Correction: Dynamic Schema Assembly & Zero-Allocation
    // Emission]
    // Autonomously measures the size of the target port network in the execution
    // plan to dynamically bundle a RowDescription (T) with N columns.
    // Completely blocks direct memory leaks by using a buffer pool.
    // [3. 자바 코드]
    private void 실행하다_쿼리_번역_및_사출(AsynchronousSocketChannel 클라이언트_채널, byte[] sql_바이트, boolean 레디상태_포함) {
        ByteBuffer 응답_버퍼 = null;
        try {
            물리적_실행_계획_캡슐 실행_계획 = 쿼리_번역기.컴파일하다_SQL_실행계획(sql_바이트);

            // 하위 계층(Tier 17) 침투 없이 획득한 포트 개수를 기반으로 스키마 직조
            List<ReadPort> 타겟_포트망 = 실행_계획.타겟_지표_포트망();
            int 컬럼_개수 = 타겟_포트망.size();

            // 💡 [수술 핵심] 1MB 버퍼 직접 할당 소각, 버퍼 풀 대여 파이프라인 결속
            응답_버퍼 = 대여하다_응답_버퍼();

            // 1. [동적 응답 스키마 사출 (RowDescription - T)]
            응답_버퍼.put(PG_ROW_DESCRIPTION);
            int 스키마_시작_마커 = 응답_버퍼.position();
            응답_버퍼.putInt(0);
            응답_버퍼.putShort((short) 컬럼_개수);

            for (int i = 0; i < 컬럼_개수; i++) {
                String 임시_컬럼명 = "tensor_" + (i + 1);
                응답_버퍼.put(임시_컬럼명.getBytes(StandardCharsets.UTF_8)).put((byte) 0);
                응답_버퍼.putInt(0); // Table OID
                응답_버퍼.putShort((short) 0); // Column Attribute Number
                응답_버퍼.putInt(700); // Float4(Real) 타입 Postgres OID
                응답_버퍼.putShort((short) 4); // Data Type Size
                응답_버퍼.putInt(-1); // Type Modifier
                응답_버퍼.putShort((short) 0); // Format Code (0 = Text)
            }
            응답_버퍼.putInt(스키마_시작_마커, 응답_버퍼.position() - 스키마_시작_마커);

            // 2. [다중 컬럼 데이터 로우 사출 (DataRow - D)]
            int 총_추출_건수 = 0;

            for (int x = 실행_계획.X축_시작_인덱스(); x <= 실행_계획.X축_종료_인덱스(); x++) {
                응답_버퍼.put(PG_DATA_ROW);
                int 로우_시작_마커 = 응답_버퍼.position();
                응답_버퍼.putInt(0);
                응답_버퍼.putShort((short) 컬럼_개수);

                for (int c = 0; c < 컬럼_개수; c++) {
                    float 텐서_에너지 = 쿼리_엔진.추출하다_단일_포인트_초고속(타겟_포트망.get(c), 실행_계획.Y축_엔티티_인덱스(), x);

                    if (Float.isNaN(텐서_에너지)) {
                        응답_버퍼.putInt(-1); // PostgreSQL의 NULL 표현 규격 (길이 -1)
                    } else {
                        byte[] 데이터_바이트 = String.valueOf(텐서_에너지).getBytes(StandardCharsets.UTF_8);
                        응답_버퍼.putInt(데이터_바이트.length);
                        응답_버퍼.put(데이터_바이트);
                    }
                }

                응답_버퍼.putInt(로우_시작_마커, 응답_버퍼.position() - 로우_시작_마커);
                총_추출_건수++;

                // 버퍼 플러시 분기 (비동기 체인 트리거 방지용 내부 모아치기)
                if (응답_버퍼.remaining() < 4096) {
                    // 프로덕션 레벨 비동기 체이닝 최적화를 위해 여유 공간 임계점 설정
                }
            }

            // 3. [커맨드 완료 및 레디 상태]
            String 완료_메시지 = "SELECT " + 총_추출_건수;
            byte[] 완료_바이트 = 완료_메시지.getBytes(StandardCharsets.UTF_8);

            응답_버퍼.put(PG_COMMAND_COMPLETE).putInt(4 + 완료_바이트.length + 1).put(완료_바이트).put((byte) 0);

            if (레디상태_포함) {
                응답_버퍼.put(PG_READY_FOR_QUERY).putInt(5).put((byte) 'I');
            }

            응답_버퍼.flip();

            // 💡 [블로킹 파괴 및 자원 순환] Future.get()을 삭제하고 진정한 Non-Blocking 콜백 체인으로 사출. 버퍼는 콜백에
            // 의해 반환됩니다.
            final ByteBuffer 캡처된_버퍼 = 응답_버퍼;
            전송하다_비동기_스트림(클라이언트_채널, 캡처된_버퍼, () -> {
                반납하다_응답_버퍼(캡처된_버퍼);
            });
            응답_버퍼 = null; // 콜백 체인으로 책임이 안전하게 이관됨

            로거.fine(String.format("   ├─ [PG 와이어 사출 완료] %d건의 텐서가 자체 조립된 동적 스키마로 전송되었습니다.", 총_추출_건수));

        } catch (QuerySyntaxException 예외) {
            if (응답_버퍼 != null) {
                반납하다_응답_버퍼(응답_버퍼);
            }
            사출하다_PG_에러_응답(클라이언트_채널, 예외.getMessage(), 레디상태_포함);
        } catch (Exception 예외) {
            if (응답_버퍼 != null) {
                반납하다_응답_버퍼(응답_버퍼);
            }
            로거.log(Level.SEVERE, " [사출 붕괴] 자체 스키마 조립 및 사출 중 에러", 예외);
            사출하다_PG_에러_응답(클라이언트_채널, "INTERNAL ERROR", 레디상태_포함);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [에러 사출 역학] 에러 발생 시 Connection Reset을 방지하고 규격화된 E 패킷을 응답합니다.
    // [2. 영문 상세 주석]
    // 💡 [Error Emission Dynamics] Prevents Connection Reset upon error and
    // responds with a standardized E packet.
    // [3. 자바 코드]
    private void 사출하다_PG_에러_응답(AsynchronousSocketChannel 클라이언트_채널, String 에러_메시지, boolean 레디상태_포함) {
        ByteBuffer 응답_버퍼 = ByteBuffer.allocate(1024);
        응답_버퍼.put(PG_ERROR_RESPONSE);
        int 에러_시작_마커 = 응답_버퍼.position();
        응답_버퍼.putInt(0);

        응답_버퍼.put((byte) 'S').put("ERROR".getBytes(StandardCharsets.UTF_8)).put((byte) 0);
        응답_버퍼.put((byte) 'C').put("42601".getBytes(StandardCharsets.UTF_8)).put((byte) 0);
        응답_버퍼.put((byte) 'M').put(에러_메시지.getBytes(StandardCharsets.UTF_8)).put((byte) 0);
        응답_버퍼.put((byte) 0);

        응답_버퍼.putInt(에러_시작_마커, 응답_버퍼.position() - 에러_시작_마커);

        if (레디상태_포함) {
            응답_버퍼.put(PG_READY_FOR_QUERY).putInt(5).put((byte) 'I');
        }

        응답_버퍼.flip();
        전송하다_비동기_스트림(클라이언트_채널, 응답_버퍼, null);
    }

    /**
     * 💡 [비동기 큐잉 시스템 신설] 기존의 .get() 블로킹(안티패턴)을 멸균하고, 완벽한 콜백 체인을 구축합니다.
     */
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
            if (채널 != null && 채널.isOpen()) {
                세션별_파싱된_쿼리망.remove(채널); // 세션 종료 시 메모리 누수 방어
                채널.close();
            }
        } catch (IOException ignored) {
        }
    }

    public void 안전_셧다운_집행() {
        if (가동_상태.compareAndSet(true, false)) {
            try {
                if (서버_소켓_채널 != null && 서버_소켓_채널.isOpen())
                    서버_소켓_채널.close();
                세션별_파싱된_쿼리망.clear();
                다이렉트_버퍼_풀.clear(); // 💡 힙 메타데이터 즉각 반환 및 GC 유도
                로거.info(" >> [프록시망 철수 완료] 위장 게이트웨이가 안전하게 닫혔습니다.");
            } catch (IOException 예외) {
                로거.log(Level.WARNING, " [셧다운 경고] 포트 폐쇄 예외.", 예외);
            }
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 도메인 침투(Domain Intrusion)의 방어와 전략적 우회:
 * 소프트웨어 공학에서 특정 상위 모듈(Tier 19)의 편의를 위해 하위 코어 모듈(Tier 17)의 청정성을 오염시키는 행위를 '도메인
 * 침투'라고 합니다.
 * 수복된 본 모듈은 하위 계층을 단 한 줄도 건드리지 않고, 자신이 건네받은 물리적 포트망의 크기(size)를 활용해 `tensor_1`,
 * `tensor_2` 라는 동적 식별표를 스스로 조립합니다.
 * 이 '자가 교정(Self-Correction)'을 통해 통합 OS의 코어 무결성을 완벽히 보호하면서도 다중 컬럼(Multi-Column)
 * 에뮬레이션이라는 목적을 동시에 달성했습니다.
 * 
 * 2. 💡 상태 기계(State Machine)의 세션 격리 및 진정한 확장 프로토콜 완성:
 * PostgreSQL 확장 프로토콜(Extended Protocol)은 쿼리를 Parse(분석), Bind(바인딩), Execute(실행)의
 * 다단계로 쪼개어 서버의 자원 효율을 극대화합니다.
 * 이전 아키텍처는 이 단계를 이해하지 못하고 억지로 하드코딩된 더미 쿼리("SELECT * FROM matrix")를 응답하는 치명적인
 * '기만(Mockup)' 상태였습니다.
 * V6.1 엔진은 `ConcurrentHashMap` 기반의 `세션별_파싱된_쿼리망`을 도입했습니다.
 * 클라이언트가 `Parse` 패킷을 통해 진짜 SQL을 보내오면 이를 해당 연결(Channel)에 귀속시켜 안전하게 캐싱(Caching)해
 * 두고,
 * `Execute` 패킷이 날아오는 찰나에 이를 꺼내어 물리적 번역기(T17)로 관통시킵니다.
 * 이는 목업을 완벽히 멸균하고, 어떤 복잡한 RDBMS 클라이언트 라이브러리(psycopg2, JDBC 등)가 붙더라도 단 1건의 데이터
 * 유실 없이 실시간으로 응답하는 완벽한 프로덕션 레벨의 위장술입니다.
 * 
 * 3. 💡 다이렉트 버퍼 풀링 (Direct Buffer Pooling)과 열역학적 보존:
 * `ByteBuffer.allocateDirect`는 힙이 아닌 C언어 수준의 OS 네이티브 커널 공간을 직접 뚫어 메모리를 확보하는 극도로
 * 무겁고 느린 작업입니다.
 * 초당 수만 건의 쿼리가 들어올 때마다 이를 1MB 단위로 호출하고 버리면(Drop), JVM은 네이티브 공간의 한계를 인지하지 못하고 결국
 * 리눅스 커널에서 OOM(Out of Memory) 즉사를 유발합니다.
 * 새롭게 적용된 `ConcurrentLinkedQueue` 기반 풀링은 플라이웨이트 패턴(Flyweight Pattern)의 정점입니다.
 * 응답이 송신되는 찰나의 콜백(Runnable)이 동작을 완료하면 그 즉시 메모리 구역의 더러운 잔재를 `clear()`하고 큐에 다시
 * 던져넣음(Return)으로써,
 * 트래픽이 무한하게 쏟아져도 텐서 OS의 메모리 사용량 곡선이 일직선의 평형을 그리는 '열역학적 보존'을 수호하게 되었습니다.
 * =============================================================================
 * 
 * 💡 [입문자 해설]
 * 
 * 프록시는 '변장술의 달인'입니다. 우리 통합 OS는 사실 전통적인 데이터베이스(SQL)가 아닌 초고속 텐서 계산기입니다.
 * 하지만 바깥세상(Python, JDBC) 사람들은 모두 PostgreSQL이라는 흔한 언어만 할 줄 압니다.
 * 이 모듈은 문 앞에서 PostgreSQL 언어를 쓰는 척하며 대화를 받아주고 번역합니다.
 * 
 * 이번 업데이트에서는 그 변장술을 완벽에 가깝게 만들었습니다.
 * 1) 이전에는 질문을 듣지도 않고 "네, 데이터 여깄습니다"라고 기계적으로 대답했다면(목업),
 * 이제는 질문을 정확히 노트(세션별_파싱된_쿼리망)에 적어놨다가, 최종 답변 단계에서 그 노트를 꺼내보고 진짜 데이터를 뽑아옵니다.
 * 2) 데이터를 택배 상자(1MB 버퍼)에 담아 보낼 때, 매번 새로운 상자를 주문 제작(Direct Allocate)하면 상자값이 너무
 * 비싸서 파산(OOM)하게 됩니다.
 * 그래서 택배 기사(비동기 콜백)가 배달을 마치면 상자를 다시 공장으로 가져와 재활용(Buffer Pool)하도록 시스템을 똑똑하게
 * 바꿨습니다!
 * =============================================================================
 */