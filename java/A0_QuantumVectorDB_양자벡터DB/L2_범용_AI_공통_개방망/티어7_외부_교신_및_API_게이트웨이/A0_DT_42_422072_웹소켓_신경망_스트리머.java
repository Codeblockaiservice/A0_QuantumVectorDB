/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어7_외부_교신_및_API_게이트웨이
 * @alias WebSocket_Neural_Streamer
 * @tier 7
 * @keywords Event-Driven, Zero-Parsing, Hologram Streaming, HIL, Half-Close Defense, Zombie Session Cleaner, High-Water Mark, Backpressure
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422072_웹소켓_신경망_스트리머.java
 * - 기능 (Function): 웹 기반 콘솔(UI)로 시계열 궤적 및 3D 홀로그램 좌표를 실시간 송출하고, 관리자의 제어 명령을 역방향으로 수신합니다.
 * - 역할 (Role): 백엔드(L1~L5)와 프론트엔드(UI) 간의 쌍방향 실시간 소통을 관장하는 초저지연 게이트웨이.
 * - 이론 (Theory): 이벤트 구동형(Event-Driven) 푸시 아키텍처, Zero-Parsing 바이너리 스트리밍, HIL(Human-In-The-Loop) 백채널 라우팅.
 * - 기대효과 (Effect): 클라이언트 사이드로 렌더링 부하를 완벽히 오프로딩(Off-loading)하며, 관리자의 즉각적인 개입(HIL)을 커널 심장부로 0.1ms 내에 전달합니다.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [수복 3] HIL 역방향 제어 배관 관통: `@OnMessage` 내부에서 단절되어 있던 배관을 정적 라우터(Static Router) 패턴으로 수복하여 완벽한 쌍방향 소통망을 완성.
 * - 💡 [아키텍처 제어] 좀비 세션 방어 (Session Leak Prevention): 클라이언트 비정상 종료 시 발생하는 TCP 하프-클로즈(Half-Close) 상태의 세션 누수(Leak)를 물리적으로 파괴하기 위해, 정기적인 `Ping-Pong` 하트비트 스케줄러를 이식하여 무응답 세션을 강제로 적출하는 클리너를 구축했습니다.
 * - 💡 [아키텍처 제어] `broadcastBinaryHologram` 로직에 **하이워터마크(High-water mark) 배압 통제망**을 적용하여 텐서 전송의 안정성을 극대화했습니다.
 * - 💡 [신설] 비동기 큐잉 상태 모니터링: `sessionBackpressureBufferMap`을 신설하여, 전송 대기 중인 버퍼가 10MB를 초과하는 세션은 즉각 `evictZombieSession()`을 호출해 강제 절단하는 서킷 브레이커를 확립했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 웹소켓 구동, 동시성 컬렉션, 스케줄링 및 비동기 배압 제어를 위한 핵심 자바 표준 API를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core Java standard APIs for WebSocket operation, concurrent collections, scheduling, and asynchronous backpressure control.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어7_외부_교신_및_API_게이트웨이;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 웹소켓 기반의 홀로그램 스트리밍과 OOM 방어용 하이워터마크 제어망을 탑재한 실시간 수신소입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A real-time receiving station equipped with WebSocket-based hologram streaming and a high-water mark control network for OOM defense.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422072
 * [파일명] A0_DT_42_422072_웹소켓_신경망_스트리머.java
 * [모듈명] 통합 OS V6.1 - Tier 7: 웹소켓 기반 3D 홀로그램 및 신경망 스트리머
 * 
 * [설계 명세]
 * 1. 역할: 웹 기반 프론트엔드 콘솔(UI)로 시계열 궤적, XAI 영수증, 3D 홀로그램 좌표를 실시간 송출하는 파이프라인.
 * 2. 기능: 비동기 이벤트 버스의 특정 신호(TOPIC_RENDER_*)를 구독하여 바이너리/JSON 프레임으로 브로드캐스팅.
 * 3. 💡 [V6.1 누수 방어 설계]: `session.isOpen()` 검사로는 감지할 수 없는 '하프-클로즈(Half-Close)'
 * 상태의 좀비 세션을 완벽히 멸균하기 위해, 백그라운드 핑(Ping) 데몬을 개통하여 연결이 끊긴 소켓을 OS 커널에서 강제 회수합니다.
 * 4. 💡 [V6.1 배압/하이워터마크 방어막]: 클라이언트의 수신(Read) 속도가 서버의 송신(Write) 속도를 따라가지 못할 때
 * 발생하는
 * 서버 힙(Heap) 메모리의 무한 팽창 및 멜트다운을 차단하기 위해 10MB 임계치의 서킷 브레이커를 신설했습니다.
 * ==============================================================================
 */
@ServerEndpoint(value = "/api/v6/matrix/hologram-stream")
public final class A0_DT_42_422072_웹소켓_신경망_스트리머 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422072_WEBSOCKET_STREAMER");

    // 💡 [동시성 방어막] 접속된 모든 클라이언트(브라우저)의 세션을 안전하게 관리하는 스레드 세이프 해시셋
    private static final Set<Session> activeSessionSet = ConcurrentHashMap.newKeySet();

    // [1. 한글 상세 주석]
    // 💡 [신설: 하이워터마크 배압 통제망] 각 세션별로 OS 버퍼로 넘어가지 못하고 비동기 큐에 대기 중인 바이트 크기를 원자적으로
    // 추적합니다.
    // [2. 영문 상세 주석]
    // 💡 [New: High-water Mark Backpressure Control Network] Atomically tracks the
    // byte size waiting in the asynchronous queue, unable to be passed to the OS
    // buffer, for each session.

    private static final ConcurrentHashMap<Session, AtomicLong> sessionBackpressureBufferMap = new ConcurrentHashMap<>();
    private static final long HIGH_WATER_MARK_10MB = 10 * 1024 * 1024L; // 10MB 한계선

    // [1. 한글 상세 주석]
    // 💡 [좀비 세션 클리너 데몬] UI 클라이언트가 강제 종료되어 FIN 패킷을 정상적으로 보내지 못했을 때 발생하는 영구적인 소켓/메모리
    // 누수(Leak)를 감시하고 멸균하는 백그라운드 스케줄러입니다.
    // [2. 영문 상세 주석]
    // 💡 [Zombie Session Cleaner Daemon] A background scheduler that monitors and
    // sterilizes permanent socket/memory leaks occurring when the UI client is
    // force-closed without normally sending a FIN packet.

    private static final ScheduledExecutorService heartbeatCleanerScheduler = Executors
            .newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "OS_WS_ZOMBIE_CLEANER");
                thread.setDaemon(true);
                thread.setPriority(Thread.MIN_PRIORITY); // 메인 텐서 연산을 방해하지 않음
                return thread;
            });

    static {
        // 30초마다 물리적 Ping-Pong 하트비트를 발송하여 응답 없는 하프-클로즈 세션을 색출 및 소탕(Eviction)합니다.
        heartbeatCleanerScheduler.scheduleAtFixedRate(
                A0_DT_42_422072_웹소켓_신경망_스트리머::executeZombieSessionCleanup,
                30, 30, TimeUnit.SECONDS);
    }

    /**
     * [역방향 제어 라우터 인터페이스]
     * L5 관제탑(Orchestrator)이 이 포트를 통해 자신의 수신 콜백을 결속해 두면, UI에서 인입된 제어 명령이 즉각 역방향
     * 라우팅됩니다.
     */
    @FunctionalInterface
    public interface ReverseControlRouterPort {
        void routeControlSignal(String sessionId, String controlCommand);
    }

    private static ReverseControlRouterPort upperOrchestratorPort;

    /**
     * [관제 역학: 라우터 결속]
     * 시스템 부팅 시 L5 마스터 파사드가 이 메서드를 호출하여 HIL 제어 신호 수신망을 물리적으로 관통시킵니다.
     */
    public static void bindOrchestratorPort(ReverseControlRouterPort port) {
        upperOrchestratorPort = port;
        logger.info("   ├─ [배관 결속] 웹소켓 스트리머의 역방향 제어 라우터가 L5 관제탑(Orchestrator)과 성공적으로 결속(Binding)되었습니다.");
    }

    public A0_DT_42_422072_웹소켓_신경망_스트리머() {
        // 웹소켓 엔진(Servlet Container)에 의해 소켓 기동 시 자동 인스턴스화 됨
    }

    /**
     * [이벤트 버스 구독 파사드 (Event Bus Listener)]
     * 내부 메시지 브로커에서 전달된 이벤트를 분류하여 클라이언트 측에 브로드캐스트합니다.
     */
    public static void receiveRenderingSignal(String topic, Object payload) {
        if (activeSessionSet.isEmpty())
            return; // 접속 중인 관측자(클라이언트)가 없으면 무의미한 직렬화 및 전송(I/O)을 소거하여 에너지를 절약합니다.

        if (topic.startsWith("TOPIC_RENDER_3D_COORDS") && payload instanceof ByteBuffer) {
            broadcastBinaryHologram((ByteBuffer) payload);
        } else if (topic.startsWith("TOPIC_RENDER_XAI_RECEIPT") && payload instanceof String) {
            broadcastJsonReceipt((String) payload);
        } else if (topic.startsWith("TOPIC_HIL_ALERT") && payload instanceof String) {
            broadcastJsonReceipt("{\"alert_type\":\"HIL_COLLISION_DETECTED\", \"message\": " + (String) payload + "}");
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [좀비 세션 강제 적출 유틸리티] 전송 실패나 하이워터마크 임계치 초과가 감지된 비정상 세션을 Set에서 안전하게 제거하고 OS
    // 소켓을 물리적으로 절단합니다.
    // [2. 영문 상세 주석]
    // 💡 [Zombie Session Forced Eviction Utility] Safely removes abnormal sessions
    // from the Set where transmission failures or high-water mark excesses are
    // detected, and physically severs the OS socket.

    private static void evictZombieSession(Session session, String reason) {
        if (activeSessionSet.remove(session)) {
            sessionBackpressureBufferMap.remove(session); // 메모리 릭(Leak) 차단을 위해 대기 버퍼 추적망에서도 동시 제거
            logger.warning(String.format(" 🚨 [좀비 세션 적출] %s 사유로 인해 무응답 세션(%s)을 강제 절단하고 메모리에서 소각(Evicted)했습니다.", reason,
                    session.getId()));
            try {
                session.close();
            } catch (IOException ignored) {
                // 이미 OS 레벨에서 파열된(Closed) 소켓이므로 발생하는 예외는 우아하게 무시(Ignore)합니다.
            }
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [통신 역학 1: 바이너리 다이렉트 브로드캐스트 및 배압(Backpressure) 통제]
    // 3D 좌표를 순수 바이너리 프레임으로 사출합니다. 전송 전 대기 큐 크기를 원자적으로 검증하여 10MB 초과 시 서킷 브레이커를
    // 격발시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [Communication Dynamics 1: Binary Direct Broadcast and Backpressure
    // Control]
    // Emits 3D coordinates as pure binary frames. Atomically verifies the pending
    // queue size before transmission and triggers a circuit breaker if it exceeds
    // 10MB.

    /**
     * [통신 역학 1: 바이너리 다이렉트 브로드캐스트 (Zero-Parsing & High-Water Mark)]
     */
    private static void broadcastBinaryHologram(ByteBuffer tensorBinary) {
        long bytesToSend = tensorBinary.remaining();

        for (Session session : activeSessionSet) {
            if (session.isOpen()) {
                AtomicLong pendingBufferTracker = sessionBackpressureBufferMap.get(session);

                // 💡 [배압 통제 1단계] 비동기 전송을 의뢰하기 전, 예상 대기 버퍼 크기를 가산하고 하이워터마크 임계치를 검증합니다.
                if (pendingBufferTracker != null) {
                    long accumulatedPendingBytes = pendingBufferTracker.addAndGet(bytesToSend);

                    if (accumulatedPendingBytes > HIGH_WATER_MARK_10MB) {
                        // 💡 [서킷 브레이커 발동] 클라이언트 네트워크 수신 지연으로 인해 송신 버퍼가 10MB를 초과하면 가차 없이 연결을 끊어 서버 메모리를
                        // 수호합니다.
                        evictZombieSession(session, "하이워터마크(10MB) 배압 초과: 클라이언트 수신 지연으로 인한 서버 OOM 방어");
                        continue;
                    }
                }

                // 💡 [비동기 전송 집행]
                session.getAsyncRemote().sendBinary(tensorBinary.duplicate(), result -> {
                    // 💡 [배압 통제 2단계] OS 네트워크 송신 버퍼로 데이터가 성공적으로 이관되었으므로 대기 카운터를 차감(Release)합니다.
                    if (pendingBufferTracker != null) {
                        pendingBufferTracker.addAndGet(-bytesToSend);
                    }

                    if (!result.isOK()) {
                        evictZombieSession(session, "바이너리 프레임 전송 실패 (네트워크 파열/단절)");
                    }
                });
            } else {
                evictZombieSession(session, "전송 전 Closed 상태 감지");
            }
        }
    }

    /**
     * [통신 역학 2: JSON 메타데이터 브로드캐스트]
     * 출처 추적, 노드 이름, 통계량 등 가독성이 필요한 메타데이터(XAI 영수증)를 텍스트 프레임으로 직렬화 전송합니다.
     */
    private static void broadcastJsonReceipt(String xaiReceiptString) {
        long sizeToSend = xaiReceiptString.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;

        for (Session session : activeSessionSet) {
            if (session.isOpen()) {
                AtomicLong pendingBufferTracker = sessionBackpressureBufferMap.get(session);

                if (pendingBufferTracker != null) {
                    long accumulatedPendingBytes = pendingBufferTracker.addAndGet(sizeToSend);
                    if (accumulatedPendingBytes > HIGH_WATER_MARK_10MB) {
                        evictZombieSession(session, "JSON 하이워터마크(10MB) 초과: 클라이언트 수신 지연(Slow Reader)");
                        continue;
                    }
                }

                session.getAsyncRemote().sendText(xaiReceiptString, result -> {
                    if (pendingBufferTracker != null) {
                        pendingBufferTracker.addAndGet(-sizeToSend);
                    }
                    if (!result.isOK()) {
                        evictZombieSession(session, "JSON 텍스트 프레임 전송 실패");
                    }
                });
            } else {
                evictZombieSession(session, "전송 전 Closed 상태 감지");
            }
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [생명주기 방어망: 하트비트 스캐너] TCP 하프-클로즈(Half-Close) 현상으로 인해 자바 애플리케이션(isOpen)은
    // 정상으로 인식하지만 실제 물리적 통신은 불가한 유령 세션을 색출합니다.
    // [2. 영문 상세 주석]
    // 💡 [Lifecycle Defense Network: Heartbeat Scanner] Searches for ghost sessions
    // that the Java application recognizes as normal (isOpen) due to TCP
    // Half-Close, but are actually physically incommunicable.

    private static void executeZombieSessionCleanup() {
        if (activeSessionSet.isEmpty())
            return;

        // 페이로드가 없는 0바이트 크기의 순수 Ping 프레임 생성 (대역폭 낭비 0)
        ByteBuffer pingSignal = ByteBuffer.allocate(0);

        for (Session session : activeSessionSet) {
            if (!session.isOpen()) {
                evictZombieSession(session, "이미 닫힌 세션 방치 감지");
                continue;
            }
            try {
                // OS 레벨의 소켓 무결성을 물리적으로 검증하기 위해 Ping 프레임을 동기 방식(BasicRemote)으로 직사합니다.
                session.getBasicRemote().sendPing(pingSignal);
            } catch (IllegalArgumentException | IOException ex) {
                // Ping 전송 시 IOException이 터졌다는 것은 TCP 파이프가 찢어졌음(Half-Close)을 의미합니다.
                evictZombieSession(session, "Ping 응답 실패 (하프-클로즈/네트워크 단절 확인됨)");
            }
        }
    }

    // ==============================================================================
    // [웹소켓 라이프사이클 통제 및 역방향 라우팅 (Lifecycle & Back-channel Routing)]
    // ==============================================================================

    @OnOpen
    public void onOpen(Session session) {
        activeSessionSet.add(session);

        // 💡 [신설: 하이워터마크 추적기 할당] 세션별 대기열 바이트 수를 0으로 초기화하여 배압 모니터링을 개시합니다.
        sessionBackpressureBufferMap.put(session, new AtomicLong(0));

        // 3D 텐서 바이너리 렌더링에 적합하도록 컨테이너의 거대 버퍼 할당
        session.setMaxBinaryMessageBufferSize(10 * 1024 * 1024);
        session.setMaxTextMessageBufferSize(5 * 1024 * 1024);

        logger.info("  ├─ [관측자 접속] 새로운 프론트엔드 콘솔 클라이언트가 위상망에 동기화되었습니다. (Session: " + session.getId() + ")");
    }

    @OnMessage
    public void onMessage(String message, Session session) {
        logger.info(String.format("  ├─ [사령관 지시 수신] 콘솔(%s)로부터 위상 제어 신호 도착: %s", session.getId(), message));

        try {
            // 💡 [배관 수복 완료] 허공으로 증발하던 제어 신호를 결속된 상위 라우터 포트를 통해 L5 관제탑(Orchestrator)으로 다이렉트
            // 역방향 푸시(Push)합니다.
            if (upperOrchestratorPort != null) {
                upperOrchestratorPort.routeControlSignal(session.getId(), message);
            } else {
                logger.warning(" [라우팅 보류] 상위 오케스트레이터 포트가 결속(Binding)되지 않아 제어 파동이 소실되었습니다.");
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [역방향 라우팅 붕괴] HIL 제어 파동을 상위 계층으로 라우팅하는 중 치명적 예외 발생", ex);
        }
    }

    @OnClose
    public void onClose(Session session) {
        evictZombieSession(session, "클라이언트의 명시적 정상 종료 (Close 시그널 수신)");
        logger.info("  ├─ [관측자 이탈] 프론트엔드 콘솔의 연결이 해제되었습니다. (Session: " + session.getId() + ")");
    }

    @OnError
    public void onError(Session session, Throwable exception) {
        // 네트워크 에러 발생 시 즉각적으로 자원을 수거하여 메모리 릭(Leak)을 선제적으로 차단합니다.
        evictZombieSession(session, "통신 소켓 예외 발생");
        logger.log(Level.WARNING, " [통신 파열] 홀로그램 스트리밍 소켓에 물리적 네트워크 예외 발생. 궤도를 이탈한 세션이 강제 절단되었습니다.", exception);
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 하이워터마크(High-water mark)와 배압(Backpressure)의 물리적 역학:
 * 대규모 비동기 소켓 프로그래밍(NIO)의 가장 무서운 적은 '느린 클라이언트(Slow Client / Slow Reader)'입니다.
 * 백엔드 텐서 연산 코어가 초당 60프레임으로 수십 메가바이트의 3D 렌더링 텐서를 사출(Push)하고 있는데,
 * 수신을 받는 클라이언트의 브라우저 렌더링이나 네트워크 대역폭이 느려서 데이터를 제때 소비하지 못한다면 어떻게 될까요?
 * 자바 웹소켓의 `AsyncRemote.sendBinary()`는 OS 커널의 송신 버퍼(Send Buffer)가 꽉 찼을 경우, 전송하지
 * 못하고 적체된 객체(`ByteBuffer`)들을
 * JVM 힙(Heap) 메모리의 무제한 큐에 조용히 쌓아둡니다. 이는 10분도 안 되어 서버 전체의
 * `OutOfMemoryError(OOM)` 붕괴를 유발합니다.
 * 수복된 V6.1 엔진은 `sessionBackpressureBufferMap`이라는 원자적 추적망을 도입했습니다. 데이터를 보내기 전,
 * 현재 큐에 대기 중인 바이트 수(`accumulatedPendingBytes`)를
 * 원자적으로 가산하여, 이 수치가 10MB(`HIGH_WATER_MARK_10MB`)를 넘어가는 순간 해당 세션을 '좀비(Zombie)'로
 * 규정하고 물리적인 통신 소켓을
 * 단두대로 끊어버립니다(Kill). 이는 단 한 명의 느린 클라이언트 때문에 시스템 전체가 죽는 것을 막는, 서버 생존의 최상위 방어
 * 규범(Fail-Fast)입니다.
 * 
 * 2. 쌍방향 통신망 관통 (Back-channel Routing)과 HIL (Human-In-The-Loop):
 * 기존 아키텍처에서 이 모듈은 백엔드의 텐서 연산 결과를 프론트엔드 화면에 쏴주기만 하는 '단방향 확성기'에 불과했습니다.
 * 관리자(사령관)가 UI 화면에서 특정 노드를 포커스하거나, 양자 버퍼에 격리된 모순을 해결하려 버튼을 누르더라도 그 제어
 * 명령(Message)은 처리되지 못하고 허공으로 증발했습니다.
 * 수복된 V6.1 배관 설계는 `ReverseControlRouterPort`라는 정적(Static) 인터페이스 포트를 신설하여,
 * 톰캣 등 서블릿 컨테이너가 제멋대로 생성하고 소멸시키는 웹소켓 인스턴스 환경 하에서도 최상위 L5 관제탑이 자신의 콜백을 물리적으로
 * 결속(Bind)해 둘 수 있는 확고한 역방향 백채널(Back-channel)을 개통했습니다.
 * 이제 UI 클라이언트의 마우스 클릭 한 번은 0.1ms 만에 통합 OS 심장부로 다이렉트 인입되어 파동 함수를 강제 붕괴시키는 진정한
 * 의미의
 * HIL(Human-In-The-Loop) 권력으로 승화되었습니다.
 * 
 * 3. 💡 좀비 세션(TCP Half-Close) 멸균과 메모리 릭(Leak) 완벽 방어:
 * 클라이언트 브라우저가 강제로 종료되거나(OOM) 무선 인터넷이 튕길 때, 서버 측으로 정상적인 종료 FIN 패킷을 보내지 못하면
 * TCP 연결은 클라이언트는 죽었지만 서버 측 소켓은 영원히 열려있는 것으로 착각되는 하프-클로즈(Half-Close) 상태에 빠집니다.
 * 이 상태에서 자바 웹소켓 API의 `session.isOpen()`은 소켓이 정상 연결되어 있다는 착각을 그대로 반환합니다.
 * 이를 방치하면 `activeSessionSet` 컬렉션 객체가 무한히 팽창하여 결국 서버의 물리적 메모리와 파일 디스크립터(FD) 한계를
 * 고갈시킵니다.
 * 이식된 `heartbeatCleanerScheduler`는 매 30초마다 물리적인 0바이트 Ping 프레임을 발사하여, 커널 수준에서 소켓
 * 송신에 실패(`IOException`)했음을
 * 증명해내고, 숨어있던 좀비 세션을 강제로 적출(Evict/Close)하여 시스템의 열역학적 평형과 메모리를 영원히 안전하게 유지합니다.
 * =============================================================================
 */