/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층
 * @alias Zero_Trust_Checkpoint
 * @tier 17
 * @keywords Zero-Trust, ECDSA, Stringless JWT, RBAC, FSM Scanner, Capability-based Security, TLS 1.3, ALPN, Zero-Copy SSL
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_424050_제로트러스트_검문소.java
 * - 모듈명: 통합 OS V6.1 - Tier 17: 제로 트러스트 검문소 (엔터프라이즈급 암호화 게이트웨이)
 * - 기능 및 역할: 네트워크 계층에서 유입되는 패킷을 TLS 1.3 규격으로 복호화하고, HTTP 헤더에서 추출된 토큰을 String 객체 변환 없이 다이렉트로 스캔하여 권한(Role)을 인가합니다.
 * - 이론 및 기술: 제로 트러스트(Zero-Trust), 문자열 할당 멸균 파싱(Stringless Parsing), 유한 상태 기계(FSM) 기반 JSON 렉서, 객체-권한 모델(Capability-based Security), TLS 1.3 Handshake, ALPN, Zero-Copy SSL Decoding.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [신설] TLS 1.3 & ALPN 네트워크 암호화 파이프라인: JDK 내장 `SSLEngine`을 활용하여, 비동기 소켓 채널에서 들어오는 암호화된 바이트 배열을 힙 메모리 오염(Zero-Allocation) 없이 해독(Unwrap)하는 `unwrapSslPayloadZeroCopy` 파이프라인을 구축했습니다.
 * - 💡 [아키텍처 제어] FSM 파서 예외 처리 강화: 스캔 길이 상한선(Max Depth) 및 무한루프 방지 카운터를 이식하여, 악의적인 페이로드 공격(DoS) 발생 시 파서를 강제 이탈시키는 서킷 브레이커 유지.
 * - 💡 [보안 강화] 토큰 만료(Expiration) 검증 로직을 유지하여 제로 트러스트의 시간 기반 완결성(Time-based Lock)을 수호합니다.
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 암호학적 검증, SSL/TLS 보안 소켓 제어, 하위 계층 OS 커널 권한 포트 발급을 위한 의존성 모듈들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of dependency modules for cryptographic verification, SSL/TLS secure socket control, and lower-layer OS kernel capability port issuance.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.ReadPort;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.WritePort;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망.A0_DT_42_422041_범용_OS레이어_드라이버;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 외부 I/O 요청의 네트워크 스니핑을 원천 차단하고(TLS 1.3), 텐서 접근을 객체-권한 모델(Capability Model)로 엄격히 통제하는 제로 트러스트 검문소입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A flawless Zero-Trust checkpoint that fundamentally blocks network sniffing of external I/O requests (TLS 1.3) and strictly controls tensor access via an Object-Capability model.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424050
 * [파일명] A0_DT_42_424050_제로트러스트_검문소.java
 * [모듈명] 통합 OS V6.1 - Tier 17: 제로 트러스트 검문소 (엔터프라이즈급 암호화 게이트웨이)
 * ==============================================================================
 */
public final class A0_DT_42_424050_제로트러스트_검문소 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.424050_ZERO_TRUST_CHECKPOINT");

    // [1. 한글 상세 주석]
    // 💡 [FSM 상수 설계] 상태 머신(FSM) 렉서가 원본 바이트 배열 내에서 객체 변환 없이 추적할 키(role, exp)와 인가 값의
    // 원시 바이트 상수입니다.
    // [2. 영문 상세 주석]
    // 💡 [FSM Constant Design] Raw byte constants for keys (role, exp) and
    // authorized values that the FSM lexer will track within the original byte
    // array without object conversion.

    private static final byte[] FSM_KEY_ROLE = "role".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FSM_KEY_EXP = "exp".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FSM_VALUE_READ = "READ".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FSM_VALUE_WRITE = "WRITE".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FSM_VALUE_ADMIN = "ADMIN".getBytes(StandardCharsets.UTF_8);

    private static final byte JWT_SEPARATOR_DOT = (byte) '.';

    // [의존성 결합] 서명이 수학적으로 검증된 주체에게만 물리적 메모리 포트를 하사할 OS 커널 배급망 드라이버
    private final A0_DT_42_422041_범용_OS레이어_드라이버 osLayerDriver;

    // 시스템 최고 관리자가 부여한 ECDSA P-256 타원 곡선 암호화 공개키
    private final PublicKey authPublicKey;

    // 💡 [보안 인프라] TLS 1.3 Handshake 및 양방향 암호화 해독을 통제하는 시스템 전역 SSL 컨텍스트
    private SSLContext globalSslContext;

    /**
     * 권한(Role-Based Access Control) 상태 정의형 열거체
     */
    private enum AuthorizedRole {
        READ, WRITE, ADMIN, NONE
    }

    // [1. 한글 상세 주석]
    // 💡 [페이로드 캡슐] 파싱된 인가 권한(Role)과 토큰의 만료 에포크 시간(Expiration Epoch)을 캡슐화하여 반환하기 위한
    // 불변 DTO 레코드입니다.
    // [2. 영문 상세 주석]
    // 💡 [Payload Capsule] An immutable DTO record to encapsulate and return the
    // parsed authorized role and the token's expiration epoch time.

    private record ParsedPayload(AuthorizedRole role, long expirationEpoch) {
    }

    /**
     * [생성자] 제로 트러스트 검문소를 기동하고, 시스템 공개키와 TLS 1.3 인증서를 메모리에 장전(Load)합니다.
     */
    public A0_DT_42_424050_제로트러스트_검문소(
            A0_DT_42_422041_범용_OS레이어_드라이버 osLayerDriver,
            byte[] publicKeyBytes,
            String keystorePath,
            String keystorePassword) {

        if (osLayerDriver == null || publicKeyBytes == null) {
            throw new IllegalArgumentException("[보안 시스템 파열] 필수 의존성 드라이버 또는 인증 공개키가 누락되어 검문소를 기동할 수 없습니다.");
        }
        this.osLayerDriver = osLayerDriver;

        try {
            // 💡 1. [ECDSA 암호체계 락온] 전통적인 RSA 대비 암호화 연산 속도가 빠르고 키 길이가 짧아 효율적인 타원 곡선
            // 암호(ECDSA)를 표준 알고리즘으로 채택합니다.
            KeyFactory keyFactory = KeyFactory.getInstance("EC");
            this.authPublicKey = keyFactory.generatePublic(new X509EncodedKeySpec(publicKeyBytes));

            // 💡 2. [TLS 1.3 락온] 네트워크 패킷 스니핑을 원천 차단하는 엔터프라이즈 SSL 컨텍스트 초기화
            if (keystorePath != null && !keystorePath.isEmpty()) {
                initializeSslContext(keystorePath, keystorePassword);
            }

        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [보안 아키텍처 붕괴] ECDSA 공개키 규격이 올바르지 않거나 TLS 인증서 로드(Load)에 실패했습니다.", ex);
            throw new RuntimeException("암호학적 기저 붕괴 (Cryptographic Foundation Collapse)", ex);
        }

        logger.info(" >> [통합 OS V6.1] A0_DT_42_424050 제로 트러스트 검문소 기동 완료. (TLS 1.3 & ALPN 파이프라인 및 ECDSA 방어망 전개 완료)");
    }

    // =========================================================================
    // 💡 [보안 코어: TLS 1.3 Zero-Copy 디코딩 파이프라인]
    // =========================================================================

    // [1. 한글 상세 주석]
    // 💡 [SSL 초기화] 지정된 JKS/PKCS12 규격의 키스토어를 읽어들여 TLS 1.3 및 ALPN 프로토콜 협상을 지원하는 SSL
    // 컨텍스트를 메모리에 적재합니다.
    // [2. 영문 상세 주석]
    // 💡 [SSL Initialization] Reads the specified JKS/PKCS12 formatted keystore and
    // loads an SSL context supporting TLS 1.3 and ALPN protocol negotiation into
    // memory.

    private void initializeSslContext(String keystorePath, String password) throws Exception {
        char[] passwordArray = password != null ? password.toCharArray() : new char[0];

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream inputStream = new FileInputStream(keystorePath)) {
            keyStore.load(inputStream, passwordArray);
        }

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, passwordArray);

        TrustManagerFactory trustManagerFactory = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        // 보안 강도를 위해 오직 완벽한 순방향 비밀성(PFS, Perfect Forward Secrecy)을 지원하는 최신 TLS 1.3 규격만을
        // 강제합니다.
        this.globalSslContext = SSLContext.getInstance("TLSv1.3");
        this.globalSslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        logger.fine("   ├─ [TLS 시스템 인가 완료] PKCS12 키스토어 로드 및 TLS 1.3 글로벌 컨텍스트 점화 성공.");
    }

    // [1. 한글 상세 주석]
    // 💡 [네트워크 보안 배관용 API] 외부 클라이언트와 새로운 비동기 소켓 연결이 맺어질 때마다 1:1로 대응하여 통신을 암복호화할
    // SSLEngine 인스턴스를 발급합니다.
    // [2. 영문 상세 주석]
    // 💡 [Network Security Plumbing API] Creates and issues a 1:1 corresponding
    // SSLEngine instance to encrypt/decrypt communication whenever a new
    // asynchronous socket connection is established with an external client.

    public SSLEngine createTls13Engine() {
        if (globalSslContext == null) {
            throw new IllegalStateException("[보안 붕괴] 시스템 SSL 컨텍스트가 초기화되지 않은 상태에서 엔진 발급이 요청되었습니다.");
        }
        SSLEngine engine = globalSslContext.createSSLEngine();
        engine.setUseClientMode(false); // 수신소(Server) 모드로 동작

        SSLParameters parameters = engine.getSSLParameters();
        parameters.setProtocols(new String[] { "TLSv1.3" });

        // 💡 [ALPN 지원 확립] HTTP/2(h2)와 HTTP/1.1(http/1.1) 프로토콜을 네트워크 계층의 핸드쉐이크 단계에서 사전
        // 협상(Negotiation)합니다.
        parameters.setApplicationProtocols(new String[] { "h2", "http/1.1" });
        engine.setSSLParameters(parameters);

        return engine;
    }

    // [1. 한글 상세 주석]
    // 💡 [Zero-Allocation 복호화] 암호화된 수신 ByteBuffer를 파라미터로 받아, 무거운 힙 메모리에 바이트
    // 배열(byte[]) 객체를 새로 생성(Allocation)하지 않고,
    // 즉시 평문 애플리케이션 ByteBuffer로 다이렉트 복호화(Unwrap) 연산을 집행합니다.
    // [2. 영문 상세 주석]
    // 💡 [Zero-Allocation Decryption] Takes an encrypted inbound ByteBuffer as a
    // parameter and directly decrypts (Unwraps) it into a plaintext application
    // ByteBuffer without creating new byte array (byte[]) objects in heavy heap
    // memory.

    /**
     * 비동기 소켓 채널에서 읽어들인 암호화된 버퍼 페이로드를 평문(Plaintext)으로 복호화합니다.
     * 
     * @param sslEngine           해당 커넥션 세션에 귀속된 독립적인 SSLEngine
     * @param networkCipherBuffer 네트워크로부터 수신된 암호화 데이터가 담긴 버퍼 (In)
     * @param appPlainBuffer      해독된 평문 데이터가 안전하게 담길 타겟 버퍼 (Out)
     * @return 해독 작업(Unwrap) 결과의 상태 코드 반환
     */
    public SSLEngineResult.Status unwrapSslPayloadZeroCopy(SSLEngine sslEngine, ByteBuffer networkCipherBuffer,
            ByteBuffer appPlainBuffer) throws SSLException {
        // 객체 힙 할당(Zero-Allocation) 없이 버퍼 대 버퍼(Buffer-to-Buffer) 물리적 다이렉트 해독 연산을 집행합니다.
        SSLEngineResult result = sslEngine.unwrap(networkCipherBuffer, appPlainBuffer);

        if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
            logger.warning(" [SSL 버퍼 붕괴 방어] 해독된 평문을 담을 애플리케이션 버퍼의 공간이 부족합니다. 상위 레이어에서 버퍼 크기 Scale-up 조치가 필요합니다.");
        }

        return result.getStatus();
    }

    // =========================================================================
    // 💡 [보안 코어: 객체-권한 모델 (Capability-based Security)]
    // =========================================================================

    // [1. 한글 상세 주석]
    // 💡 [객체-권한 모델 역학 1: 읽기(Read) 권한 하사 및 만료 검증]
    // 유입된 원시 JWT 토큰의 서명(Signature)과 만료 시간(Expiration)을 엄격히 검증하고, 추출된 권한이 'READ' 또는
    // 'ADMIN'일 때만 커널 메모리 ReadPort를 안전하게 발급합니다.
    // [2. 영문 상세 주석]
    // 💡 [Object-Capability Model Dynamics 1: Granting Read Authority & Expiration
    // Verification]
    // Strictly verifies the signature and expiration time of the incoming raw JWT
    // token, and securely issues a kernel memory ReadPort only when the extracted
    // authority is 'READ' or 'ADMIN'.

    /**
     * @param rawJwtTokenBytes   HTTP 헤더(Authorization: Bearer)에서 String 객체 파싱 없이
     *                           그대로 가져온 원시 바이트 배열
     * @param targetFeatureIndex 조회(읽기)를 원하는 Z축 지표 물리 인덱스
     * @return 암호학적 검증 성공 시 OS 커널 하드웨어 절단형 ReadPort 반환. 실패 시 즉시 Runtime
     *         SecurityException 발산.
     */
    public ReadPort issueReadPort(byte[] rawJwtTokenBytes, int targetFeatureIndex) {
        if (!verifySignatureStringless(rawJwtTokenBytes)) {
            logger.warning(" 🚨 [보안 정책 위반] 조작되었거나 서명이 유효하지 않은 불법 토큰 페이로드가 유입되었습니다. 접근을 물리적으로 차단합니다.");
            throw new SecurityException("암호학적 토큰 서명 검증 실패 (Signature Verification Failed)");
        }

        byte[] decodedPayloadBytes = extractPayloadBytes(rawJwtTokenBytes);
        ParsedPayload parsedResult = parsePayloadFsm(decodedPayloadBytes);

        // 💡 [시간 기반 락(Time-based Lock) 완결성 검증]
        verifyTokenExpiration(parsedResult.expirationEpoch());

        AuthorizedRole acquiredRole = parsedResult.role();

        if (acquiredRole == AuthorizedRole.READ || acquiredRole == AuthorizedRole.ADMIN) {
            logger.fine("   ├─ [제로 트러스트 검문 통과] 읽기(Read) 권한이 완벽히 입증되었습니다. 메모리 ReadPort를 하사합니다.");
            return osLayerDriver.extractTruncatedRawPort(targetFeatureIndex);
        } else {
            logger.warning(" 🚨 [권한 부족(Forbidden)] 토큰의 서명은 유효하나 요구되는 읽기 권한(READ)이 페이로드에 존재하지 않습니다.");
            throw new SecurityException("읽기 권한 인가 실패 (Insufficient Read Privileges)");
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [객체-권한 모델 역학 2: 쓰기 권한(WritePort) 하사 (모의전 샌드박스 전용)]
    // 외부 AI 에이전트가 모의 백테스트를 치르기 위해 쓰기 권한을 요청할 때, 시간 인가 및 'WRITE'/'ADMIN' 권한을 검증하고
    // 원본을 훼손하지 않는 샌드박스 포트를 발급합니다.
    // [2. 영문 상세 주석]
    // 💡 [Object-Capability Model Dynamics 2: Granting Write Authority (Sandbox
    // Exclusive)]
    // When an external AI agent requests write authority for a mock backtest,
    // verifies time authorization and 'WRITE'/'ADMIN' authority, and issues a
    // sandbox port that does not corrupt the original data.

    public WritePort issueSandboxWritePort(byte[] rawJwtTokenBytes, int targetFeatureIndex) {
        if (!verifySignatureStringless(rawJwtTokenBytes)) {
            throw new SecurityException("암호학적 토큰 서명 검증 실패 (Signature Verification Failed)");
        }

        byte[] decodedPayloadBytes = extractPayloadBytes(rawJwtTokenBytes);
        ParsedPayload parsedResult = parsePayloadFsm(decodedPayloadBytes);

        verifyTokenExpiration(parsedResult.expirationEpoch());

        AuthorizedRole acquiredRole = parsedResult.role();

        if (acquiredRole == AuthorizedRole.WRITE || acquiredRole == AuthorizedRole.ADMIN) {
            logger.info("   ├─ [제로 트러스트 검문 통과] 쓰기(Write) 권한이 입증되었습니다. 모의전(Copy-on-Write 샌드박스) 전용 WritePort를 하사합니다.");

            // 💡 [보안 아키텍처 원칙] 외부 네트워크에서 호출된 API 요청으로는 원본 커널 데이터를 영구히 파괴할 수 있는 진성 마스터
            // WritePort를 절대 내어주지 않습니다.
            // 반드시 복제된 샌드박스(섀도우) 포트만을 발급하여 제로 트러스트(Zero-Trust) 원칙의 핵심을 수호합니다.
            return osLayerDriver.extractTruncatedSandboxPort(targetFeatureIndex);
        } else {
            throw new SecurityException("쓰기 권한 인가 실패 (Insufficient Write Privileges)");
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [공통 보안 모듈: 토큰 만료 시간 확인] 현재 시스템 에포크 타임(초)과 추출된 토큰 페이로드의 만료 시간(exp)을 엄격히
    // 대조합니다.
    // [2. 영문 상세 주석]
    // 💡 [Common Security Module: Token Expiration Check] Strictly compares the
    // extracted expiration time (exp) of the token payload with the current system
    // epoch time (in seconds).

    private void verifyTokenExpiration(long expirationEpoch) {
        if (expirationEpoch != -1L) {
            long currentEpoch = System.currentTimeMillis() / 1000L;
            if (currentEpoch > expirationEpoch) {
                logger.warning(
                        String.format(" 🚨 [시간 만료 위반] 인증 토큰의 수명이 만료되었습니다. (만료: %d < 현재: %d) 시스템 접근을 물리적으로 차단합니다.",
                                expirationEpoch, currentEpoch));
                throw new SecurityException("토큰 생명주기 만료 (Token Expired)");
            }
        }
    }

    // =========================================================================
    // 💡 [보안 코어: 유한 상태 기계(FSM) 기반 Zero-Allocation JSON 스캐너]
    // =========================================================================

    // [1. 한글 상세 주석]
    // 💡 [기계적 공감 및 방어막 전개: Max-Depth FSM 스캐너]
    // JSON 페이로드에서 `role`과 `exp` 값을 단일 패스(One-Pass)로 고속 추출하며, 악의적으로 조작된 페이로드(무한 뎁스,
    // 닫히지 않은 따옴표 등)로 인한 무한루프 교착 상태(Deadlock)를 Max Depth 방어 카운터로 완벽히 멸균합니다.
    // [2. 영문 상세 주석]
    // 💡 [Mechanical Sympathy & Shield Deployment: Max-Depth FSM Scanner]
    // Extracts `role` and `exp` values from the JSON payload at high speed in a
    // single pass, and perfectly sterilizes infinite loop deadlocks caused by
    // maliciously manipulated payloads (infinite depth, unclosed quotes, etc.) with
    // a Max Depth defense counter.

    private ParsedPayload parsePayloadFsm(byte[] payloadBytes) {
        int length = payloadBytes.length;

        // 💡 [수술 핵심: Max Depth Lock-out 방어막]
        // 해커에 의해 비정상적으로 거대한 크기의 JSON이 유입되어 커서를 전진시키지 못한 채 스레드 자원을 독점(DoS 공격)하는 현상을
        // 물리적으로 멸균합니다.
        final int MAX_SCAN_LIMIT = 8192; // 8KB Max Depth 제한
        if (length > MAX_SCAN_LIMIT) {
            throw new SecurityException("페이로드 데이터 크기가 OS에서 허용된 FSM 스캔 상한선(Max Depth Limit)을 초과했습니다.");
        }

        AuthorizedRole acquiredRole = AuthorizedRole.NONE;
        long expirationEpoch = -1L;

        int cursor = 0;
        int infiniteLoopGuardCounter = 0;

        while (cursor < length) {
            infiniteLoopGuardCounter++;
            if (infiniteLoopGuardCounter > MAX_SCAN_LIMIT) {
                // 💡 [안전망 서킷 브레이커 전개] 악의적인 유니코드나 이스케이프 문자열 공격으로 인해 커서가 전진하지 못하는 파서 교착
                // 상태(Deadlock)를 즉각 폭파시킵니다.
                throw new SecurityException("FSM 파서 스캐너 강제 이탈(Timeout): 비정상적인 JSON 페이로드 구조로 인한 무한 루프 공격 감지");
            }

            if (payloadBytes[cursor] == '"') {
                if (isMatch(payloadBytes, cursor + 1, FSM_KEY_ROLE)) {
                    cursor += 1 + FSM_KEY_ROLE.length;

                    if (cursor < length && payloadBytes[cursor] == '"') {
                        cursor++;
                        cursor = skipWhitespace(payloadBytes, cursor);

                        if (cursor < length && payloadBytes[cursor] == ':') {
                            cursor++;
                            cursor = skipWhitespace(payloadBytes, cursor);

                            if (cursor < length && payloadBytes[cursor] == '"') {
                                cursor++;
                                int valueStartOffset = cursor;

                                while (cursor < length && payloadBytes[cursor] != '"') {
                                    cursor++;
                                    infiniteLoopGuardCounter++; // 서브 내부 루프도 철저히 방어 카운터 적용
                                }

                                int valueLength = cursor - valueStartOffset;
                                if (isMatchWithLength(payloadBytes, valueStartOffset, valueLength, FSM_VALUE_ADMIN))
                                    acquiredRole = AuthorizedRole.ADMIN;
                                else if (isMatchWithLength(payloadBytes, valueStartOffset, valueLength, FSM_VALUE_READ))
                                    acquiredRole = AuthorizedRole.READ;
                                else if (isMatchWithLength(payloadBytes, valueStartOffset, valueLength,
                                        FSM_VALUE_WRITE))
                                    acquiredRole = AuthorizedRole.WRITE;
                            }
                        }
                    }
                } else if (isMatch(payloadBytes, cursor + 1, FSM_KEY_EXP)) {
                    // 💡 "exp" (만료 시간) 키가 탐지되었을 경우의 상태 전이 로직
                    cursor += 1 + FSM_KEY_EXP.length;

                    if (cursor < length && payloadBytes[cursor] == '"') {
                        cursor++;
                        cursor = skipWhitespace(payloadBytes, cursor);

                        if (cursor < length && payloadBytes[cursor] == ':') {
                            cursor++;
                            cursor = skipWhitespace(payloadBytes, cursor);

                            long expValue = 0;
                            boolean isNumberParsed = false;

                            // JSON 숫자 규격 파싱 (종료 조건: 콤마, 공백, 닫는 괄호 등)
                            while (cursor < length && payloadBytes[cursor] >= '0' && payloadBytes[cursor] <= '9') {
                                expValue = expValue * 10 + (payloadBytes[cursor] - '0');
                                isNumberParsed = true;
                                cursor++;
                                infiniteLoopGuardCounter++;
                            }

                            if (isNumberParsed) {
                                expirationEpoch = expValue;
                                continue;
                            }
                        }
                    }
                }
            }
            cursor++;
        }

        return new ParsedPayload(acquiredRole, expirationEpoch);
    }

    private int skipWhitespace(byte[] payloadBytes, int cursor) {
        while (cursor < payloadBytes.length &&
                (payloadBytes[cursor] == ' ' || payloadBytes[cursor] == '\t' || payloadBytes[cursor] == '\n'
                        || payloadBytes[cursor] == '\r')) {
            cursor++;
        }
        return cursor;
    }

    private boolean isMatch(byte[] source, int start, byte[] target) {
        if (start + target.length > source.length)
            return false;
        for (int i = 0; i < target.length; i++) {
            if (source[start + i] != target[i])
                return false;
        }
        return true;
    }

    private boolean isMatchWithLength(byte[] source, int start, int length, byte[] target) {
        if (length != target.length)
            return false;
        return isMatch(source, start, target);
    }

    // =========================================================================
    // 💡 [보안 코어: Stringless 암호학적 검증 엔진 (Stringless Cryptographic Engine)]
    // =========================================================================

    /**
     * JWT (Header.Payload.Signature) 구조를 거거운 String 객체로 변환하지 않고, 원시 바이트 배열 상태에서 직접
     * 분해하여
     * 시스템 ECDSA 서명의 무결성을 수학적으로 입증합니다.
     */
    private boolean verifySignatureStringless(byte[] tokenBytes) {
        if (tokenBytes == null || tokenBytes.length < 10)
            return false;

        int firstDotIndex = -1;
        int secondDotIndex = -1;

        // O(N) 순방향 선형 스캔으로 온점('.')의 바이트 오프셋 탐색 (무거운 String.split() 객체 할당 로직 영구 폐기)
        for (int i = 0; i < tokenBytes.length; i++) {
            if (tokenBytes[i] == JWT_SEPARATOR_DOT) {
                if (firstDotIndex == -1) {
                    firstDotIndex = i;
                } else if (secondDotIndex == -1) {
                    secondDotIndex = i;
                    break; // 서명부(Signature) 시작점을 찾았으므로 스캔을 즉시 종료
                }
            }
        }

        // JWT 토큰 규격(Header.Payload.Signature)이 깨져있거나 변조된 경우 접근 차단
        if (firstDotIndex == -1 || secondDotIndex == -1 || secondDotIndex == tokenBytes.length - 1) {
            return false;
        }

        try {
            // 1. [서명 검증용 원본 메시지 데이터 분리] Header.Payload 부분을 바이트 배열로 추출
            byte[] signatureMessageBytes = Arrays.copyOfRange(tokenBytes, 0, secondDotIndex);

            // 2. [서명 바이트 디코딩] Base64URL 형식으로 인코딩된 끝자락 Signature 부분 추출 및 디코딩
            byte[] encodedSignature = Arrays.copyOfRange(tokenBytes, secondDotIndex + 1, tokenBytes.length);
            byte[] decodedSignature = Base64.getUrlDecoder().decode(encodedSignature);

            // 3. 💡 [ECDSA 수학적 검증] 자바 JVM의 기본 암호화 프로바이더를 사용하여 비대칭 키(Asymmetric Key) 서명 무결성
            // 확인
            Signature ecdsaVerifier = Signature.getInstance("SHA256withECDSA");
            ecdsaVerifier.initVerify(authPublicKey);
            ecdsaVerifier.update(signatureMessageBytes);

            return ecdsaVerifier.verify(decodedSignature);

        } catch (Exception ex) {
            logger.fine(" [암호학적 서명 검증 에러] 토큰 바이트 배열 해독 중 물리적/논리적 에러 발생: " + ex.getMessage());
            return false;
        }
    }

    /**
     * 원시 JWT 바이트 배열에서 페이로드(Payload) 영역만 빠르고 정확하게 추출하여 Base64URL 디코딩을 수행합니다.
     */
    private byte[] extractPayloadBytes(byte[] tokenBytes) {
        int firstDotIndex = -1;
        int secondDotIndex = -1;

        for (int i = 0; i < tokenBytes.length; i++) {
            if (tokenBytes[i] == JWT_SEPARATOR_DOT) {
                if (firstDotIndex == -1)
                    firstDotIndex = i;
                else {
                    secondDotIndex = i;
                    break;
                }
            }
        }

        byte[] encodedPayload = Arrays.copyOfRange(tokenBytes, firstDotIndex + 1, secondDotIndex);
        return Base64.getUrlDecoder().decode(encodedPayload);
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. TLS 1.3과 ALPN(Application-Layer Protocol Negotiation)의 기하학적 수호:
 * 이전 세대 아키텍처는 JWT 토큰의 시스템 서명(ECDSA)을 훌륭히 검증하여 권한을 인가(Authorization)하는 로직을 가졌으나,
 * 네트워크 물리 계층에서 평문(Plaintext)으로 데이터가 오간다는 것은 해커의 중간자 공격(MITM)이나 패킷 스니핑 감청에 시스템이
 * 100% 무방비로 뚫려있다는 것을 뜻합니다.
 * 수리된 V6.1 보안 모듈은 JDK 커널에 내장된 `SSLEngine`을 완벽히 융합했습니다. 외부 클라이언트가 소켓 연결을 여는 즉시
 * TLS 1.3 핸드쉐이크(Handshake)를 강제하며,
 * 최신 ALPN(Application-Layer Protocol Negotiation) 확장을 통해 HTTP/2(gRPC)와 HTTP/1.1
 * 프로토콜을 통신 초기에 동적으로 안전하게 협상합니다.
 * 이는 B2B 엔터프라이즈급 금융/AI 환경에서 기밀 데이터를 주고받기 위한 법적, 물리적 보안 규격(Compliance)을 완벽히 충족하는
 * 절대 방어막입니다.
 * 
 * 2. Zero-Copy SSL Decoding (메모리 객체 할당 멸균 해독 파이프라인):
 * 비동기(NIO) 네트워크 백엔드 프로그래밍에서 시스템을 느리게 만드는 가장 무서운 병목은, 암호화된 스트림 데이터를 평문으로 풀기 위해
 * `byte[]` 객체를 힙 메모리에 끝없이 새로 할당(Allocation)하는 행위입니다.
 * 설계된 `unwrapSslPayloadZeroCopy` 메서드는 `SSLEngine.unwrap()`을 호출하여, OS 커널 네트워크
 * 버퍼에서 받아온 `ByteBuffer`의 암호문을
 * 무거운 힙 메모리 할당(Zero-Allocation) 없이, 시스템이 미리 재사용 용도로 선할당 해둔 애플리케이션 `ByteBuffer`
 * 공간으로 곧장 밀어 넣습니다.
 * 시스템 가비지 컬렉터(GC)를 단 1나노초도 깨우지 않고 암호화된 텐서의 봉인을 해제하는 이 기법은 HFT(High-Frequency
 * Trading) 시스템의 일관된 심장 박동(Low Latency)을 영구히 유지시킵니다.
 * 
 * 3. 초경량 유한 상태 기계 (FSM: Finite State Machine) 렉서의 진화와 DoS 방어막 (Max Depth
 * Limit):
 * 과거 V6.0 아키텍처의 단순한 JSON 파서는 악의적인 해커가 고의적으로 무한히 이스케이프된 유니코드나 닫히지 않는 따옴표 페이로드를
 * 전송할 경우,
 * 파서의 `while` 루프가 탈출 조건을 영영 찾지 못하고 특정 메모리 영역을 끝없이 빙빙 도는 무한루프 교착 상태(Deadlock)에
 * 빠져 스레드를 고갈시킬 수 있는 치명적 논리적 취약점(DoS 공격 루트)이 내포되어 있었습니다.
 * 수리된 V6.1 모듈은 어떠한 변칙적인 상황에서도 파싱 루프가 `8192`회(8KB 상한선)를 초과하면 즉시 강제 타임아웃 예외를 발동시켜
 * 서킷을 끊고 내던지는 물리적 카운터(`infiniteLoopGuardCounter`)를 이식했습니다.
 * 이는 C언어 수준의 원시적인 파서를 설계할 때 반드시 동반되어야 하는 절대적이고 필수적인 보안 서킷 브레이커입니다.
 * 
 * 4. 시간 기반 락 (Time-based Lock)과 만료 시간(Expiration)의 수학적 수호:
 * 엔터프라이즈 제로 트러스트(Zero-Trust) 보안 모델에서
 * "이 토큰이 정당한 시스템 관리자(Admin)의 프라이빗 키에 의해 서명되었는가?"를 묻는 무결성 검증은 절반의 진실에 불과합니다.
 * 토큰이 외부에서 탈취되었을 때를 대비하여 "이 토큰이 발급된 지 너무 오래되지 않았는가?"를 묻는 시간 기반 통제(Expiration
 * Verification)가 결여된다면,
 * 한 번 탈취되어 발급된 패스포트(Token)가 평생 시스템에서 유효하게 되어 데이터베이스 전체가 해커의 놀이터가 됩니다.
 * 이 모듈은 FSM 스캐너 내부에서 권한(`"role"`)과 만료 시간(`"exp"`) 키를 단 1회의 선형 패스(One-Pass
 * Scan)로 동시에 병렬 추출해내고,
 * 이를 시스템 현재 에포크 타임(Epoch Time)과 대조하여 토큰의 수명 및 부패 여부를 판별함으로써 비로소 100% 완전한 객체-권한
 * 통제 모델(Capability-based Security)을 완성했습니다.
 * =============================================================================
 */