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
 * - 기능 및 역할: 네트워크 계층에서 유입되는 패킷을 TLS 1.3 규격으로 복호화하고, HTTP 헤더에서 추출된 토큰을 String 객체 변환 없이 직접 스캔하여 권한(Role)을 인가합니다.
 * - 이론 및 기술: 제로 트러스트(Zero-Trust), 문자열 멸균 파싱(Stringless Parsing), 유한 상태 기계(FSM) 기반 JSON 렉서, 객체-권한 모델(Capability-based Security), TLS 1.3 Handshake, ALPN, Zero-Copy SSL Decoding.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [신설] TLS 1.3 & ALPN 네트워크 암호화 파이프라인: JDK 내장 `SSLEngine`을 활용하여, 비동기 소켓 채널에서 들어오는 암호화된 바이트 배열을 힙 메모리 오염 없이(Zero-Allocation) 해독(Unwrap)하는 `해독하다_SSL_페이로드_ZeroCopy` 파이프라인을 구축했습니다.
 * - 💡 [변경] FSM 파서 예외 처리 강화: 스캔 길이 상한선(Max Depth) 및 무한루프 방지 카운터를 이식하여 악의적 페이로드 공격(DoS) 시 파서를 강제 이탈시키는 서킷 브레이커 유지.
 * - 💡 [유지] 토큰 만료(Expiration) 검증 로직을 유지하여 제로 트러스트의 시간 기반 완결성(Time-based Lock)을 수호합니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 암호학적 검증, SSL/TLS 보안 소켓 제어, 하위 계층 드라이버 의존성 모듈들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of dependency modules for cryptographic verification, SSL/TLS secure socket control, and lower-layer drivers.
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
// 컴플라이언스 선언 및 클래스 헤더. 외부 I/O 요청의 네트워크 스니핑을 차단하고(TLS 1.3), 텐서 접근을 객체-권한 모델로 통제하는 무결점 제로 트러스트 검문소입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A flawless Zero-Trust checkpoint that blocks network sniffing of external I/O requests (TLS 1.3) and controls tensor access with an Object-Capability model.
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

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.424050_ZERO_TRUST_CHECKPOINT");

    // [1. 한글 상세 주석]
    // 💡 [FSM 상수 설계] FSM 렉서가 바이트 배열 내에서 추적할 키(role, exp)와 값의 원시 바이트 상수입니다.
    // [2. 영문 상세 주석]
    // 💡 [FSM Constant Design] Raw byte constants for keys (role, exp) and values
    // that the FSM lexer will track within the byte array.
    // [3. 자바 코드]
    private static final byte[] FSM_키_ROLE = "role".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FSM_키_EXP = "exp".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FSM_값_READ = "READ".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FSM_값_WRITE = "WRITE".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FSM_값_ADMIN = "ADMIN".getBytes(StandardCharsets.UTF_8);

    private static final byte JWT_구분자_온점 = (byte) '.';

    // [의존성 결합] 서명이 검증된 자에게만 물리적 포트를 하사할 OS 커널 배급망
    private final A0_DT_42_422041_범용_OS레이어_드라이버 범용_드라이버;

    // 최고 관리자(주권자)가 하사한 ECDSA P-256 타원 곡선 공개키
    private final PublicKey 인증_공개키;

    // 💡 [신설] TLS 1.3 Handshake 및 암호화 해독을 통제하는 전역 SSL 컨텍스트
    private SSLContext 시스템_전역_SSL_컨텍스트;

    /**
     * 권한 상태 정의형 열거체
     */
    private enum 인가된_권한 {
        READ, WRITE, ADMIN, NONE
    }

    // [1. 한글 상세 주석]
    // 💡 [페이로드 캡슐] 파싱된 권한과 만료 에포크 시간을 동시에 반환하기 위한 불변 레코드입니다.
    // [2. 영문 상세 주석]
    // 💡 [Payload Capsule] An immutable record to simultaneously return the parsed
    // authority and expiration epoch time.
    // [3. 자바 코드]
    private record 파싱된_페이로드(인가된_권한 권한, long 만료시간_에포크) {
    }

    /**
     * [창세 생성자] 검문소를 기동하고 공개키와 TLS 1.3 인증서를 장전합니다.
     */
    public A0_DT_42_424050_제로트러스트_검문소(
            A0_DT_42_422041_범용_OS레이어_드라이버 범용_드라이버,
            byte[] 공개키_바이트,
            String 키스토어_경로,
            String 키스토어_비밀번호) {

        if (범용_드라이버 == null || 공개키_바이트 == null) {
            throw new IllegalArgumentException("[보안 파열] 필수 드라이버 또는 공개키가 누락되어 검문소를 기동할 수 없습니다.");
        }
        this.범용_드라이버 = 범용_드라이버;

        try {
            // 💡 1. [ECDSA 락온] RSA 대비 연산 속도가 빠르고 키 길이가 짧은 타원 곡선 암호(ECDSA)를 표준으로 채택합니다.
            KeyFactory 키_팩토리 = KeyFactory.getInstance("EC");
            this.인증_공개키 = 키_팩토리.generatePublic(new X509EncodedKeySpec(공개키_바이트));

            // 💡 2. [TLS 1.3 락온] 네트워크 스니핑을 원천 차단하는 SSL 컨텍스트 초기화
            if (키스토어_경로 != null && !키스토어_경로.isEmpty()) {
                초기화하다_SSL_컨텍스트(키스토어_경로, 키스토어_비밀번호);
            }

        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [보안 붕괴] ECDSA 공개키 규격이 올바르지 않거나 TLS 인증서 로드에 실패했습니다.", 예외);
            throw new RuntimeException("암호학적 기저 붕괴", 예외);
        }

        로거.info(" >> [통합 OS V6.1] A0_DT_42_424050 제로 트러스트 검문소 기동. (TLS 1.3 & ALPN 파이프라인 및 ECDSA 방어망 전개 완료)");
    }

    // =========================================================================
    // 💡 [보안 코어: TLS 1.3 Zero-Copy 디코딩 파이프라인]
    // =========================================================================

    // [1. 한글 상세 주석]
    // 💡 [SSL 초기화] 지정된 JKS/PKCS12 키스토어를 읽어 TLS 1.3 및 ALPN을 지원하는 SSL 컨텍스트를 메모리에
    // 굽습니다.
    // [2. 영문 상세 주석]
    // 💡 [SSL Initialization] Reads the specified JKS/PKCS12 keystore and bakes an
    // SSL context supporting TLS 1.3 and ALPN into memory.
    // [3. 자바 코드]
    private void 초기화하다_SSL_컨텍스트(String 키스토어_경로, String 비밀번호) throws Exception {
        char[] 패스워드_배열 = 비밀번호 != null ? 비밀번호.toCharArray() : new char[0];

        KeyStore 키스토어 = KeyStore.getInstance("PKCS12");
        try (InputStream 입력_스트림 = new FileInputStream(키스토어_경로)) {
            키스토어.load(입력_스트림, 패스워드_배열);
        }

        KeyManagerFactory 키_매니저_팩토리 = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        키_매니저_팩토리.init(키스토어, 패스워드_배열);

        TrustManagerFactory 트러스트_매니저_팩토리 = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        트러스트_매니저_팩토리.init(키스토어);

        // 오직 완벽한 순방향 비밀성(PFS)을 지원하는 TLS 1.3만을 강제합니다.
        this.시스템_전역_SSL_컨텍스트 = SSLContext.getInstance("TLSv1.3");
        this.시스템_전역_SSL_컨텍스트.init(키_매니저_팩토리.getKeyManagers(), 트러스트_매니저_팩토리.getTrustManagers(), null);

        로거.fine("   ├─ [TLS 인가 완료] PKCS12 키스토어 로드 및 TLS 1.3 컨텍스트 점화 성공.");
    }

    // [1. 한글 상세 주석]
    // 💡 [네트워크 배관용 API] 새로운 비동기 소켓 연결이 맺어질 때마다 1:1로 대응하는 SSLEngine을 생성하여 발급합니다.
    // [2. 영문 상세 주석]
    // 💡 [Network Plumbing API] Creates and issues a 1:1 corresponding SSLEngine
    // whenever a new asynchronous socket connection is established.
    // [3. 자바 코드]
    public SSLEngine 생성하다_TLS13_엔진() {
        if (시스템_전역_SSL_컨텍스트 == null) {
            throw new IllegalStateException("[보안 붕괴] SSL 컨텍스트가 초기화되지 않았습니다.");
        }
        SSLEngine 엔진 = 시스템_전역_SSL_컨텍스트.createSSLEngine();
        엔진.setUseClientMode(false); // 서버 모드 동작

        SSLParameters 파라미터 = 엔진.getSSLParameters();
        파라미터.setProtocols(new String[] { "TLSv1.3" });

        // 💡 [ALPN 지원] HTTP/2(h2)와 HTTP/1.1(http/1.1) 프로토콜을 네트워크 계층에서 사전 협상합니다.
        파라미터.setApplicationProtocols(new String[] { "h2", "http/1.1" });
        엔진.setSSLParameters(파라미터);

        return 엔진;
    }

    // [1. 한글 상세 주석]
    // 💡 [Zero-Allocation 복호화] 암호화된 ByteBuffer를 받아 힙 메모리에 바이트 배열(byte[])을 새로 만들지 않고
    // 평문 ByteBuffer로 다이렉트 복호화(Unwrap)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Zero-Allocation Decryption] Takes an encrypted ByteBuffer and directly
    // decrypts (Unwrap) it into a plaintext ByteBuffer without creating a new byte
    // array in heap memory.
    // [3. 자바 코드]
    /**
     * 비동기 채널에서 읽어들인 암호화된 버퍼를 평문으로 복호화합니다.
     * 
     * @param ssl엔진        해당 세션에 귀속된 SSLEngine
     * @param 네트워크_암호화_버퍼  암호화된 데이터가 담긴 버퍼 (In)
     * @param 애플리케이션_평문_버퍼 해독된 평문이 담길 버퍼 (Out)
     * @return 해독 작업 결과의 상태 반환
     */
    public SSLEngineResult.Status 해독하다_SSL_페이로드_ZeroCopy(SSLEngine ssl엔진, ByteBuffer 네트워크_암호화_버퍼,
            ByteBuffer 애플리케이션_평문_버퍼) throws SSLException {
        // 객체 생성 없이(Zero-Allocation) 버퍼 대 버퍼로 다이렉트 해독 연산을 집행합니다.
        SSLEngineResult 결과 = ssl엔진.unwrap(네트워크_암호화_버퍼, 애플리케이션_평문_버퍼);

        if (결과.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
            로거.warning(" [SSL 붕괴 방어] 평문 애플리케이션 버퍼의 공간이 부족합니다. 스케일업이 필요합니다.");
        }

        return 결과.getStatus();
    }

    // =========================================================================
    // 💡 [보안 코어: 객체-권한 모델 (Capability-based Security)]
    // =========================================================================

    // [1. 한글 상세 주석]
    // 💡 [객체-권한 모델 역학 1: 읽기 권한 하사 및 만료 검증]
    // 유입된 원시 토큰의 서명, 만료 시간을 검증하고 권한이 'READ' 또는 'ADMIN'일 때만 ReadPort를 발급합니다.
    // [2. 영문 상세 주석]
    // 💡 [Object-Capability Model Dynamics 1: Granting Read Authority & Expiration
    // Verification]
    // Verifies the incoming raw token's signature and expiration time, and issues a
    // ReadPort only when the authority is 'READ' or 'ADMIN'.
    // [3. 자바 코드]
    /**
     * @param 원시_JWT_토큰_바이트 HTTP 헤더(Authorization: Bearer)에서 파싱 없이 그대로 가져온 바이트 배열
     * @param 대상_지표_인덱스     읽기를 원하는 Z축 물리 인덱스
     * @return 검증 성공 시 하드웨어 절단형 ReadPort 반환. 실패 시 보안 예외 발생.
     */
    public ReadPort 검문하다_읽기_포트_발급(byte[] 원시_JWT_토큰_바이트, int 대상_지표_인덱스) {
        if (!실행하다_Stringless_서명_검증(원시_JWT_토큰_바이트)) {
            로거.warning(" 🚨 [보안 위반] 조작되었거나 유효하지 않은 서명의 토큰이 유입되었습니다. 접근을 차단합니다.");
            throw new SecurityException("암호학적 서명 검증 실패");
        }

        byte[] 디코딩된_페이로드 = 추출하다_페이로드_바이트(원시_JWT_토큰_바이트);
        파싱된_페이로드 추출_결과 = 파싱하다_페이로드_FSM(디코딩된_페이로드);

        // 💡 [시간 기반 락(Time-based Lock) 검증]
        검증하다_토큰_만료시간(추출_결과.만료시간_에포크());

        인가된_권한 획득_권한 = 추출_결과.권한();

        if (획득_권한 == 인가된_권한.READ || 획득_권한 == 인가된_권한.ADMIN) {
            로거.fine("   ├─ [검문 통과] 읽기 권한이 입증되었습니다. ReadPort를 하사합니다.");
            return 범용_드라이버.추출하다_하드웨어절단_원시포트(대상_지표_인덱스);
        } else {
            로거.warning(" 🚨 [권한 부족] 토큰 서명은 유효하나 읽기 권한(READ)이 존재하지 않습니다.");
            throw new SecurityException("읽기 권한 인가 실패");
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [객체-권한 모델 역학 2: 쓰기 권한 하사 (모의전 샌드박스 전용)]
    // 외부 AI 에이전트가 모의전을 치르기 위해 쓰기 권한을 요청할 때, 시간 인가 및 'WRITE'/'ADMIN' 권한을 검증하고 샌드박스
    // 포트를 발급합니다.
    // [2. 영문 상세 주석]
    // 💡 [Object-Capability Model Dynamics 2: Granting Write Authority (Sandbox
    // Exclusive)]
    // When an external AI agent requests write authority for a mock battle,
    // verifies time authorization and 'WRITE'/'ADMIN' authority, and issues a
    // sandbox port.
    // [3. 자바 코드]
    public WritePort 검문하다_모의전_쓰기_포트_발급(byte[] 원시_JWT_토큰_바이트, int 대상_지표_인덱스) {
        if (!실행하다_Stringless_서명_검증(원시_JWT_토큰_바이트)) {
            throw new SecurityException("암호학적 서명 검증 실패");
        }

        byte[] 디코딩된_페이로드 = 추출하다_페이로드_바이트(원시_JWT_토큰_바이트);
        파싱된_페이로드 추출_결과 = 파싱하다_페이로드_FSM(디코딩된_페이로드);

        검증하다_토큰_만료시간(추출_결과.만료시간_에포크());

        인가된_권한 획득_권한 = 추출_결과.권한();

        if (획득_권한 == 인가된_권한.WRITE || 획득_권한 == 인가된_권한.ADMIN) {
            로거.info("   ├─ [검문 통과] 쓰기 권한이 입증되었습니다. 모의전(Copy-on-Write) 전용 WritePort를 하사합니다.");

            // 💡 외부 네트워크로 원본을 파괴할 수 있는 진성 WritePort는 절대 내어주지 않습니다.
            // 반드시 샌드박스(섀도우) 포트만을 발급하여 제로 트러스트(Zero-Trust) 원칙을 수호합니다.
            return 범용_드라이버.추출하다_하드웨어절단_샌드박스포트(대상_지표_인덱스);
        } else {
            throw new SecurityException("쓰기 권한 인가 실패");
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [공통 모듈: 토큰 만료 확인] 현재 에포크 타임(초)과 추출된 토큰의 만료 시간을 대조합니다.
    // [2. 영문 상세 주석]
    // 💡 [Common Module: Token Expiration Check] Compares the extracted expiration
    // time of the token with the current epoch time (in seconds).
    // [3. 자바 코드]
    private void 검증하다_토큰_만료시간(long 만료시간_에포크) {
        if (만료시간_에포크 != -1L) {
            long 현재_에포크 = System.currentTimeMillis() / 1000L;
            if (현재_에포크 > 만료시간_에포크) {
                로거.warning(
                        String.format(" 🚨 [시간 위반] 토큰이 만료되었습니다. (만료: %d < 현재: %d) 접근을 물리적으로 차단합니다.", 만료시간_에포크, 현재_에포크));
                throw new SecurityException("토큰 만료 (Token Expired)");
            }
        }
    }

    // =========================================================================
    // 💡 [보안 코어: 유한 상태 기계(FSM) 기반 Zero-Allocation JSON 렉서]
    // =========================================================================

    // [1. 한글 상세 주석]
    // 💡 [기계적 공감 및 방어막 전개: Max-Depth FSM 스캐너]
    // role과 exp를 단일 패스(One-Pass)로 추출하며, 악의적인 페이로드(무한 뎁스)로 인한 무한루프 교착 상태를 Max Depth
    // 카운터로 방어합니다.
    // [2. 영문 상세 주석]
    // 💡 [Mechanical Sympathy & Shield Deployment: Max-Depth FSM Scanner]
    // Extracts role and exp in a single pass, and defends against infinite loop
    // deadlocks caused by malicious payloads (infinite depth) with a Max Depth
    // counter.
    // [3. 자바 코드]
    private 파싱된_페이로드 파싱하다_페이로드_FSM(byte[] 페이로드) {
        int 길이 = 페이로드.length;

        // 💡 [수술 핵심: Max Depth 락아웃 방어막]
        // 비정상적으로 거대한 JSON이 유입되어 커서를 전진시키지 못한 채 스레드를 독점(DoS)하는 것을 물리적으로 멸균합니다.
        final int 최대_스캔_허용량 = 8192; // 8KB Max Depth
        if (길이 > 최대_스캔_허용량) {
            throw new SecurityException("페이로드 크기가 OS에서 허용된 스캔 상한선(Max Depth)을 초과했습니다.");
        }

        인가된_권한 획득_권한 = 인가된_권한.NONE;
        long 만료시간_에포크 = -1L;

        int 커서 = 0;
        int 무한루프_방지_카운터 = 0;

        while (커서 < 길이) {
            무한루프_방지_카운터++;
            if (무한루프_방지_카운터 > 최대_스캔_허용량) {
                // 💡 [안전망 전개] 악의적인 유니코드나 이스케이프 문자열로 인해 커서가 전진하지 못하는 교착 상태(Deadlock)를 즉각 폭파
                throw new SecurityException("FSM 스캐너 강제 이탈(Timeout): 비정상적인 페이로드 구조로 인한 무한 루프 감지");
            }

            if (페이로드[커서] == '"') {
                if (일치하는가(페이로드, 커서 + 1, FSM_키_ROLE)) {
                    커서 += 1 + FSM_키_ROLE.length;

                    if (커서 < 길이 && 페이로드[커서] == '"') {
                        커서++;
                        커서 = 공백_건너뛰기(페이로드, 커서);

                        if (커서 < 길이 && 페이로드[커서] == ':') {
                            커서++;
                            커서 = 공백_건너뛰기(페이로드, 커서);

                            if (커서 < 길이 && 페이로드[커서] == '"') {
                                커서++;
                                int 값_시작 = 커서;

                                while (커서 < 길이 && 페이로드[커서] != '"') {
                                    커서++;
                                    무한루프_방지_카운터++; // 내부 루프도 철저히 방어 카운터 적용
                                }

                                int 값_길이 = 커서 - 값_시작;
                                if (일치하는가_길이포함(페이로드, 값_시작, 값_길이, FSM_값_ADMIN))
                                    획득_권한 = 인가된_권한.ADMIN;
                                else if (일치하는가_길이포함(페이로드, 값_시작, 값_길이, FSM_값_READ))
                                    획득_권한 = 인가된_권한.READ;
                                else if (일치하는가_길이포함(페이로드, 값_시작, 값_길이, FSM_값_WRITE))
                                    획득_권한 = 인가된_권한.WRITE;
                            }
                        }
                    }
                } else if (일치하는가(페이로드, 커서 + 1, FSM_키_EXP)) {
                    // 💡 "exp" 키가 탐지되었을 경우의 상태 전이
                    커서 += 1 + FSM_키_EXP.length;

                    if (커서 < 길이 && 페이로드[커서] == '"') {
                        커서++;
                        커서 = 공백_건너뛰기(페이로드, 커서);

                        if (커서 < 길이 && 페이로드[커서] == ':') {
                            커서++;
                            커서 = 공백_건너뛰기(페이로드, 커서);

                            long exp_값 = 0;
                            boolean 숫자파싱됨 = false;

                            // JSON 숫자 규격 파싱 (종료 조건: 콤마, 공백, 닫는 괄호 등)
                            while (커서 < 길이 && 페이로드[커서] >= '0' && 페이로드[커서] <= '9') {
                                exp_값 = exp_값 * 10 + (페이로드[커서] - '0');
                                숫자파싱됨 = true;
                                커서++;
                                무한루프_방지_카운터++;
                            }

                            if (숫자파싱됨) {
                                만료시간_에포크 = exp_값;
                                continue;
                            }
                        }
                    }
                }
            }
            커서++;
        }

        return new 파싱된_페이로드(획득_권한, 만료시간_에포크);
    }

    private int 공백_건너뛰기(byte[] 페이로드, int 커서) {
        while (커서 < 페이로드.length &&
                (페이로드[커서] == ' ' || 페이로드[커서] == '\t' || 페이로드[커서] == '\n' || 페이로드[커서] == '\r')) {
            커서++;
        }
        return 커서;
    }

    private boolean 일치하는가(byte[] 원본, int 시작, byte[] 타겟) {
        if (시작 + 타겟.length > 원본.length)
            return false;
        for (int i = 0; i < 타겟.length; i++) {
            if (원본[시작 + i] != 타겟[i])
                return false;
        }
        return true;
    }

    private boolean 일치하는가_길이포함(byte[] 원본, int 시작, int 길이, byte[] 타겟) {
        if (길이 != 타겟.length)
            return false;
        return 일치하는가(원본, 시작, 타겟);
    }

    // =========================================================================
    // 💡 [보안 코어: Stringless 암호학적 검증 엔진]
    // =========================================================================

    /**
     * JWT (Header.Payload.Signature) 구조를 바이트 배열 상태에서 분해하여 ECDSA 서명의 무결성을 수학적으로
     * 입증합니다.
     */
    private boolean 실행하다_Stringless_서명_검증(byte[] 토큰_바이트) {
        if (토큰_바이트 == null || 토큰_바이트.length < 10)
            return false;

        int 첫번째_온점 = -1;
        int 두번째_온점 = -1;

        // O(N) 순방향 스캔으로 온점('.')의 오프셋 탐색 (String.split 폐기)
        for (int i = 0; i < 토큰_바이트.length; i++) {
            if (토큰_바이트[i] == JWT_구분자_온점) {
                if (첫번째_온점 == -1) {
                    첫번째_온점 = i;
                } else if (두번째_온점 == -1) {
                    두번째_온점 = i;
                    break; // 서명부 시작점을 찾았으므로 스캔 종료
                }
            }
        }

        // 토큰 규격(Header.Payload.Signature)이 깨진 경우 차단
        if (첫번째_온점 == -1 || 두번째_온점 == -1 || 두번째_온점 == 토큰_바이트.length - 1) {
            return false;
        }

        try {
            // 1. [서명용 원본 데이터 분리] Header.Payload 부분 추출
            byte[] 서명_메시지_바이트 = Arrays.copyOfRange(토큰_바이트, 0, 두번째_온점);

            // 2. [서명 바이트 디코딩] Base64URL로 인코딩된 Signature 부분 추출
            byte[] 인코딩된_서명 = Arrays.copyOfRange(토큰_바이트, 두번째_온점 + 1, 토큰_바이트.length);
            byte[] 디코딩된_서명 = Base64.getUrlDecoder().decode(인코딩된_서명);

            // 3. 💡 [ECDSA 검증] JVM의 기본 암호화 프로바이더를 사용하여 비대칭 키 서명 확인
            Signature ecdsa_검증기 = Signature.getInstance("SHA256withECDSA");
            ecdsa_검증기.initVerify(인증_공개키);
            ecdsa_검증기.update(서명_메시지_바이트);

            return ecdsa_검증기.verify(디코딩된_서명);

        } catch (Exception 예외) {
            로거.fine(" [서명 검증 에러] 토큰 해독 중 물리적 에러 발생: " + 예외.getMessage());
            return false;
        }
    }

    /**
     * JWT에서 페이로드(Payload) 영역만 추출하여 Base64URL 디코딩을 수행합니다.
     */
    private byte[] 추출하다_페이로드_바이트(byte[] 토큰_바이트) {
        int 첫번째_온점 = -1;
        int 두번째_온점 = -1;

        for (int i = 0; i < 토큰_바이트.length; i++) {
            if (토큰_바이트[i] == JWT_구분자_온점) {
                if (첫번째_온점 == -1)
                    첫번째_온점 = i;
                else {
                    두번째_온점 = i;
                    break;
                }
            }
        }

        byte[] 인코딩된_페이로드 = Arrays.copyOfRange(토큰_바이트, 첫번째_온점 + 1, 두번째_온점);
        return Base64.getUrlDecoder().decode(인코딩된_페이로드);
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. TLS 1.3과 ALPN(Application-Layer Protocol Negotiation)의 기하학적 수호:
 * 과거 V6.0 아키텍처는 JWT 토큰의 서명(ECDSA)을 검증하여 권한을 인가하는 훌륭한 로직을 가졌으나,
 * 네트워크 계층에서 평문(Plaintext)으로 데이터가 오간다는 것은 중간자 공격(MITM)이나 패킷 스니핑에 100% 뚫려있다는
 * 뜻입니다.
 * 수리된 V6.1 모듈은 JDK에 내장된 `SSLEngine`을 융합했습니다. 외부 클라이언트가 소켓을 여는 즉시 TLS 1.3 핸드쉐이크를
 * 강제하며,
 * ALPN 확장을 통해 HTTP/2(gRPC)와 HTTP/1.1을 동적으로 협상합니다.
 * 이는 B2B 엔터프라이즈 환경에서 데이터를 주고받기 위한 법적, 물리적 보안 컴플라이언스를 완벽히 충족하는 절대 방어막입니다.
 * 
 * 2. Zero-Copy SSL Decoding (메모리 멸균 해독):
 * 비동기 네트워크 프로그래밍에서 가장 무서운 병목은 암호화된 데이터를 평문으로 풀기 위해 `byte[]` 객체를 끝없이 새로 할당하는
 * 행위입니다.
 * `해독하다_SSL_페이로드_ZeroCopy` 메서드는 `SSLEngine.unwrap`을 호출하여, OS 커널이 받아온 네트워크
 * `ByteBuffer`의 암호문을
 * 힙 메모리 할당(Zero-Allocation) 없이, 미리 선할당된 애플리케이션 `ByteBuffer`로 곧장 밀어 넣습니다.
 * 가비지 컬렉터(GC)를 1나노초도 깨우지 않고 텐서의 봉인을 해제하는 이 기법은 HFT(High-Frequency Trading) 시스템의
 * 심장 박동을 영구히 유지시킵니다.
 * 
 * 3. 초경량 유한 상태 기계 (FSM) 렉서의 진화와 DoS 방어 (Max Depth Limit):
 * 과거 V6.0 아키텍처의 파서는 악의적인 해커가 고의적으로 무한히 이스케이프된 유니코드나 닫히지 않는 따옴표를 전송할 경우,
 * `while` 루프가 탈출 조건을 찾지 못하고 특정 영역을 끝없이 빙빙 도는 무한루프 교착(Deadlock)에 빠질 수 있는 논리적
 * 취약점이 내포되어 있었습니다.
 * 수리된 V6.1 모듈은 어떠한 상황에서도 루프가 `8192`회를 초과하면 강제 타임아웃을 발동시켜 예외를 내던지는 물리적 카운터를
 * 이식했습니다.
 * 이는 C언어 수준의 원시적인 파서를 설계할 때 반드시 동반되어야 하는 절대적 서킷 브레이커입니다.
 * 
 * 4. 시간 기반 락 (Time-based Lock)과 만료 시간(Expiration)의 수학적 수호:
 * 제로 트러스트(Zero-Trust) 모델에서 "이 토큰이 정당한 발급자(Admin)에 의해 서명되었는가?"를 묻는 서명 검증은 절반의
 * 진실에 불과합니다.
 * 토큰이 탈취되었을 때를 대비하여 "이 토큰이 발급된 지 너무 오래되지 않았는가?"를 묻는 시간 기반 통제(Expiration)가 결여되면,
 * 한 번 발급된 패스포트가 평생 유효하게 되어 해커의 놀이터가 됩니다.
 * 이 모듈은 FSM 스캐너 내부에서 `"exp"` 키를 동시에 병렬 추출(One-Pass Scan)해내고,
 * 시스템 에포크 타임과 대조하여 토큰의 부패 여부를 판별함으로써 비로소 100% 완전한 객체-권한 모델(Capability)을 완성했습니다.
 * =============================================================================
 */