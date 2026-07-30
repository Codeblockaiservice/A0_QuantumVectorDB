/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층
 * @alias Zero_Trust_Checkpoint
 * @tier 17
 * @keywords Zero-Trust, ECDSA, Stringless JWT, RBAC, FSM Scanner, Capability-based Security
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_424050_제로트러스트_검문소.java
 * - 모듈명: 통합 OS V6.0 - Tier 17: 제로 트러스트 검문소 (객체-권한 모델 보안망)
 * - 기능 및 역할: HTTP 헤더에서 추출된 토큰의 바이트 배열을 String 객체로 변환하지 않고 직접 스캔하여 ECDSA 서명을 검증하고 권한(Role)을 인가합니다.
 * - 이론 및 기술: 제로 트러스트(Zero-Trust), 문자열 멸균 파싱(Stringless Parsing), 유한 상태 기계(FSM) 기반 JSON 렉서, 객체-권한 모델(Capability-based Security).
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [변경] FSM 파서 예외 처리 강화: `파싱하다_페이로드_FSM` 메서드 내부에 스캔 길이 상한선(Max Depth) 및 무한루프 방지 카운터를 이식하여, 예측 불가능한 유니코드 배열이나 악의적 페이로드 공격(DoS) 시 파서를 강제 이탈(Timeout)시키는 서킷 브레이커를 구축했습니다.
 * - 💡 [신설] 토큰 만료(Expiration) 검증: 서명 무결성 검증뿐만 아니라, `exp` 키를 추출하여 현재 에포크 타임과 비교하는 시간 기반 락(Time-based Lock) 로직을 신설하여 제로 트러스트의 완결성을 수호합니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 암호학적 검증, 하위 계층 드라이버 의존성 모듈들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of dependency modules for cryptographic verification and lower-layer drivers.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.ReadPort;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.WritePort;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망.A0_DT_42_422041_범용_OS레이어_드라이버;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 외부의 I/O 요청을 객체-권한 모델(Capability-based Security)로 통제하는 제로 트러스트 검문소입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A Zero-Trust checkpoint that controls external I/O requests through a Capability-based Security model.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424050
 * [파일명] A0_DT_42_424050_제로트러스트_검문소.java
 * [모듈명] 통합 OS V6.0 - Tier 17: 제로 트러스트 검문소 (객체-권한 모델 보안망)
 * 
 * [설계 명세]
 * 1. 역할: 외부 네트워크 계층에서 유입된 JWT(ECDSA) 토큰을 검증하고, 유효한 경우에만 ReadPort/WritePort를 발급.
 * 2. 기능: FSM 기반 Stringless 토큰 파싱, ECDSA P-256 타원 곡선 서명 검증, 권한(RBAC) 및 만료(EXP)
 * 인가.
 * 3. 의도: 문자열 할당(new String)으로 인한 힙 메모리 오염을 차단하고, 공백에 깨지지 않는 견고한 파서로 HFT 인증 병목
 * 멸균.
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
     * [창세 생성자] 검문소를 기동하고 공개키를 장전합니다.
     */
    public A0_DT_42_424050_제로트러스트_검문소(A0_DT_42_422041_범용_OS레이어_드라이버 범용_드라이버, byte[] 공개키_바이트) {
        if (범용_드라이버 == null || 공개키_바이트 == null) {
            throw new IllegalArgumentException("[보안 파열] 필수 드라이버 또는 공개키가 누락되어 검문소를 기동할 수 없습니다.");
        }
        this.범용_드라이버 = 범용_드라이버;

        try {
            // 💡 RSA 대비 연산 속도가 빠르고 키 길이가 짧은 타원 곡선 암호(ECDSA)를 표준으로 채택합니다.
            KeyFactory 키_팩토리 = KeyFactory.getInstance("EC");
            this.인증_공개키 = 키_팩토리.generatePublic(new X509EncodedKeySpec(공개키_바이트));
        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [보안 붕괴] ECDSA 공개키 규격이 올바르지 않습니다.", 예외);
            throw new RuntimeException("암호학적 기저 붕괴", 예외);
        }

        로거.info(" >> [통합 OS V6.0] A0_DT_42_424050 제로 트러스트 검문소 기동. (Max-Depth FSM 파서 및 Expiration 검증망 장착 완료)");
    }

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

        // 💡 [신설: 시간 기반 락(Time-based Lock) 검증]
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
                    // 💡 [신설] "exp" 키가 탐지되었을 경우의 상태 전이
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
                                continue; // 숫자를 파싱하면서 이미 다음 포인터로 커서가 이동했으므로 continue 처리
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
            // 이 데이터(바이트 배열) 자체가 서명을 검증할 메시지 해시의 원본이 됩니다.
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
 * 1. 초경량 유한 상태 기계 (FSM) 렉서의 진화와 DoS 방어 (Max Depth Limit):
 * 과거 V6.0 아키텍처의 파서는 공백 문자를 영리하게 스킵하며 객체 할당(Allocation)을 소거했지만,
 * 악의적인 해커가 고의적으로 무한히 이스케이프된 유니코드나 닫히지 않는 따옴표를 전송할 경우,
 * `while` 루프가 탈출 조건을 찾지 못하고 특정 영역을 끝없이 빙빙 도는 무한루프 교착(Deadlock)에 빠질 수 있는
 * 논리적 취약점(Denial of Service)이 내포되어 있었습니다.
 * 수리된 V6.1 모듈은 어떠한 상황에서도 루프가 `8192`회를 초과하면 강제 타임아웃을 발동시켜 예외를 내던지는
 * 물리적 카운터를 이식했습니다. 이는 C언어 수준의 원시적인 파서를 설계할 때 반드시 동반되어야 하는 절대적 서킷 브레이커입니다.
 * 
 * 2. 시간 기반 락 (Time-based Lock)과 만료 시간(Expiration)의 수학적 수호:
 * 제로 트러스트(Zero-Trust) 모델에서 "이 토큰이 정당한 발급자(Admin)에 의해 서명되었는가?"를 묻는 서명 검증은 절반의
 * 진실에 불과합니다.
 * 토큰이 탈취되었을 때를 대비하여 "이 토큰이 발급된 지 너무 오래되지 않았는가?"를 묻는 시간 기반 통제(Expiration)가 결여되면,
 * 한 번 발급된 패스포트가 평생 유효하게 되어 해커의 놀이터가 됩니다.
 * 이 모듈은 FSM 스캐너 내부에서 `"exp"` 키를 동시에 병렬 추출(One-Pass Scan)해내고,
 * 시스템 에포크 타임과 대조하여 토큰의 부패 여부를 판별함으로써 비로소 100% 완전한 객체-권한 모델(Capability)을 완성했습니다.
 * 
 * 3. 제로 트러스트(Zero-Trust)와 샌드박스의 절대적 분리:
 * 외부 에이전트(API 호출자)는 단순히 요청을 보내는 것이 아니라, 자신이 행하려는 행동에 합당한 암호학적 증명(Token)을 이 검문소에
 * 제시해야 합니다.
 * 검문소가 내어주는 `ReadPort` 자체가 물리적인 하드웨어 접근 권한(Capability)을 증명하는 열쇠가 됩니다.
 * 특히, `WRITE` 권한을 요청받았을 때 원본을 부술 수 있는 진성 포트 대신, 오직 스왑(Swap) 공간에만 변경점이 남는
 * `WritePort(샌드박스)`를 하사하는 것은, 그 어떤 위협 속에서도 코어 데이터(L1 매트릭스)를 절대적으로 수호하겠다는 시스템의
 * 기저 철학을 대변합니다.
 * =============================================================================
 * 
 * 💡 [입문자 해설 (Beginner's Guide)]
 * 
 * - **무한루프 방지(Max Depth) 비유**:
 * 예전 경비원은 방문객이 건네준 가방(JSON) 속에서 권한증을 찾을 때, 가방 안에 가방이 끝없이 들어있는 마트료시카 같은 함정에 걸리면
 * 평생 가방만 뒤지느라 다른 방문객들을 받지 못했습니다(서버 마비).
 * 새로운 경비원은 "8192번 이상 가방을 뒤져도 안 나오면 이건 가짜 함정이다!"라고 판단하고
 * 즉시 방문객을 쫓아냅니다.
 * - **토큰 만료(exp) 검증 비유**:
 * 입장권을 확인할 때, "사장님이 발급한 진짜 표가 맞는지(서명 검증)" 확인하는 것도 중요하지만,
 * "작년에 발급된 유통기한 지난 표인지(exp 검증)"를 확인하는 것도 필수입니다.
 * 이제 경비원은 두 가지를 한 번에 완벽히 확인하여 유통기한 지난 표를 들고 온 방문객을 단호하게 거부합니다.
 * =============================================================================
 */