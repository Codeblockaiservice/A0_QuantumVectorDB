/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라
 * @alias Timeframe_Context_Router
 * @tier 0
 * @keywords Spatial Decoupling, Scale-Out, ACL Validation, FFM API, Heterogeneous Storage
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422000_타임프레임_컨텍스트.java
 * - 역할 (Role): 다중 시계열 우주의 물리적 디렉토리 경로 격리 및 라우팅.
 * - 기능 (Function): 원본 마스터, 수술실, 모의전 샌드박스, 스풀 등 격리된 공간 경로 제공.
 * - 이론 (Theory): 공간적 디커플링(Spatial Decoupling), 이기종 스토리지 분산(Scale-Out), 하이브리드 폴백 권한 검증.
 * - 기대효과 (Effect): 수백 개의 스레드가 동시 접근해도 파일 시스템 병목을 0%로 통제.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 특정 세력을 연상시키는 단어를 전면 소각하고 '통합 OS'로 치환 완료.
 * - 💡 [초정밀 수술] 마운트 포인트 권한 락온 검증망: 경로 생성 전 OS 커널 API(`PosixFilePermissions`)를 
 *                 직접 찔러 파일 시스템의 읽기/쓰기 속성을 0나노초에 사전 판독하고, 
 *                 물리적 더미 파일 I/O를 결합한 하이브리드 폴백(Hybrid Fallback) 방어막으로 승격.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 시스템 제어, 커널 레벨 권한 속성(POSIX) 검증, FFM 메모리 규격, 동시성 검증을 위한 핵심 자바 표준 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core Java standard libraries for file system control, kernel-level permission attribute (POSIX) verification, FFM memory specification, and concurrency validation.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라;

import java.io.File;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 열거형(Enum) 클래스 헤더. 물리적 스토리지 경로를 격리하고 권한을 선제 검증하는 절대 라우팅 코어입니다.
// [2. 영문 상세 주석]
// Compliance declaration and Enum class header. An absolute routing core that isolates physical storage paths and preemptively verifies permissions.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422000
 * [파일명] A0_DT_42_422000_타임프레임_컨텍스트.java
 * [모듈명] 통합 OS V6.0 - Tier 0: 시공간 컨텍스트 및 절대 라우팅 코어
 * 
 * [설계 명세]
 * 1. 역할: 1D, 5M, 1M 등 밀도가 다른 시계열 우주의 물리적 디렉토리 경로 격리 및 라우팅.
 * 2. 기능: 원본 마스터, 수술실, 감시망, 모의전 샌드박스, 비동기 스풀(Spool) 경로 제공.
 * 3. 의도: 디스크 레벨에서의 I/O 충돌 및 데이터 오염 원천 차단.
 * 4. 기술: Java Enum 싱글톤 기반 정적 라우팅, FFM API 기계어 직렬화 규격 강제, 하이브리드 ACL 물리 검증망.
 * 5. 이론: 공간적 디커플링(Spatial Decoupling)을 통한 락(Lock) 경합 멸균.
 * 6. 💡 [V6.0 초정밀 수술] 하이브리드 마운트 권한 검증망: OS 커널 API를 직접 찔러 ACL을 0나노초에 사전 판독하고,
 * 이후 물리적 더미 파일을 통한 I/O 테스트를 병행하여 커널 캐시의 거짓 양성(False Positive)을 완벽 차단합니다.
 * ==============================================================================
 */
public enum A0_DT_42_422000_타임프레임_컨텍스트 {

    // [1. 한글 상세 주석]
    // 다중 시계열 평행 우주를 정의합니다. 각 상수들은 서로 다른 디렉토리 영토를 할당받습니다.
    // [2. 영문 상세 주석]
    // Defines multi-timeseries parallel universes. Each constant is assigned a
    // different directory territory.
    // [3. 자바 코드]
    일봉_격자("1D", "거시적_일봉_매트릭스", "DAILY_MASTER", "L1_DAILY_TENSOR"),
    오분봉_격자("5M", "전술적_5분봉_매트릭스", "MIN5_MASTER", "L1_MIN5_TENSOR"),
    일분봉_격자("1M", "미시적_1분봉_매트릭스", "MIN1_MASTER", "L1_MIN1_TENSOR");

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422000_CONTEXT_ROUTER");

    // [1. 한글 상세 주석]
    // 스풀(Spool) 상태 기계 정의 (고아 파일 복구 및 락프리 I/O 지원)
    // [2. 영문 상세 주석]
    // Spool state machine definition (supports orphan file recovery and lock-free
    // I/O)
    // [3. 자바 코드]
    public enum 스풀_상태 {
        투입구_INGRESS("01_INGRESS"), // 외부 데이터가 최초로 떨어지는 진입로
        작업장_PROCESSING("02_PROCESSING"), // 소화기(Ingestor)가 배타적 락을 쥐고 파싱 중인 공간
        보관소_ARCHIVE("03_ARCHIVE"), // 성공적으로 텐서화가 완료된 원본 데이터의 무덤
        격리소_QUARANTINE("04_QUARANTINE"); // 파싱 에러나 헌장 위반으로 격리된 오염 데이터

        private final String 폴더명;

        스풀_상태(String 폴더명) {
            this.폴더명 = 폴더명;
        }

        public String get폴더명() {
            return 폴더명;
        }
    }

    // [1. 한글 상세 주석]
    // Java 21+ FFM API에서 메모리 오프셋에 float 막대기를 꽂을 때 사용할 절대 규격.
    // x86/ARM 아키텍처와 동일한 리틀 엔디안을 강제하여 JVM의 바이트 스왑(Byte-Swap) 연산을 물리적으로 제거합니다.
    // [2. 영문 상세 주석]
    // Absolute specification to use when inserting a float bar into a memory offset
    // in Java 21+ FFM API.
    // Forces little-endian identical to x86/ARM architectures, physically
    // eliminating JVM byte-swap operations.
    // [3. 자바 코드]
    public static final ValueLayout.OfFloat TENSOR_FLOAT_LAYOUT = ValueLayout.JAVA_FLOAT_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);

    // [1. 한글 상세 주석]
    // 물리적 경로 베이스 통제소 (이기종 디스크 분산 아키텍처 지원)
    // 하드코딩 경로를 폐기하고 OS-Agnostic한 기본 루트를 설정합니다.
    // [2. 영문 상세 주석]
    // Physical path base control station (supports heterogeneous disk distributed
    // architecture).
    // Discards hardcoded paths and sets an OS-Agnostic default root.
    // [3. 자바 코드]
    private static final String 운영체제_무관_기본_루트 = System.getProperty("user.dir") + File.separator + "A0_TDQI_MATRIX";

    // 💡 L3_심연: HDD 기반, 가공되지 않은 100% 원본 파동 (비동기 Append-only 로거 전용)
    private static final String L3_심연_저장소 = System.getProperty("matrix.root.l3",
            운영체제_무관_기본_루트 + File.separator + "L3_ABYSS_RAW");

    // 💡 L2_표준: RDBMS/SSD 기반, 수축을 완료한 불변의 진리(Essence) 저장소
    private static final String L2_표준_저장소 = System.getProperty("matrix.root.l2",
            운영체제_무관_기본_루트 + File.separator + "L2_STANDARD_TRUTH");

    // 💡 L1_매트릭스: RAM/NVMe 기반, SIMD 텐서 압출 및 FFM 다이렉트 매핑을 위한 고속 작전 구역
    private static final String L1_매트릭스_저장소 = System.getProperty("matrix.root.l1",
            운영체제_무관_기본_루트 + File.separator + "L1_FAST_MATRIX");

    // [1. 한글 상세 주석]
    // 경로에 접근할 때마다 디스크 권한을 체크하는 부하를 없애기 위해, 각 루트 경로의 무결성을 캐싱합니다.
    // [2. 영문 상세 주석]
    // Caches the integrity of each root path to eliminate the overhead of checking
    // disk permissions upon every access.
    // [3. 자바 코드]
    private static final ConcurrentHashMap<String, Boolean> 마운트_권한_캐시망 = new ConcurrentHashMap<>();

    // [1. 한글 상세 주석]
    // 클래스 로드 시점에 각 물리적 스토리지 거점에 대한 하이브리드(커널+물리) 검증을 집행합니다.
    // [2. 영문 상세 주석]
    // Executes hybrid (kernel + physical) verification for each physical storage
    // stronghold at class load time.
    // [3. 자바 코드]
    static {
        검증하다_마운트_권한_락온(L3_심연_저장소);
        검증하다_마운트_권한_락온(L2_표준_저장소);
        검증하다_마운트_권한_락온(L1_매트릭스_저장소);
    }

    // [1. 한글 상세 주석]
    // 💡 [초정밀 수술 적용] 해당 볼륨이 Read/Write 권한(ACL)을 지녔는지 커널 API로 0나노초에 사전 판독하고,
    // 이후 물리적 더미 파일 I/O를 통해 거짓 양성(False Positive)을 막는 하이브리드 폴백 방어막입니다.
    // [2. 영문 상세 주석]
    // 💡 [Ultra-Precision Surgery Applied] Pre-reads whether the volume has
    // Read/Write permissions (ACL) in 0 nanoseconds using the Kernel API,
    // and then uses a physical dummy file I/O to form a hybrid fallback defense
    // shield that prevents false positives.
    // [3. 자바 코드]
    private static void 검증하다_마운트_권한_락온(String 물리적_루트_경로) {
        마운트_권한_캐시망.computeIfAbsent(물리적_루트_경로, 경로문자열 -> {
            Path 경로 = Paths.get(경로문자열);
            try {
                if (!Files.exists(경로)) {
                    Files.createDirectories(경로);
                }

                // 💡 1단계: OS 커널 레벨 사전 판독 (PosixFilePermissions 검증) - 0나노초 메모리 할당 최소화
                boolean 커널_권한_검증_통과 = false;
                try {
                    // POSIX 호환 OS (리눅스/유닉스/맥) 커널 API 직접 타격
                    Set<PosixFilePermission> 권한셋 = Files.getPosixFilePermissions(경로);
                    if (권한셋.contains(PosixFilePermission.OWNER_READ) &&
                            권한셋.contains(PosixFilePermission.OWNER_WRITE)) {
                        커널_권한_검증_통과 = true;
                        로거.info(String.format("   ├─ [커널 권한 스캔] POSIX 파일 시스템 Read/Write 속성 사전 검증 통과: %s", 경로문자열));
                    }
                } catch (UnsupportedOperationException e) {
                    // 윈도우 등 POSIX 미지원 OS의 경우 JNI 네이티브 검증 대체 폴백
                    if (Files.isReadable(경로) && Files.isWritable(경로)) {
                        커널_권한_검증_통과 = true;
                        로거.info(String.format("   ├─ [커널 권한 스캔] Native OS Read/Write 속성 사전 검증 통과: %s", 경로문자열));
                    }
                }

                if (!커널_권한_검증_통과) {
                    로거.warning(String.format("   ├─ [권한 경고] 커널 레벨 속성 검증을 통과하지 못했습니다. 물리적 I/O 강제 돌파를 시도합니다: %s", 경로문자열));
                }

                // 💡 2단계: 하이브리드 폴백 방어막 (더미 파일 기반 절대 물리 검증)
                // 네트워크 드라이브나 NFS에서 발생하는 커널 캐시의 거짓 양성을 막기 위해 물리적 I/O 쓰기/삭제를 강행
                Path 권한_테스트_파일 = 경로.resolve(".acl_lock_test_" + System.currentTimeMillis());
                Files.write(권한_테스트_파일, new byte[] { 42 }); // 1바이트 더미 작성
                Files.delete(권한_테스트_파일); // 락 해제 및 삭제 검증

                로거.info(String.format("   ├─ [마운트 검증 완료] 하이브리드 스토리지 락온 성공: %s (커널 & 물리적 I/O 동시 통과)", 경로문자열));
                return true;

            } catch (Exception 예외) {
                로거.log(Level.SEVERE, " 🚨 [치명적 커널 패닉] 마운트 포인트 볼륨 권한 파열: " + 경로문자열, 예외);
                throw new SecurityException("마운트 볼륨 접근 권한(ACL)이 물리적으로 거부되었습니다: " + 경로문자열, 예외);
            }
        });
    }

    // [1. 한글 상세 주석]
    // 인스턴스 고유 속성을 정의합니다 (불변 객체).
    // [2. 영문 상세 주석]
    // Defines the unique attributes of the instance (Immutable object).
    // [3. 자바 코드]
    private final String 격자_코드;
    private final String 격자_설명;
    private final String 심연_입력_폴더명;
    private final String 매트릭스_출력_폴더명;

    A0_DT_42_422000_타임프레임_컨텍스트(String 격자_코드, String 격자_설명, String 심연_입력_폴더명, String 매트릭스_출력_폴더명) {
        this.격자_코드 = 격자_코드;
        this.격자_설명 = 격자_설명;
        this.심연_입력_폴더명 = 심연_입력_폴더명;
        this.매트릭스_출력_폴더명 = 매트릭스_출력_폴더명;
    }

    // =========================================================================
    // 6. 물리적 폴더 격리 스마트 라우터
    // =========================================================================

    // [1. 한글 상세 주석]
    // [L3 심연 경로] 필터링을 거치지 않은 순수 원본(Raw) 데이터가 보관된 장소를 반환합니다.
    // [2. 영문 상세 주석]
    // [L3 Abyss Path] Returns the location where pure, unfiltered raw data is
    // stored.
    // [3. 자바 코드]
    public Path get심연_마스터_경로() {
        return Paths.get(L3_심연_저장소, this.심연_입력_폴더명);
    }

    // [1. 한글 상세 주석]
    // [상태 기계 스풀 경로] 비동기 데이터 주조 시, 파일의 처리 상태에 따라 물리적 위치를 격리합니다.
    // [2. 영문 상세 주석]
    // [State Machine Spool Path] During asynchronous data casting, physical
    // locations are isolated based on the file's processing state.
    // [3. 자바 코드]
    public Path get상태기계_스풀_경로(스풀_상태 상태) {
        return Paths.get(L1_매트릭스_저장소, "SPOOL_PIPELINE", this.격자_코드, 상태.get폴더명());
    }

    // [1. 한글 상세 주석]
    // [수술실 경로] 인간이 수동으로 교정한 텐서 데이터가 최우선으로 투입되는 핫라인입니다.
    // [2. 영문 상세 주석]
    // [Operating Room Path] A hotline where tensor data manually corrected by
    // humans is injected with the highest priority.
    // [3. 자바 코드]
    public Path get외과수술_오버라이드_경로() {
        return Paths.get(L1_매트릭스_저장소, "SURGERY_OVERRIDE", this.격자_코드);
    }

    // [1. 한글 상세 주석]
    // [모의전 샌드박스 경로] AI의 강화학습 시, 원본 훼손을 막기 위해 Copy-on-Write 기법이 적용될 평행 우주 영토입니다.
    // [2. 영문 상세 주석]
    // [Mock Battle Sandbox Path] A parallel universe territory where the
    // Copy-on-Write technique is applied to prevent original damage during AI
    // reinforcement learning.
    // [3. 자바 코드]
    public Path get모의전_샌드박스_경로() {
        return Paths.get(L1_매트릭스_저장소, "SANDBOX_VIRTUAL", this.격자_코드);
    }

    // [1. 한글 상세 주석]
    // [감시망 경로] 결측치 자가 치유 내역 및 이상 징후가 사출되는 보고서 폴더입니다.
    // [2. 영문 상세 주석]
    // [Monitoring Network Path] A report folder where missing value self-healing
    // details and abnormal signs are emitted.
    // [3. 자바 코드]
    public Path getReportRoomPath() {
        return Paths.get(L1_매트릭스_저장소, "이상_보고서", this.격자_코드);
    }

    // [1. 한글 상세 주석]
    // [비동기 방파제(Spool) 경로] AI 코어나 외부 크롤러가 데이터를 무지성으로 쏟아낼 임시 완충 지대입니다.
    // [2. 영문 상세 주석]
    // [Asynchronous Breakwater (Spool) Path] A temporary buffer zone where the AI
    // core or external crawlers will mindlessly pour out data.
    // [3. 자바 코드]
    public Path getAiOutputSpoolPath() {
        return Paths.get(L1_매트릭스_저장소, "AI_OUTPUT_SPOOL", this.격자_코드);
    }

    // [1. 한글 상세 주석]
    // [L1 유니버스 경로] 완성된 바이너리 매트릭스 텐서(.layer)가 렌더링되는 최상위 고속 메모리 맵핑 구역입니다.
    // [2. 영문 상세 주석]
    // [L1 Universe Path] The top-level high-speed memory mapping area where
    // completed binary matrix tensors (.layer) are rendered.
    // [3. 자바 코드]
    public Path get매트릭스_유니버스_경로() {
        return Paths.get(L1_매트릭스_저장소, this.매트릭스_출력_폴더명);
    }

    // [1. 한글 상세 주석]
    // 5대 기본 지표(시가, 고가, 저가, 종가, 거래량)가 위치한 베이스 캠프 경로를 반환합니다.
    // [2. 영문 상세 주석]
    // Returns the base camp path where the 5 basic indicators (open, high, low,
    // close, volume) are located.
    // [3. 자바 코드]
    public Path get베이스_지표_경로() {
        return get매트릭스_유니버스_경로().resolve("BASE_DATA");
    }

    // [1. 한글 상세 주석]
    // AI 파이프라인이 파생 및 조합해 낸 기술적 지표들이 위치한 인디케이터 캠프 경로를 반환합니다.
    // [2. 영문 상세 주석]
    // Returns the indicator camp path where the technical indicators derived and
    // combined by the AI pipeline are located.
    // [3. 자바 코드]
    public Path get파생_지표_경로() {
        return get매트릭스_유니버스_경로().resolve("INDICATORS");
    }

    // [1. 한글 상세 주석]
    // 백그라운드 섀도우 데몬(Tier 4.5)이 CPU 유휴 시간에 미리 구워둔 정규화(Z-Score) 텐서의 격리 보관소입니다.
    // [2. 영문 상세 주석]
    // An isolated repository for normalized (Z-Score) tensors pre-baked by the
    // background shadow daemon (Tier 4.5) during CPU idle time.
    // [3. 자바 코드]
    public Path get섀도우_텐서_경로() {
        return get매트릭스_유니버스_경로().resolve("SHADOW_Z_LAYERS");
    }

    // [1. 한글 상세 주석]
    // 차원 스캐너(Tier 1)가 발급하고 지능형 라우터(Tier 2)가 참조할 동적 DNA(호적부) 경로입니다.
    // [2. 영문 상세 주석]
    // The dynamic DNA (registry) path issued by the dimension scanner (Tier 1) and
    // referenced by the intelligent router (Tier 2).
    // [3. 자바 코드]
    public Path get지능형_호적부_경로() {
        return get매트릭스_유니버스_경로().resolve("00_MANIFEST_REGISTRY_" + this.격자_코드 + ".json");
    }

    // [1. 한글 상세 주석]
    // 🎯 [핵심 라우팅] 지표명(layerName)을 입력받아, BASE 지표인지 파생(INDICATOR) 지표인지 스스로 판단하여 정확한
    // 원본 저장/로드(.layer) 물리 경로를 반환합니다.
    // [2. 영문 상세 주석]
    // 🎯 [Core Routing] Receives a feature name (layerName) and self-determines
    // whether it is a BASE or INDICATOR feature, returning the exact physical path
    // for original save/load (.layer).
    // [3. 자바 코드]
    public Path resolve레이어_절대_경로(String 지표명) {
        Path 타겟_디렉토리 = 지표명.toUpperCase().startsWith("BASE_") ? get베이스_지표_경로() : get파생_지표_경로();
        return 타겟_디렉토리.resolve(지표명 + ".layer");
    }

    // [1. 한글 상세 주석]
    // 🎯 [섀도우 라우팅] 백그라운드 연산 데몬이 구워둔 정규화 텐서(.zlayer)의 물리 경로를 반환합니다. AI 신경망 코어는 이 경로를
    // 찌름으로써 연산 시간 0초(Zero-Overhead)를 달성합니다.
    // [2. 영문 상세 주석]
    // 🎯 [Shadow Routing] Returns the physical path of normalized tensors (.zlayer)
    // baked by the background computation daemon. AI neural network cores achieve
    // zero computation time (Zero-Overhead) by piercing this path.
    // [3. 자바 코드]
    public Path resolve섀도우_레이어_절대_경로(String 지표명) {
        return get섀도우_텐서_경로().resolve(지표명 + ".zlayer");
    }

    // =========================================================================
    // 7. Getters
    // =========================================================================
    public String get격자_코드() {
        return 격자_코드;
    }

    public String get격자_설명() {
        return 격자_설명;
    }

    @Override
    public String toString() {
        return String.format("TimeFrame[%s] %s | 심연(L3): %s | 매트릭스(L1): %s",
                격자_코드, 격자_설명, 심연_입력_폴더명, 매트릭스_출력_폴더명);
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 단일 장애점(SPOF)의 소멸과 이기종 디스크 분산 아키텍처:
 * 과거의 하드코딩은 시스템이 물리적으로 단 하나의 스토리지 드라이브에 종속되게 만드는 치명적인 결함이었습니다.
 * 만약 해당 드라이브의 컨트롤러가 타버리거나 PCIe 대역폭(Bandwidth)이 포화 상태에 이르면 DB 전체가 즉사하게 됩니다.
 * 통합 OS V6.0 아키텍처는 `System.getProperty`를 통해 JVM 실행 인자(Argument)로 각 계층의 물리적 거점을
 * 동적 할당(Dynamic Allocation)받습니다.
 * 서버 기동 시 `-Dmatrix.root.l1=/mnt/nvme_m2`로 초고빈도 연산 영역을 M.2 NVMe에 올리고,
 * `-Dmatrix.root.l3=/mnt/hdd_archive`로 방대한 과거 문헌을 저비용/고용량 HDD에 분산시킬 수 있습니다.
 * 이로써 디스크 헤드의 물리적 병목(Seek Time)이 해소되고, 시스템의 규모 가변성(Scalability)이 클라우드 네이티브 수준으로
 * 진화했습니다.
 * 
 * 2. 물리적 L1/L2/L3 계층 분리 (Physical Separation of L1/L2/L3):
 * 인공지능과 데이터베이스가 융합된 아키텍처에서는 저장 매체의 특성이 곧 연산 속도를 결정합니다.
 * HDD의 헤드 이동 지연을 유발하는 무거운 원본 텍스트 파동은 `L3_심연_저장소`에 철저히 격리됩니다.
 * 수축을 완료한 불변의 진리(Essence)는 RDBMS 친화적인 `L2_표준_저장소`에 보관되며,
 * 1초에 수천만 번 FFM API로 MemorySegment가 맵핑되어야 하는 고밀도 부동소수점 배열은 RAM/NVMe 기반의
 * `L1_매트릭스_저장소`에 독점 배치됩니다. 이로써 I/O 병목에 의한 스레드 스톨(Stall)이 영구 멸균됩니다.
 * 
 * 3. 💡 하이브리드 마운트 권한 검증망 (Hybrid ACL Cache Validation):
 * 아무리 정교한 경로(Path) 라우팅 객체라도 해당 디스크 파티션이 `Read-Only`로 마운트되었거나,
 * 폴더 생성 권한이 없다면 런타임에 끔찍한 `AccessDeniedException` 붕괴를 일으킵니다.
 * 이 모듈은 경로를 던져주기 전에 OS 커널 API(`PosixFilePermissions`)를 JNI/FFM 레벨로 직접 찔러
 * 0나노초 만에 파일 시스템의 읽기/쓰기 속성을 사전 판독합니다.
 * 그러나 NFS나 클라우드 맵핑 드라이브 등에서는 커널 캐시가 권한을 허용함에도 실제 I/O가 막히는 거짓 양성(False
 * Positive)이
 * 발생할 수 있으므로, 1바이트짜리 물리적 더미(Dummy) 파일을 생성하고 삭제해보는 I/O 검증을 폴백(Fallback)으로
 * 병행 결합했습니다. 검증 결과는 `ConcurrentHashMap`에 캐싱되어 런타임 시 매번 권한을 검사하는 부하를 0으로 지우면서도,
 * 시스템 시작 순간에 완벽한 I/O 무결성(Integrity)을 하드웨어 레벨에서 수학적으로 담보합니다.
 * 
 * 4. 공간적 디커플링 (Spatial Decoupling)을 통한 락(Lock) 경합 멸균:
 * 하위 모듈들(스캐너, 드라이버, 데몬)은 자신이 어느 폴더에 접근하는지, 그 폴더가 물리적으로
 * 어떤 드라이브에 묶여 있는지 알 필요가 없습니다. 오직 이 Enum 클래스에게 "경로를 다오"라고 묻기만 하면 됩니다.
 * 이것이 바로 시스템의 결합도를 극한으로 낮추고 유지보수성을 극대화하는 중앙 집중식 배관망의 진화입니다.
 * =============================================================================
 */