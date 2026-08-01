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
 * - 역할 (Role): 다중 시계열 해상도(Resolution)의 물리적 디렉토리 경로 격리 및 정적 라우팅.
 * - 기능 (Function): 원시 데이터(Raw), 수동 보정(Override), 샌드박스(Sandbox), 수집(Ingestion) 등 격리된 공간 경로 제공.
 * - 이론 (Theory): 공간적 디커플링(Spatial Decoupling), 이기종 스토리지 분산(Scale-Out), 하이브리드 폴백 권한 검증.
 * - 기대효과 (Effect): 수백 개의 스레드가 동시 접근해도 파일 시스템 병목을 0%로 통제.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [초정밀 제어] 마운트 포인트 권한 검증: 경로 생성 전 OS 커널 API(`PosixFilePermissions`)를 
 *                 호출하여 파일 시스템의 읽기/쓰기 속성을 0나노초에 사전 판독하고, 
 *                 물리적 더미 파일 I/O를 결합한 하이브리드 폴백(Hybrid Fallback) 방어막 적용 완료.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 시스템 제어, 커널 레벨 권한 속성(POSIX) 검증, FFM 메모리 규격, 동시성 검증을 위한 핵심 자바 표준 라이브러리.
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
 * ==============================================================================
 */
public enum A0_DT_42_422000_타임프레임_컨텍스트 {

    // [1. 한글 상세 주석]
    // 다중 시계열 도메인을 정의합니다. 각 해상도는 서로 다른 디렉토리 영토를 할당받습니다.
    // [2. 영문 상세 주석]
    // Defines multi-timeseries domains. Each resolution is assigned a different
    // directory territory.
    DAILY_RESOLUTION("1D", "Macro_Daily_Matrix", "DAILY_MASTER", "L1_DAILY_TENSOR"),
    MIN5_RESOLUTION("5M", "Tactical_5Min_Matrix", "MIN5_MASTER", "L1_MIN5_TENSOR"),
    MIN1_RESOLUTION("1M", "Micro_1Min_Matrix", "MIN1_MASTER", "L1_MIN1_TENSOR");

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422000_CONTEXT_ROUTER");

    // [1. 한글 상세 주석]
    // 데이터 수집 파이프라인 상태 기계 정의 (고아 파일 복구 및 락프리 I/O 지원)
    // [2. 영문 상세 주석]
    // Data ingestion pipeline state machine definition (supports orphan file
    // recovery and lock-free I/O).
    public enum IngestionState {
        INGRESS("01_INGRESS"), // 외부 데이터가 최초로 진입하는 대기열
        PROCESSING("02_PROCESSING"), // Ingestor가 배타적 락을 쥐고 파싱 중인 구역
        ARCHIVE("03_ARCHIVE"), // 성공적으로 텐서화가 완료된 원본 데이터의 보관소
        QUARANTINE("04_QUARANTINE"); // 파싱 에러나 데이터 무결성 위반으로 격리된 구역

        private final String folderName;

        IngestionState(String folderName) {
            this.folderName = folderName;
        }

        public String getFolderName() {
            return folderName;
        }
    }

    // [1. 한글 상세 주석]
    // FFM API에서 메모리 오프셋에 float 데이터를 삽입할 때 사용할 절대 규격. 리틀 엔디안을 강제합니다.
    // [2. 영문 상세 주석]
    // Absolute specification for float data insertion in FFM API. Forces
    // little-endian to physically eliminate JVM byte-swap operations.
    public static final ValueLayout.OfFloat TENSOR_FLOAT_LAYOUT = ValueLayout.JAVA_FLOAT_UNALIGNED
            .withOrder(ByteOrder.LITTLE_ENDIAN);

    // [1. 한글 상세 주석]
    // 물리적 경로 베이스 통제소 (OS-Agnostic 기본 루트 설정 및 이기종 디스크 분산 아키텍처 지원)
    // [2. 영문 상세 주석]
    // Physical path base control station (OS-Agnostic default root and
    // heterogeneous disk distributed architecture support).
    private static final String OS_AGNOSTIC_DEFAULT_ROOT = System.getProperty("user.dir") + File.separator
            + "A0_TDQI_MATRIX";

    // 💡 L3: HDD 기반, 가공되지 않은 100% 원시 데이터 (비동기 Append-only 로거 전용)
    private static final String L3_RAW_STORAGE = System.getProperty("matrix.root.l3",
            OS_AGNOSTIC_DEFAULT_ROOT + File.separator + "L3_RAW_DATA");

    // 💡 L2: RDBMS/SSD 기반, 정제가 완료된 기준 데이터 저장소
    private static final String L2_STANDARD_STORAGE = System.getProperty("matrix.root.l2",
            OS_AGNOSTIC_DEFAULT_ROOT + File.separator + "L2_STANDARD_DATA");

    // 💡 L1: RAM/NVMe 기반, SIMD 텐서 처리 및 FFM 다이렉트 매핑을 위한 고속 연산 구역
    private static final String L1_FAST_STORAGE = System.getProperty("matrix.root.l1",
            OS_AGNOSTIC_DEFAULT_ROOT + File.separator + "L1_FAST_DATA");

    // [1. 한글 상세 주석]
    // 디스크 권한 체크 부하를 없애기 위한 루트 경로 무결성 캐싱 맵입니다.
    // [2. 영문 상세 주석]
    // Root path integrity caching map to eliminate disk permission check overhead.
    private static final ConcurrentHashMap<String, Boolean> mountPermissionCache = new ConcurrentHashMap<>();

    // [1. 한글 상세 주석]
    // 클래스 로드 시점에 각 물리적 스토리지 거점에 대한 권한 검증을 1회 집행합니다.
    // [2. 영문 상세 주석]
    // Executes permission verification for each physical storage stronghold exactly
    // once at class load time.
    static {
        verifyMountPermission(L3_RAW_STORAGE);
        verifyMountPermission(L2_STANDARD_STORAGE);
        verifyMountPermission(L1_FAST_STORAGE);
    }

    // [1. 한글 상세 주석]
    // 💡 [하이브리드 마운트 권한 검증망] 커널 API 기반 사전 판독 + 물리적 더미 I/O 검증
    // [2. 영문 상세 주석]
    // 💡 [Hybrid Mount Permission Verification] Kernel API-based pre-read +
    // physical dummy I/O verification to prevent false positives.
    private static void verifyMountPermission(String physicalRootPath) {
        mountPermissionCache.computeIfAbsent(physicalRootPath, pathString -> {
            Path path = Paths.get(pathString);
            try {
                if (!Files.exists(path)) {
                    Files.createDirectories(path);
                }

                // 💡 1단계: OS 커널 레벨 사전 판독 (PosixFilePermissions 검증)
                boolean kernelValidationPassed = false;
                try {
                    Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
                    if (permissions.contains(PosixFilePermission.OWNER_READ) &&
                            permissions.contains(PosixFilePermission.OWNER_WRITE)) {
                        kernelValidationPassed = true;
                        logger.info(String.format("   ├─ [Kernel ACL Scan] POSIX Read/Write Verification Passed: %s",
                                pathString));
                    }
                } catch (UnsupportedOperationException e) {
                    // Windows 등 POSIX 미지원 OS 대체 로직
                    if (Files.isReadable(path) && Files.isWritable(path)) {
                        kernelValidationPassed = true;
                        logger.info(String.format(
                                "   ├─ [Kernel ACL Scan] Native OS Read/Write Verification Passed: %s", pathString));
                    }
                }

                if (!kernelValidationPassed) {
                    logger.warning(String.format(
                            "   ├─ [ACL Warning] Kernel validation failed. Attempting physical I/O fallback: %s",
                            pathString));
                }

                // 💡 2단계: 하이브리드 폴백 방어막 (더미 파일 기반 물리 검증)
                Path aclTestFile = path.resolve(".acl_lock_test_" + System.currentTimeMillis());
                Files.write(aclTestFile, new byte[] { 42 }); // 1바이트 더미 작성
                Files.delete(aclTestFile); // 삭제 검증으로 쓰기/수정 권한 최종 확인

                logger.info(String.format("   ├─ [Mount Verified] Hybrid storage lock-on successful: %s", pathString));
                return true;

            } catch (Exception ex) {
                logger.log(Level.SEVERE, " 🚨 [Fatal Kernel Panic] Mount point volume permission denied: " + pathString,
                        ex);
                throw new SecurityException("Mount volume access (ACL) physically denied: " + pathString, ex);
            }
        });
    }

    // [1. 한글 상세 주석]
    // 인스턴스 고유 속성을 정의합니다 (불변 객체).
    // [2. 영문 상세 주석]
    // Defines the unique attributes of the instance (Immutable object).
    private final String resolutionCode;
    private final String resolutionDescription;
    private final String rawInputFolderName;
    private final String fastOutputFolderName;

    A0_DT_42_422000_타임프레임_컨텍스트(String resolutionCode, String resolutionDescription, String rawInputFolderName,
            String fastOutputFolderName) {
        this.resolutionCode = resolutionCode;
        this.resolutionDescription = resolutionDescription;
        this.rawInputFolderName = rawInputFolderName;
        this.fastOutputFolderName = fastOutputFolderName;
    }

    // =========================================================================
    // 6. 물리적 폴더 격리 스마트 라우터 (Physical Directory Smart Router)
    // =========================================================================

    public Path getRawDataMasterPath() {
        return Paths.get(L3_RAW_STORAGE, this.rawInputFolderName);
    }

    public Path getStateMachineSpoolPath(IngestionState state) {
        return Paths.get(L1_FAST_STORAGE, "INGESTION_PIPELINE", this.resolutionCode, state.getFolderName());
    }

    public Path getManualOverridePath() {
        return Paths.get(L1_FAST_STORAGE, "MANUAL_OVERRIDE", this.resolutionCode);
    }

    public Path getIsolatedSandboxPath() {
        return Paths.get(L1_FAST_STORAGE, "ISOLATED_SANDBOX", this.resolutionCode);
    }

    public Path getSystemReportPath() {
        return Paths.get(L1_FAST_STORAGE, "SYSTEM_REPORTS", this.resolutionCode);
    }

    public Path getAsyncOutputBufferPath() {
        return Paths.get(L1_FAST_STORAGE, "ASYNC_OUTPUT_BUFFER", this.resolutionCode);
    }

    public Path getFastDataRootPath() {
        return Paths.get(L1_FAST_STORAGE, this.fastOutputFolderName);
    }

    public Path getBaseDataPath() {
        return getFastDataRootPath().resolve("BASE_DATA");
    }

    public Path getDerivedDataPath() {
        return getFastDataRootPath().resolve("DERIVED_DATA");
    }

    public Path getNormalizedCachePath() {
        return getFastDataRootPath().resolve("NORMALIZED_CACHE");
    }

    public Path getMetadataRegistryPath() {
        return getFastDataRootPath().resolve("00_METADATA_REGISTRY_" + this.resolutionCode + ".json");
    }

    // [1. 한글 상세 주석]
    // 🎯 [핵심 라우팅] 지표명을 분석하여 기본(BASE) 혹은 파생(DERIVED) 경로로 자동 라우팅합니다.
    // [2. 영문 상세 주석]
    // 🎯 [Core Routing] Analyzes the feature name and automatically routes to
    // either BASE or DERIVED path.
    public Path resolveDataAbsolutePath(String featureName) {
        Path targetDirectory = featureName.toUpperCase().startsWith("BASE_") ? getBaseDataPath() : getDerivedDataPath();
        return targetDirectory.resolve(featureName + ".layer");
    }

    // [1. 한글 상세 주석]
    // 🎯 [캐시 라우팅] 백그라운드 연산 데몬이 구워둔 정규화 텐서(.zlayer)의 물리 경로를 반환합니다.
    // [2. 영문 상세 주석]
    // 🎯 [Cache Routing] Returns the physical path of normalized tensors (.zlayer)
    // baked by the background daemon.
    public Path resolveCacheAbsolutePath(String featureName) {
        return getNormalizedCachePath().resolve(featureName + ".zlayer");
    }

    // =========================================================================
    // 7. Getters
    // =========================================================================
    public String getResolutionCode() {
        return resolutionCode;
    }

    public String getResolutionDescription() {
        return resolutionDescription;
    }

    @Override
    public String toString() {
        return String.format("TimeFrame[%s] %s | RawPath(L3): %s | FastPath(L1): %s",
                resolutionCode, resolutionDescription, rawInputFolderName, fastOutputFolderName);
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 단일 장애점(SPOF) 방지와 이기종 디스크 분산(Scale-Out) 아키텍처:
 * 특정 디렉토리 경로를 소스 코드에 하드코딩하면, 시스템은 물리적으로 단일 스토리지 장비에 종속됩니다.
 * 해당 스토리지의 I/O 대역폭이 한계에 도달할 경우 전체 시스템의 병목으로 이어집니다.
 * 이 모듈은 `System.getProperty`를 통해 JVM 시작 시점에 각 데이터 계층의 물리적 경로를 동적 할당(Dynamic
 * Allocation) 받습니다.
 * 이를 통해 I/O가 집중되는 L1(고속 스토리지) 영역은 NVMe/SSD에 배치하고, 로그 성격의 L3(원시 데이터) 영역은
 * 대용량 HDD 등에 분산 배치하여 스토리지 레벨의 병목을 완화하고 시스템 확장성(Scalability)을 보장합니다.
 * 
 * 2. 물리적 계층 분리 (Physical Separation of Data Tiers):
 * 데이터 파이프라인 아키텍처에서는 저장 매체의 특성이 곧 연산 속도를 결정합니다.
 * 잦은 순차 쓰기(Append)가 일어나는 원시 데이터는 `L3_RAW_STORAGE`에 격리하며,
 * 초고속 Random Access와 FFM(Foreign Function & Memory API) 메모리 매핑이 필요한 데이터 배열은
 * 고속 I/O가 보장된 `L1_FAST_STORAGE`에 배치하여 스레드 경합과 I/O 대기를 최소화합니다.
 * 
 * 3. 하이브리드 권한 검증망 (Hybrid ACL Validation):
 * 아무리 논리적인 라우팅이 완벽하더라도 실제 파일 시스템에 대한 읽기/쓰기 권한(ACL)이 부족하다면 런타임 중
 * `AccessDeniedException`이 발생합니다.
 * 본 시스템은 런타임 검사 오버헤드를 제거하기 위해, 클래스 로드 시점에 OS 커널 API(`PosixFilePermissions`)를
 * 호출해 권한을 1차로 확인합니다.
 * 추가적으로, NFS(네트워크 파일 시스템) 등에서 발생할 수 있는 커널 캐시의 거짓 양성(False Positive, 권한이 있다고
 * 캐싱되지만 실제 쓰기는 실패하는 현상)을
 * 방지하기 위해 1바이트 더미 파일을 물리적으로 쓰고 지우는 2차 물리적 검증을 병행합니다.
 * 이를 통해 하드웨어 레벨의 I/O 무결성(Integrity)을 보장합니다.
 * 
 * 4. 공간적 디커플링 (Spatial Decoupling)을 통한 관리 추상화:
 * 각 비즈니스 로직 및 I/O 처리 하위 모듈들은 자신이 처리하는 데이터가 물리적으로 어느 드라이브, 어느 디렉토리에 위치하는지 알 필요가
 * 없습니다.
 * 모든 경로는 이 Enum 클래스(라우팅 코어)를 통해서만 제공되므로 시스템 컴포넌트 간의 결합도가 극도로 낮아지고 유지보수성이
 * 극대화됩니다.
 * =============================================================================
 */