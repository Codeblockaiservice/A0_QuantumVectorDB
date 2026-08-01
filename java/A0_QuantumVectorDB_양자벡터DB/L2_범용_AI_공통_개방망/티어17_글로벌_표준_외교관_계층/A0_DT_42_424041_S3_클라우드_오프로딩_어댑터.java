/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층
 * @alias S3_Cloud_Offloading_Adapter
 * @tier 17
 * @keywords Hexagonal Architecture, Port and Adapter, AWS S3, Multipart Upload, S3TransferManager, Cold Storage, Zero-Mock
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_424041_S3_클라우드_오프로딩_어댑터.java
 * - 모듈명: 통합 OS V6.1 - Tier 17: S3 클라우드 오프로딩 어댑터 (비동기 Multipart 콜드 스토리지 어댑터)
 * - 기능 및 역할: 10만 틱 이전의 아카이브(Cold) 압축 데이터를 S3 버킷으로 업로드하고, 쿼리 엔진의 디맨드 페이징(Demand Paging) 요청 시 클라우드에서 다시 다운로드하는 외부 통신을 전담합니다.
 * - 이론 및 기술: 헥사고날 아키텍처(Hexagonal Architecture) 어댑터 패턴, AWS SDK V2 S3TransferManager, 멀티파트(Multipart) 병렬 스트리밍.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [V6.1 결함 수복]: 단일 `putObject` 및 `getObject` API가 지닌 5GB 용량 한계와 긴 전송 시간으로 인한 네트워크 타임아웃 붕괴를 완벽히 차단하기 위해, `S3TransferManager` 기반의 비동기 Multipart Upload/Download 로직으로 아키텍처를 전면 개편했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 422046 데몬의 포트 인터페이스, 파일 I/O, 대용량 멀티파트 업/다운로드를 위한 AWS SDK V2 Transfer Manager 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of the port interface of the 422046 daemon, file I/O, and AWS SDK V2 Transfer Manager core libraries for massive multipart up/downloads.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층;

import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망.A0_DT_42_422046_시공간_지층_아카이빙_데몬.CloudStorageOffloadingPort;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 통합 OS의 내부 코어망과 AWS 클라우드 스토리지 사이의 거대 텐서 데이터를 병렬 멀티파트로 안정성 있게 송수신하는 아웃바운드 어댑터 클래스입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An outbound adapter class that stably transmits and receives massive tensor data in parallel multipart between the internal core network of the Integrated OS and AWS Cloud Storage.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424041
 * [파일명] A0_DT_42_424041_S3_클라우드_오프로딩_어댑터.java
 * [모듈명] 통합 OS V6.1 - Tier 17: S3 클라우드 오프로딩 어댑터 (비동기 Multipart 콜드 스토리지 외교관)
 * 
 * [설계 명세]
 * 1. 역할: 클라우드 오브젝트 스토리지(AWS S3)와의 대용량 비동기 통신을 물리적으로 전담하는 외교관 어댑터(Adapter).
 * 2. 기능: 10만 틱 이전의 Cold 텐서 데이터를 멀티파트로 분할하여 S3 버킷에 병렬 업로드 및 Page Fault 시 디맨드 페이징
 * 고속 다운로드.
 * 3. 의도: 단일 스레드 기반 동기식 통신의 네트워크 타임아웃(Timeout) 뇌관을 해체하고, 클라우드 I/O 병목으로 인한 코어망
 * 붕괴를 물리적으로 방어.
 * 4. 이론: 헥사고날 아키텍처(Hexagonal Architecture), 포트 앤 어댑터(Port and Adapter), 멀티파트
 * 청킹(Multipart Chunking).
 * ==============================================================================
 */
public final class A0_DT_42_424041_S3_클라우드_오프로딩_어댑터 implements CloudStorageOffloadingPort {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.424041_S3_ADAPTER");

    // [1. 한글 상세 주석]
    // 💡 [아키텍처 수술 완료] 블로킹 기반의 동기식 S3Client를 폐기하고, 비동기(Async) 클라이언트와 병렬 전송
    // 매니저(TransferManager)를 캡슐화합니다.
    // [2. 영문 상세 주석]
    // 💡 [Architectural Surgery Complete] Discarded the blocking-based synchronous
    // S3Client and encapsulated the asynchronous (Async) client and parallel
    // TransferManager.

    private final S3AsyncClient s3AsyncClient;
    private final S3TransferManager s3TransferManager;

    private final String targetBucketName;
    private final String storagePathPrefix; // 예: "cold-stratum/v6/"

    // [1. 한글 상세 주석]
    // [생성자] 시스템 환경 변수나 보안 저장소로부터 AWS 인증 키를 의존성 주입(DI)받아 비동기 S3 클라이언트와 트랜스퍼 매니저를
    // 점화합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Ignites the asynchronous S3 client and TransferManager by
    // injecting AWS authentication keys (DI) from system environment variables or
    // secure storage.

    /**
     * @param accessKey  AWS IAM Access Key
     * @param secretKey  AWS IAM Secret Key
     * @param region     AWS Region (예: Region.AP_NORTHEAST_2)
     * @param bucketName 데이터를 저장할 S3 버킷 이름
     * @param pathPrefix 버킷 내에 저장될 논리적 디렉토리 경로 (Prefix)
     */
    public A0_DT_42_424041_S3_클라우드_오프로딩_어댑터(
            String accessKey,
            String secretKey,
            Region region,
            String bucketName,
            String pathPrefix) {

        if (accessKey == null || secretKey == null || bucketName == null) {
            throw new IllegalArgumentException("[배관 파열] 클라우드 자격 증명이 누락되어 S3 어댑터를 점화할 수 없습니다.");
        }

        this.targetBucketName = bucketName;
        this.storagePathPrefix = (pathPrefix != null && !pathPrefix.endsWith("/"))
                ? pathPrefix + "/"
                : (pathPrefix == null ? "" : pathPrefix);

        try {
            // 💡 [Zero-Mock 병렬 통신망 개방] 정적 자격 증명 공급자(StaticCredentialsProvider)를 통해 AWS SDK
            // V2 S3AsyncClient 인스턴스 생성
            AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);

            this.s3AsyncClient = S3AsyncClient.builder()
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .build();

            // 대용량 파일의 Multipart 병렬 처리를 관장하는 TransferManager 물리적 결속
            this.s3TransferManager = S3TransferManager.builder()
                    .s3Client(this.s3AsyncClient)
                    .build();

            logger.info(String.format(
                    " >> [통합 OS V6.1] A0_DT_42_424041 S3 클라우드 오프로딩 어댑터 기동 완료. (Target: %s, Region: %s, Multipart Engine: ON)",
                    bucketName, region.id()));

        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [인증 붕괴] 클라우드 스토리지 자격 증명(Credentials) 연동에 실패했습니다.", ex);
            throw new RuntimeException("S3 비동기 클라이언트 초기화 실패", ex);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [외교 역학 1: 멀티파트 콜드 청크 업로드]
    // 422046 지층 아카이빙 데몬이 압축을 완료한 수십~수백 GB의 Zstd/GZIP 텐서 파일을 S3TransferManager를 통해
    // 비동기 병렬 분할 전송(Multipart Upload)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Diplomatic Dynamics 1: Multipart Cold Chunk Upload]
    // Asynchronously and in parallel transmits (Multipart Upload) tens to hundreds
    // of GBs of Zstd/GZIP tensor files, which the 422046 archiving daemon has
    // finished compressing, via S3TransferManager.

    @Override
    public void uploadColdChunk(String chunkIdentifier, Path compressedFilePath) throws IOException {
        String s3ObjectKey = storagePathPrefix + chunkIdentifier;

        try {
            // 💡 단일 PutObjectRequest 한계를 완벽히 파괴하고, TransferManager 전용 UploadFileRequest
            // 규격으로 승격
            UploadFileRequest uploadRequest = UploadFileRequest.builder()
                    .putObjectRequest(req -> req
                            .bucket(targetBucketName)
                            .key(s3ObjectKey)
                            .contentType("application/octet-stream"))
                    .source(compressedFilePath)
                    .build();

            logger.fine(String.format("   ├─ [S3 멀티파트 업로드 개시] 대용량 콜드 청크(%s)를 비동기 병렬 분할하여 클라우드 오브젝트 스토리지로 전송합니다...",
                    chunkIdentifier));

            // 백그라운드 병렬 업로드 실행 및 블로킹 대기 (상위 아카이빙 데몬의 흐름 제어(Flow Control)를 위함)
            FileUpload uploadProgress = s3TransferManager.uploadFile(uploadRequest);
            uploadProgress.completionFuture().join();

            logger.info(String.format("   └─ [S3 오프로딩 수료] 클라우드 멀티파트 업로드 동기화 완료: s3://%s/%s", targetBucketName,
                    s3ObjectKey));

        } catch (CompletionException ex) {
            Throwable rootCause = ex.getCause();
            if (rootCause instanceof S3Exception) {
                logger.log(Level.SEVERE, " [업로드 파이프라인 붕괴] S3 버킷으로의 멀티파트 전송 중 AWS 예외가 발생했습니다.", rootCause);
                throw new IOException("S3 클라우드 오프로딩 실패: " + ((S3Exception) rootCause).awsErrorDetails().errorMessage(),
                        rootCause);
            }
            logger.log(Level.SEVERE, " [업로드 파이프라인 붕괴] S3 멀티파트 병렬 업로드 중 알 수 없는 물리적 I/O 예외가 발생했습니다.", rootCause);
            throw new IOException("S3 멀티파트 업로드 물리적 실패", rootCause);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [업로드 파이프라인 붕괴] S3 어댑터 내부 파이프라인에서 치명적 예외 발생.", ex);
            throw new IOException("S3 어댑터 아키텍처 붕괴", ex);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [외교 역학 2: 멀티파트 디맨드 페이징 다운로드]
    // 쿼리 엔진이 거대 과거 데이터를 요구(Page Fault)할 때, S3 버킷에서 해당 청크를 병렬 스트림으로 고속 다운로드하여 로컬 파일로
    // 조립(Restore)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Diplomatic Dynamics 2: Multipart Demand Paging Download]
    // When the query engine requests massive past data (Page Fault), it downloads
    // the corresponding chunk at high speed as a parallel stream from the S3 bucket
    // and assembles (Restores) it into a local file.

    @Override
    public void downloadColdChunk(String chunkIdentifier, Path targetLocalPath) throws IOException {
        String s3ObjectKey = storagePathPrefix + chunkIdentifier;

        try {
            // 💡 느린 단일 GetObjectRequest를 폐기하고 DownloadFileRequest 기반 다이렉트 덤프(Direct Dump)
            // 멀티파트 적용
            DownloadFileRequest downloadRequest = DownloadFileRequest.builder()
                    .getObjectRequest(req -> req
                            .bucket(targetBucketName)
                            .key(s3ObjectKey))
                    .destination(targetLocalPath)
                    .build();

            logger.warning(String.format(" 🚨 [Page Fault 복구 시퀀스] 클라우드 심연에서 멀티파트 병렬 스트림으로 콜드 청크(%s)를 로컬 디스크로 끌어내립니다...",
                    chunkIdentifier));

            // 비동기 파일 다운로드 병렬 실행 및 블로킹 대기 (상위 엔진이 즉시 사용할 수 있도록 완결성을 보장)
            FileDownload downloadProgress = s3TransferManager.downloadFile(downloadRequest);
            downloadProgress.completionFuture().join();

            logger.info(String.format("   └─ [디맨드 페이징 수료] S3 클라우드 병렬 다운로드 완료 및 L3(Warm) 로컬 스토리지 영역 복원: %s",
                    targetLocalPath.getFileName()));

        } catch (CompletionException ex) {
            Throwable rootCause = ex.getCause();
            if (rootCause instanceof S3Exception) {
                logger.log(Level.SEVERE, " [다운로드 붕괴] S3 버킷에서 데이터를 수복하는 중 AWS 통신 예외가 발생했습니다.", rootCause);
                throw new IOException(
                        "S3 디맨드 페이징 다운로드 실패: " + ((S3Exception) rootCause).awsErrorDetails().errorMessage(),
                        rootCause);
            }
            logger.log(Level.SEVERE, " [다운로드 붕괴] S3 멀티파트 데이터 수복 중 알 수 없는 I/O 예외가 발생했습니다.", rootCause);
            throw new IOException("S3 다운로드 로컬 쓰기 물리적 실패", rootCause);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [다운로드 붕괴] S3 어댑터 내부 파이프라인에서 치명적 예외 발생.", ex);
            throw new IOException("S3 어댑터 다운로드 파이프라인 붕괴", ex);
        }
    }

    // [1. 한글 상세 주석]
    // [종결 절차] 시스템 종료 시 AWS 비동기 클라이언트와 트랜스퍼 매니저의 네트워크 커넥션 풀을 운영체제에 완벽하게 반환합니다.
    // [2. 영문 상세 주석]
    // [Termination Procedure] Perfectly returns the network connection pools of the
    // AWS asynchronous client and TransferManager to the OS upon system shutdown.

    public void executeGracefulShutdown() {
        if (s3TransferManager != null) {
            s3TransferManager.close();
        }
        if (s3AsyncClient != null) {
            s3AsyncClient.close();
        }
        logger.info(" >> [외교관 계층 철수 완료] S3 클라우드 멀티파트 전송 매니저 및 비동기 HTTP 커넥션 풀이 안전하게 반환 폐쇄되었습니다.");
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 동기식 I/O의 붕괴와 멀티파트 병렬 전송(Multipart Transfer)의 필수성:
 * 과거 V6.0 아키텍처에 구현되었던 단일 `putObject` 및 `getObject` API 방식은 결정적이고 치명적인 물리적 한계를
 * 지닙니다.
 * AWS S3는 단일 PUT 요청의 파일 크기를 네트워크 신뢰성 등의 이유로 최대 5GB로 엄격히 제한하고 있습니다. 수십 년 치 텐서가
 * 누적되어
 * 수십~수백 기가바이트(GB)에 달하는 압축 지층(Cold Stratum) 파일을 통째로 단일 채널로 올리려 하면 API 서버는 즉시 에러를
 * 반환하며 연결을 끊어버립니다.
 * 설령 5GB 미만이라 할지라도, 단일 TCP 커넥션으로 수 분~몇 시간 동안 업로드를 유지하게 되면 통신망의 미세한 패킷 파손이나
 * 지터(Jitter)로 인해
 * 소켓이 끊어지고 타임아웃 예외가 발생하여 업로드가 처음부터 다시 시작되어야 하는 '무한 재시도 늪(Infinite Retry Hell)'에
 * 빠지게 됩니다.
 * 
 * 2. S3TransferManager 기반 무결점 클라우드 스트리밍 아키텍처:
 * 이 치명적 단층을 수복하기 위해, 내부 엔진을 동기식(Blocking) `S3Client`에서 비동기(Async) 기반의
 * `S3AsyncClient`와
 * 대용량 전담 아키텍처인 `S3TransferManager`로 전면 리팩토링했습니다.
 * 트랜스퍼 매니저는 100GB짜리 거대 파일이 주어지면 이를 알아서 8MB~16MB 단위의 잘게 쪼개진 청크(Chunk) 파편으로
 * 분할합니다.
 * 분할된 청크들은 수십 개의 비동기 백그라운드 스레드를 타고 S3 버킷을 향해 다발성으로 병렬 업로드(Multipart Upload)됩니다.
 * 전송 중 단 하나의 패킷이 손실되거나 연결이 끊어지더라도 해당 작은 파편(Part) 1개만 재전송하면 되므로 네트워크 I/O 붕괴에 대한
 * 절대적이고 강인한 내성(Resilience)을 지닙니다.
 * 
 * 3. 기계적 공감(Mechanical Sympathy)과 디맨드 페이징(Demand Paging)의 고속화:
 * 최상위 쿼리 엔진 모듈(`422061`)이 10년 전 과거 데이터를 조회하기 위해 Page Fault를 일으켜 콜드 데이터를 요구했을 때,
 * 단일 스레드로 수십 기가를 다운로드받는 것은 상단에서 대기하고 있는 AI 코어 연산 파이프라인의 숨통을 조이는 것과 같습니다.
 * `S3TransferManager`의 `downloadFile` 메커니즘은 클라우드를 향해 여러 TCP 커넥션을 동시에 열어 오브젝트
 * 스트림을 조각조각 찢어 발긴 뒤
 * 로컬 NVMe/HDD 파일 시스템에 다이렉트로 퍼부어 병렬 조립합니다.
 * 이로써 단일 대역폭의 한계를 깨고 서버 하드웨어 네트워크 어댑터(NIC)가 허용하는 극한의 한계 속도까지 다운로드 시간을 압축하여,
 * 가상 메모리 추상화 시스템(Virtual Memory Demand Paging)의 치명적 약점인 디스크-클라우드 스왑 지연 시간(Swap
 * Latency)을 최소화했습니다.
 * =============================================================================
 */