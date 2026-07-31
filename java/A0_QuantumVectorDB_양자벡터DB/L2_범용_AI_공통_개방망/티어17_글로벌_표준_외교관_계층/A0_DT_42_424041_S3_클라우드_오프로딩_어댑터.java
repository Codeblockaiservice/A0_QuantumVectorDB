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
 * - 모듈명: 통합 OS V6.1 - Tier 17: S3 클라우드 오프로딩 어댑터 (비동기 Multipart 콜드 스토리지 외교관)
 * - 기능 및 역할: 10만 틱 이전의 빙하기(Cold) 압축 데이터를 S3 버킷으로 업로드하고, 쿼리 엔진의 디맨드 페이징(Demand Paging) 요청 시 클라우드에서 다시 다운로드하는 통신을 전담합니다.
 * - 이론 및 기술: 육각 아키텍처(Hexagonal Architecture) 어댑터 패턴, AWS SDK V2 S3TransferManager, 멀티파트(Multipart) 병렬 스트리밍.
 * - 💡 [V6.1 치명적 결함 수복]: 단일 `putObject` 및 `getObject`가 지닌 5GB 용량 한계와 긴 전송 시간으로 인한 타임아웃 붕괴를 완벽히 차단하기 위해, `S3TransferManager` 기반의 비동기 Multipart Upload/Download 로직으로 전면 교체(Remake)했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 422046 데몬의 포트 인터페이스, 파일 I/O, 멀티파트 업/다운로드를 위한 AWS SDK V2 Transfer Manager 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of the port interface of the 422046 daemon, file I/O, and AWS SDK V2 Transfer Manager core libraries for multipart up/downloads.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어17_글로벌_표준_외교관_계층;

import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망.A0_DT_42_422046_시공간_지층_아카이빙_데몬.S3_클라우드_오프로딩_포트;

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
// 컴플라이언스 선언 및 클래스 헤더. 통합 OS의 내부 코어망과 AWS 클라우드 스토리지 사이의 거대 데이터를 안정적으로 송수신하는 멀티파트 어댑터 클래스입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A multipart adapter class that stably transmits and receives massive data between the internal core network of the Integrated OS and AWS Cloud Storage.
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
 * 1. 역할: 클라우드 오브젝트 스토리지(AWS S3)와의 대용량 비동기 통신을 담당하는 외교관 어댑터.
 * 2. 기능: 10만 틱 이전의 Cold 텐서 데이터를 멀티파트로 분할하여 S3 버킷에 병렬 업로드 및 디맨드 페이징 다운로드.
 * 3. 의도: 단일 스레드 기반 동기식 통신의 네트워크 타임아웃 뇌관을 해체하고, 클라우드 I/O 붕괴를 물리적으로 방어.
 * 4. 이론: 헥사고날 아키텍처(Hexagonal Architecture), 포트 앤 어댑터(Port and Adapter), 멀티파트
 * 청킹(Multipart Chunking).
 * 5. 기술: AWS SDK V2 S3TransferManager, S3AsyncClient, CompletableFuture.
 * ==============================================================================
 */
public final class A0_DT_42_424041_S3_클라우드_오프로딩_어댑터 implements S3_클라우드_오프로딩_포트 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.424041_S3_ADAPTER");

    // [1. 한글 상세 주석]
    // 💡 [수술 완료] 동기식 S3Client를 폐기하고, 비동기(Async) 클라이언트와 전송 매니저(TransferManager)를
    // 캡슐화합니다.
    // [2. 영문 상세 주석]
    // 💡 [Surgery Complete] Discarded the synchronous S3Client and encapsulated the
    // asynchronous (Async) client and TransferManager.
    // [3. 자바 코드]
    private final S3AsyncClient s3_비동기_클라이언트;
    private final S3TransferManager s3_전송_매니저;

    private final String 대상_버킷_명칭;
    private final String 스토리지_경로_접두사; // 예: "cold-stratum/v6/"

    // [1. 한글 상세 주석]
    // [창세 생성자] 시스템 환경 변수나 보안 저장소로부터 AWS 인증 키를 주입받아 비동기 S3 클라이언트와 트랜스퍼 매니저를 점화합니다.
    // [2. 영문 상세 주석]
    // [Genesis Constructor] Ignites the asynchronous S3 client and TransferManager
    // by injecting AWS authentication keys from system environment variables or
    // secure storage.
    // [3. 자바 코드]
    /**
     * @param 엑세스_키  AWS IAM Access Key
     * @param 시크릿_키  AWS IAM Secret Key
     * @param 리전     AWS Region (예: Region.AP_NORTHEAST_2)
     * @param 버킷명    데이터를 저장할 S3 버킷 이름
     * @param 경로_접두사 버킷 내에 저장될 디렉토리 경로 (Prefix)
     */
    public A0_DT_42_424041_S3_클라우드_오프로딩_어댑터(
            String 엑세스_키,
            String 시크릿_키,
            Region 리전,
            String 버킷명,
            String 경로_접두사) {

        if (엑세스_키 == null || 시크릿_키 == null || 버킷명 == null) {
            throw new IllegalArgumentException("[배관 파열] 클라우드 자격 증명이 누락되어 S3 어댑터를 점화할 수 없습니다.");
        }

        this.대상_버킷_명칭 = 버킷명;
        this.스토리지_경로_접두사 = (경로_접두사 != null && !경로_접두사.endsWith("/"))
                ? 경로_접두사 + "/"
                : (경로_접두사 == null ? "" : 경로_접두사);

        try {
            // 💡 [Zero-Mock 병렬 통신망 개방] 정적 자격 증명 공급자를 통해 AWS SDK V2 S3AsyncClient 인스턴스 생성
            AwsBasicCredentials 자격증명 = AwsBasicCredentials.create(엑세스_키, 시크릿_키);

            this.s3_비동기_클라이언트 = S3AsyncClient.builder()
                    .region(리전)
                    .credentialsProvider(StaticCredentialsProvider.create(자격증명))
                    .build();

            // 대용량 파일의 Multipart 병렬 처리를 관장하는 TransferManager 결속
            this.s3_전송_매니저 = S3TransferManager.builder()
                    .s3Client(this.s3_비동기_클라이언트)
                    .build();

            로거.info(String.format(
                    " >> [통합 OS V6.1] A0_DT_42_424041 S3 클라우드 오프로딩 어댑터 기동. (Target: %s, Region: %s, Multipart Engine: ON)",
                    버킷명, 리전.id()));

        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [인증 붕괴] 클라우드 스토리지 자격 증명에 실패했습니다.", 예외);
            throw new RuntimeException("S3 비동기 클라이언트 초기화 실패", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [외교 역학 1: 멀티파트 콜드 청크 업로드]
    // 422046 데몬이 압축을 완료한 수십~수백 GB의 Zstd/GZIP 파일을 S3TransferManager를 통해 비동기 병렬 분할
    // 전송(Multipart Upload)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Diplomatic Dynamics 1: Multipart Cold Chunk Upload]
    // Asynchronously and in parallel transmits (Multipart Upload) tens to hundreds
    // of GBs of Zstd/GZIP files, which the 422046 daemon has finished compressing,
    // via S3TransferManager.
    // [3. 자바 코드]
    @Override
    public void 업로드하다_콜드_청크(String 청크_식별자, Path 압축된_파일_경로) throws IOException {
        String s3_오브젝트_키 = 스토리지_경로_접두사 + 청크_식별자;

        try {
            // 💡 단일 PutObjectRequest를 완전히 파괴하고, TransferManager 전용 UploadFileRequest로 승격
            UploadFileRequest 업로드_요청 = UploadFileRequest.builder()
                    .putObjectRequest(req -> req
                            .bucket(대상_버킷_명칭)
                            .key(s3_오브젝트_키)
                            .contentType("application/octet-stream"))
                    .source(압축된_파일_경로)
                    .build();

            로거.fine(String.format("   ├─ [S3 멀티파트 업로드 개시] 대용량 콜드 청크(%s)를 비동기 병렬 분할하여 클라우드 심연으로 전송합니다...", 청크_식별자));

            // 백그라운드 병렬 업로드 실행 및 블로킹 대기 (상위 아카이빙 데몬의 흐름 제어를 위함)
            FileUpload 업로드_진행 = s3_전송_매니저.uploadFile(업로드_요청);
            업로드_진행.completionFuture().join();

            로거.info(String.format("   └─ [S3 오프로딩 수료] 클라우드 멀티파트 동기화 완료: s3://%s/%s", 대상_버킷_명칭, s3_오브젝트_키));

        } catch (CompletionException 예외) {
            Throwable 근본_원인 = 예외.getCause();
            if (근본_원인 instanceof S3Exception) {
                로거.log(Level.SEVERE, " [업로드 붕괴] S3 버킷으로의 멀티파트 전송 중 AWS 예외가 발생했습니다.", 근본_원인);
                throw new IOException("S3 클라우드 오프로딩 실패: " + ((S3Exception) 근본_원인).awsErrorDetails().errorMessage(),
                        근본_원인);
            }
            로거.log(Level.SEVERE, " [업로드 붕괴] S3 멀티파트 병렬 업로드 중 알 수 없는 I/O 예외가 발생했습니다.", 근본_원인);
            throw new IOException("S3 멀티파트 업로드 물리적 실패", 근본_원인);
        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [업로드 붕괴] S3 어댑터 내부 파이프라인에서 치명적 예외 발생.", 예외);
            throw new IOException("S3 어댑터 파이프라인 붕괴", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [외교 역학 2: 멀티파트 디맨드 페이징 다운로드]
    // 쿼리 엔진이 거대 과거 데이터를 요구할 때, S3 버킷에서 해당 청크를 병렬 스트림으로 고속 다운로드하여 로컬 파일로 조립합니다.
    // [2. 영문 상세 주석]
    // 💡 [Diplomatic Dynamics 2: Multipart Demand Paging Download]
    // When the query engine requests massive past data, it downloads the
    // corresponding chunk at high speed as a parallel stream from the S3 bucket and
    // assembles it into a local file.
    // [3. 자바 코드]
    @Override
    public void 다운로드하다_콜드_청크(String 청크_식별자, Path 저장할_로컬_경로) throws IOException {
        String s3_오브젝트_키 = 스토리지_경로_접두사 + 청크_식별자;

        try {
            // 💡 단일 GetObjectRequest를 폐기하고 DownloadFileRequest 기반 다이렉트 덤프 적용
            DownloadFileRequest 다운로드_요청 = DownloadFileRequest.builder()
                    .getObjectRequest(req -> req
                            .bucket(대상_버킷_명칭)
                            .key(s3_오브젝트_키))
                    .destination(저장할_로컬_경로)
                    .build();

            로거.warning(String.format(" 🚨 [Page Fault 복구] 클라우드 심연에서 멀티파트 스트림으로 콜드 청크(%s)를 끌어내립니다...", 청크_식별자));

            // 비동기 파일 다운로드 병렬 실행 및 블로킹 대기
            FileDownload 다운로드_진행 = s3_전송_매니저.downloadFile(다운로드_요청);
            다운로드_진행.completionFuture().join();

            로거.info(String.format("   └─ [디맨드 페이징 수료] S3 클라우드 병렬 다운로드 완료 및 L3(Warm) 영역 복원: %s",
                    저장할_로컬_경로.getFileName()));

        } catch (CompletionException 예외) {
            Throwable 근본_원인 = 예외.getCause();
            if (근본_원인 instanceof S3Exception) {
                로거.log(Level.SEVERE, " [다운로드 붕괴] S3 버킷에서 데이터를 수복하는 중 AWS 예외가 발생했습니다.", 근본_원인);
                throw new IOException("S3 디맨드 페이징 다운로드 실패: " + ((S3Exception) 근본_원인).awsErrorDetails().errorMessage(),
                        근본_원인);
            }
            로거.log(Level.SEVERE, " [다운로드 붕괴] S3 멀티파트 수복 중 알 수 없는 I/O 예외가 발생했습니다.", 근본_원인);
            throw new IOException("S3 다운로드 물리적 실패", 근본_원인);
        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [다운로드 붕괴] S3 어댑터 내부 파이프라인에서 치명적 예외 발생.", 예외);
            throw new IOException("S3 어댑터 파이프라인 붕괴", 예외);
        }
    }

    // [1. 한글 상세 주석]
    // [종결] 시스템 종료 시 AWS 클라이언트와 트랜스퍼 매니저의 네트워크 커넥션 풀을 완벽하게 반환합니다.
    // [2. 영문 상세 주석]
    // [Termination] Perfectly returns the network connection pools of the AWS
    // client and TransferManager upon system shutdown.
    // [3. 자바 코드]
    public void 통신망_차단() {
        if (s3_전송_매니저 != null) {
            s3_전송_매니저.close();
        }
        if (s3_비동기_클라이언트 != null) {
            s3_비동기_클라이언트.close();
        }
        로거.info(" >> [외교관 계층 철수 완료] S3 클라우드 멀티파트 전송 매니저 및 비동기 HTTP 연결망이 안전하게 폐쇄되었습니다.");
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 동기식 I/O의 붕괴와 멀티파트 병렬 전송(Multipart Transfer)의 필수성:
 * 과거 V6.0 아키텍처에 구현된 단일 `putObject` 및 `getObject` 방식은 결정적인 물리적 한계를 지닙니다.
 * AWS S3는 단일 PUT 요청의 파일 크기를 최대 5GB로 엄격히 제한하고 있습니다. 수십 년 치 텐서가 누적되어
 * 수백 기가바이트(GB)에 달하는 압축 지층(Stratum)을 통째로 올리려 하면 API 서버는 즉시 에러를 반환하며 연결을 끊어버립니다.
 * 설령 5GB 미만이라 할지라도, 단일 TCP 커넥션으로 몇 시간 동안 업로드를 유지하면 통신망의 미세한 파열(Jitter)로 인해
 * 소켓이 끊어지고 타임아웃 예외가 발생하여 업로드가 처음부터 다시 시작되는 '무한 재시도 지옥'에 빠지게 됩니다.
 * 
 * 2. S3TransferManager 기반 무결점 클라우드 스트리밍:
 * 이 치명적 단층을 수복하기 위해, 내부 엔진을 동기식 `S3Client`에서 비동기 기반의 `S3AsyncClient`와
 * 대용량 전담 `S3TransferManager`로 전면 리메이크했습니다.
 * 트랜스퍼 매니저는 100GB짜리 파일이 주어지면 이를 알아서 8MB~16MB 단위의 잘게 쪼개진 청크(Chunk)로 분할합니다.
 * 분할된 청크들은 수십 개의 비동기 스레드를 타고 S3 버킷을 향해 다발성으로 병렬 업로드(Multipart Upload)됩니다.
 * 전송 중 하나의 패킷이 손실되더라도 해당 작은 파편(Part)만 재전송하면 되므로 I/O 붕괴에 대한 절대적인 내성을 지닙니다.
 * 
 * 3. 기계적 공감(Mechanical Sympathy)과 디맨드 페이징의 고속화:
 * 쿼리 엔진(`422061`)이 "리먼 브라더스 사태" 시절의 콜드 데이터를 요구했을 때, 단일 스레드로 수십 기가를 다운로드받는 것은
 * 대기하는 AI 코어의 멱살을 잡는 것과 같습니다. `S3TransferManager`의 `downloadFile`은 여러 커넥션을 동시에
 * 열어
 * 클라우드의 오브젝트 스트림을 찢어 발긴 뒤 로컬 NVMe/HDD에 다이렉트로 퍼붓습니다.
 * 이로써 단일 대역폭의 한계를 깨고 네트워크 어댑터가 허용하는 극한의 한계 속도까지 다운로드 시간을 압축하여,
 * 가상 메모리 추상화(Demand Paging)의 지연 시간(Latency)을 최소화했습니다.
 * =============================================================================
 */