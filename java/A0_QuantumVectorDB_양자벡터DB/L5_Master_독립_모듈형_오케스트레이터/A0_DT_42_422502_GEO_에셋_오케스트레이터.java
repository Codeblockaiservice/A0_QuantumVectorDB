/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L5_Master_독립_모듈형_오케스트레이터
 * @alias GEO_Asset_Orchestrator
 * @tier 5
 * @keywords Thread Bulkheading, Lazy Initialization, Dependency Injection, Zero-Copy Pipeline, Atomic Lock, Graceful Teardown
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422502_GEO_에셋_오케스트레이터.java
 * - 기능: 클라이언트의 3D 시각화 호출 시에만 L4 기하학 엔진을 지연 기동(Lazy Initialization)하고, 제로-카피(Zero-Copy) 직결 메모리 파이프라인을 관제.
 * - 역할: 무거운 렌더링(Rendering) 작업이 메인 코어(AI/DB)의 CPU/메모리 자원을 강탈하지 못하도록 완벽히 격리하는 스레드 격벽 관제탑(Thread Bulkhead Tower).
 * - 이론 및 기술: 스레드 고립화 전략(Thread Bulkheading), 지연 기동 아키텍처(Lazy Initialization), 제어의 역전(IoC) 기반 자원 주입(Dependency Injection), AtomicBoolean 기반 스핀락(Spinlock) 방어, 이중 검사 잠금(DCL: Double-Checked Locking), 커널 메모리 회수를 위한 안전 강하 대기(Graceful Teardown).
 * - 기대효과: UI 렌더링 요청 폭주 시에도 코어 시스템의 TPS를 100% 방어하며, OOM(Out of Memory) 및 데이터 복사 병목이 전혀 없는 완벽한 그래픽 파일 직사(Direct Dump) 환경을 구축합니다.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [아키텍처 신설] 안전 강하 대기(Graceful Teardown) 배관 구축: `shutdownOrchestrator()` 실행 시, 물리 디스크 상에 진행 중인 기하학 베이킹 파일 직사(mmap I/O)가 
 *                 깨지지 않도록 `awaitTermination(5, TimeUnit.SECONDS)` 대기 로직을 주입하여, 커널의 강제 종료 절차를 안전하게 유예시킵니다.
 * - 💡 [아키텍처 유지] 기존의 완벽한 제로카피 파이프라인(Zero-Copy Pipeline) 및 지연 기동(Lazy Initialization) 아키텍처는 변경 없이 100% 원형 보존합니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 의존성 모듈 Import. 
// 💡 [배관 수복 및 멸균] 중간 버퍼(Intermediate Buffer) 역할을 하던 낡은 ByteBuffer 객체의 개입을 완벽히 소거하여 불필요한 NIO 메모리 임포트를 제거했으며, 시스템 안전 강하(Graceful Teardown) 통제를 위한 TimeUnit 패키지를 추가로 임포트했습니다.
// [2. 영문 상세 주석]
// Package declaration and import of dependency modules. 
// 💡 [Plumbing Restored and Sterilized] Eliminated the unnecessary NIO memory imports by completely removing the intervention of the old ByteBuffer object that acted as an intermediate buffer, and additionally imported the TimeUnit package for system graceful teardown control.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L5_Master_독립_모듈형_오케스트레이터;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;
import A0_QuantumVectorDB_양자벡터DB.L4_시각화_및_GEO_사영망.티어13_기하학_에셋_베이킹_엔진.A0_DT_42_422131_방사형_시공간_프로젝터;
import A0_QuantumVectorDB_양자벡터DB.L4_시각화_및_GEO_사영망.티어13_기하학_에셋_베이킹_엔진.A0_DT_42_422132_GEO_바이너리_베이킹_워커;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더.
// 무거운 그래픽 렌더링 연산과 백엔드 코어 DB 연산을 물리적으로 완벽히 격리(Isolation)하는 스레드 격벽 관제탑(Bulkhead Tower)입니다.
// 💡 [컴파일 에러 수복 완료] 하위 티어 13 엔진의 규격 변경(Direct Dump) 아키텍처 도입에 맞추어 상위 오케스트레이터의 파라미터 호출 시그니처를 수복하고 시스템 배관을 재연결했습니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header.
// A thread bulkhead control tower that physically and perfectly isolates heavy graphics rendering operations from backend core DB operations.
// 💡 [Compilation Error Fix Complete] Restored the upper orchestrator's parameter call signature and reconnected the system plumbing in accordance with the introduction of the new architecture (Direct Dump) specification changes in lower Tier 13 engines.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422502
 * [파일명] A0_DT_42_422502_GEO_에셋_오케스트레이터.java
 * [모듈명] 통합 OS V6.1 - L5 관제망: GEO 에셋 오케스트레이터 (렌더링 스레드 격벽 관제탑)
 * 
 * [설계 아키텍처 명세]
 * 1. 역할: 관리자(Front-end)의 3D 시각화(렌더링) 호출 시에만 무거운 L4 기하학 베이킹 엔진을 지연 기동(Lazy Initialization)하여 불필요한 시스템 상주 자원을 최소화합니다.
 * 2. 기능: 기저 DB 파이프라인에서 시계열 오프힙 텐서를 추출하여 백그라운드 단일 렌더링 스레드로 위임 및 철저히 통제합니다.
 * 3. 의도: 막대한 ALU 연산이 동반되는 무거운 3D 렌더링 작업이 메인 DB I/O나 AI 추론 스레드의 하드웨어 자원을 강탈(Starvation)하지 못하도록 최상위 계층에서 스레드 격벽(Thread Bulkhead)을 구축합니다.
 * 4. 💡 [V6.1 배관 파열 수복] 제로-카피(Zero-Copy) 의존성 주입(DI) 파이프라인 완성:
 *    과거 구세대 오케스트레이터가 하위 엔진으로부터 거대한 중간 힙 버퍼(`ByteBuffer`)를 받아와 자신이 직접 순회(Loop)하며 디스크 워커를 호출하던 최악의 비효율적 안티 패턴 로직을 전면 파기했습니다.
 *    수복된 아키텍처는 디스크 mmap 워커(422132)를 인스턴스화하여 하위 계층인 프로젝터(422131) 생성자에 직접 주입(Dependency Inject)해 버립니다.
 *    이로써 렌더링 엔진 루프 내부에서 좌표 연산이 끝나는 그 찰나에 힙 할당을 거치지 않고 곧바로 디스크 커널 캐시로 직사(Direct Dump)되는 완벽한 하드웨어 파이프라인이 개통되었습니다.
 * ==============================================================================
 */
public final class A0_DT_42_422502_GEO_에셋_오케스트레이터 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422502_GEO_ORCHESTRATOR");

    // [1. 한글 상세 주석]
    // 💡 [렌더링 충돌 방어망 (Concurrent Collision Defense)] 무거운 3D 렌더링 파일 베이킹 연산이 진행 중일 때, 
    // 프론트엔드로부터 중복된 새로운 렌더링 요청(연타 폭주 등)이 들어오면 서버 과부하를 막기 위해 O(1) 속도로 기각(Drop)해 버리는 원자적 상태 스위치(Atomic Switch)입니다.
    // [2. 영문 상세 주석]
    // 💡 [Concurrent Collision Defense] An atomic state switch to reject (Drop) at O(1) speed any new duplicated rendering requests (e.g., rapid button mashing from frontend) that come in while heavy 3D rendering file baking operations are currently in progress to prevent server overload.

    private final AtomicBoolean isRenderingActive = new AtomicBoolean(false);

    // [1. 한글 상세 주석]
    // 💡 [지연 기동(Lazy Initialization) 대상 시스템 자원] 통합 OS 시스템 콜드스타트(부팅) 시에는 힙 메모리나 스레드를 전혀 차지하지 않다가, 
    // 오로지 외부 클라이언트의 최초 시각화 API 호출 찰나에만 물리적으로 실체화(Materialize)되는 리소스들입니다. (Volatile 키워드를 통한 가시성 보장)
    // [2. 영문 상세 주석]
    // 💡 [Lazy Initialization Target System Resources] Resources that do not occupy any heap memory or threads during the Integrated OS system cold-start (boot), 
    // but are physically materialized only at the exact moment of the first visualization API call from an external client. (Visibility guaranteed via Volatile keyword)

    private volatile ExecutorService renderBulkheadThreadPool;
    private volatile A0_DT_42_422131_방사형_시공간_프로젝터 radialProjector;
    
    // 이중 검사 잠금(Double-Checked Locking, DCL) 패턴의 스레드 경합(Race Condition) 방어 및 동기화를 위한 전용 락 모니터 객체
    private final Object lazyInitLock = new Object();

    // [생성자]
    // 무거운 하위 엔진 자원들을 즉시 생성하지 않고 빈 껍데기만 가동하여 부팅 레이턴시와 메모리 점유를 극한으로 최소화합니다.
    public A0_DT_42_422502_GEO_에셋_오케스트레이터() {
        logger.info(" >> [통합 OS V6.1] A0_DT_42_422502 GEO 에셋 오케스트레이터 기동 완료. (L5 관제망 - 자원 지연 기동(Lazy Init) 스탠바이 및 스레드 격벽 설계 완수)");
    }

    // [1. 한글 상세 주석]
    // [제어 역학 1: 지연 기동 아키텍처 (Lazy Initialization)] 
    // 시각화 요청이 들어오는 그 찰나의 순간에만 스레드 풀과 하위 L4 프로젝터를 메모리에 올립니다. 정통적인 이중 검사 잠금(DCL) 패턴을 통해 멀티스레드 인스턴스화 충돌 환경을 100% 방어합니다.
    // [2. 영문 상세 주석]
    // [Control Dynamics 1: Lazy Initialization Architecture]
    // Loads the thread pool and the lower L4 projector into memory only at the exact moment a visualization request comes in. 100% defends against multithreaded instantiation collision environments via the traditional Double-Checked Locking (DCL) pattern.

    private void activateLazyInitializationEngine() {
        if (renderBulkheadThreadPool == null) {
            synchronized (lazyInitLock) {
                if (renderBulkheadThreadPool == null) {
                    // 💡 [스레드 격벽 (Thread Bulkheading)] 메인 코어의 자원 강탈(Starvation)을 막기 위해, 렌더링 전용 단일(Single) 스레드 격벽을 생성합니다.
                    renderBulkheadThreadPool = Executors.newSingleThreadExecutor(runnable -> {
                        Thread thread = new Thread(runnable);
                        thread.setName("OS_L5_GEO_RENDER_BULKHEAD");
                        // 운영체제 레벨에서 최하위 우선순위 배정: 그래픽 화면이 버벅거리더라도, 서버의 본질인 AI 추론 연산과 DB I/O 처리에는 절대적 우선순위를 양보합니다.
                        thread.setPriority(Thread.MIN_PRIORITY); 
                        return thread;
                    });
                    
                    radialProjector = new A0_DT_42_422131_방사형_시공간_프로젝터();
                    logger.info("   ├─ [L5 지연 기동(Lazy Init) 활성화 완료] 하위 L4 기하학 사영 엔진 및 렌더링 전용 백그라운드 스레드 격벽이 물리 메모리에 실체화되었습니다.");
                }
            }
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [핵심 배관 수복 구간] 오케스트레이터가 중간 버퍼를 순회하던 무겁고 비효율적인 루프를 전면 제거하고, 
    // 디스크 워커 인스턴스를 통째로 프로젝터에 주입(DI)하여 완벽한 Zero-Copy 파이프라인을 완성했습니다.
    // 워커는 `try-with-resources` 블록 스코프(Scope)로 안전하게 감싸져, 베이킹 연산 종료 즉시 커널 오프힙 메모리와 파일 채널을 OS 풀(Pool)로 안전하게 즉각 환원(Release)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Plumbing Fix Section] Completely eliminated the heavy and inefficient loop where the orchestrator iterated over an intermediate buffer, 
    // and completed the perfect Zero-Copy pipeline by entirely injecting (DI) the disk worker instance into the projector.
    // The worker is safely wrapped in a `try-with-resources` block scope, ensuring that kernel off-heap memory and file channels are safely and immediately released back to the OS pool the moment baking operations finish.

    /**
     * [제어 역학 2: 렌더링 작업 비동기 위임 오케스트레이션 및 Zero-Copy 직결 배관 연결]
     * 
     * @param timeSeriesReadPort          오프힙 메모리 공간에서 기하학 좌표계로 치환할 시계열 정규화 텐서 데이터를 읽어올 FFM 읽기 포트
     * @param participantDimensionCount   튜브 렌더링 단면을 구성할 텐서 차원(지표)의 총 개수
     * @param startTick                   사영 시뮬레이션을 시작할 시간(Tick) 인덱스
     * @param endTick                     사영을 종료할 시간(Tick) 인덱스
     * @param outputBinaryPath            OS mmap을 통해 메모리 복사 없이 직사(Direct Write)할 `.geo` 바이너리 파일의 최종 물리적 저장 경로
     * @return 렌더링 성공 여부를 프론트엔드 비동기 루프에 알리는 CompletableFuture 체인 객체
     */
    public CompletableFuture<Boolean> delegate3DAssetBaking(
            A0_DT_42_422001_권한_포트_인터페이스.ReadPort timeSeriesReadPort, 
            int participantDimensionCount, 
            int startTick, 
            int endTick, 
            Path outputBinaryPath) {

        // 💡 [원자적 스핀락 충돌 방어] 이미 L4 하위 베이킹 엔진이 파일 생성 풀가동 중이면, 새로운 요청은 파이프라인을 타지 못하고 O(1) 속도로 즉시 기각(Drop)
        if (!isRenderingActive.compareAndSet(false, true)) {
            logger.warning(" [L5 렌더링 요청 기각] 이전 3D 기하학 에셋 베이킹 작업이 시스템에서 아직 진행 중입니다. 메인 코어 메모리 보호 및 I/O 충돌 방지를 위해 현재 유입된 렌더링 요청을 과감히 폐기(Drop)합니다.");
            return CompletableFuture.completedFuture(false);
        }

        // 지연 기동 스레드 풀 및 프로젝터 엔진 실체화 (1회성)
        activateLazyInitializationEngine();

        logger.fine("   ├─ [L5 렌더링 비동기 위임] 3D 에셋 베이킹 오케스트레이션 명령을 하위 L4 스레드 격벽 파이프라인으로 이관합니다...");

        // 파사드(Main) 스레드를 블로킹(Blocking)하지 않고 비동기 단일 렌더링 격벽으로 작업을 던져(Offload) 분리시킴
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 직사할 1D 평면 바이너리의 오프셋(Offset) 할당 용량을 규정하기 위한 목표 정점의 총개수 선 산출
                long totalVertexCount = (long) (endTick - startTick + 1) * participantDimensionCount;
                
                // 💡 [제로-카피 관제 (Inversion of Control, IoC)]
                // 오케스트레이터(L5)가 OS 커널 맵핑(mmap) 워커를 직접 팩토리 패턴으로 생성한 뒤, 사영 계산을 전담하는 하위 프로젝터(L4) 계층의 매개변수로 밀어 넣어 주입(Inject)합니다.
                // try-with-resources 블록을 통해 파이프라인이 끝나는 찰나의 순간 파일 스트림과 메모리 아레나가 `close()` 메서드에 의해 즉시 OS로 안전하게 소멸, 환원됨을 언어 차원에서 절대 보장합니다.
                try (A0_DT_42_422132_GEO_바이너리_베이킹_워커 directDumpWorker = 
                        new A0_DT_42_422132_GEO_바이너리_베이킹_워커(outputBinaryPath, totalVertexCount)) {
                    
                    // 낡은 중간 힙 배열 버퍼(ByteBuffer)를 거치지 않고, 프로젝터가 직접 `directDumpWorker`의 포트 API를 때리도록 파이프라인 직결 개통
                    radialProjector.bakeBinaryTubeModel(timeSeriesReadPort, directDumpWorker, participantDimensionCount, startTick, endTick);
                }

                logger.info(" >> [L5 오케스트레이션 대성공] 3D 기하학 에셋 베이킹 연산이 완벽한 Zero-Copy 파이프라인을 관통하여 GPU 로드(Load) 이식 준비를 무결점으로 마쳤습니다.");
                return true;

            } catch (Exception ex) {
                logger.log(Level.SEVERE, " [L5 렌더링 붕괴 파열] 기하학 에셋 베이킹 사영 파이프라인 관통 중 치명적 예외 발생", ex);
                return false;
            } finally {
                // 💡 [락(Lock) 해제 복구] 연산이 성공하든, 치명적 런타임 예외가 발생하여 터지든 무관하게 어떤 경우에든 반드시 스위치 상태 락을 안전하게 해제(false)하여 시스템이 다음 렌더링 요청을 다시 받을 수 있도록 복구(Recover) 조치합니다.
                isRenderingActive.set(false);
            }
        }, renderBulkheadThreadPool);
    }

    // [1. 한글 상세 주석]
    // [종결 역학 단계] OS 셧다운 시 백그라운드 렌더링 스레드 풀을 안전하게 회수합니다.
    // 💡 [안전 강하(Graceful Teardown) 배관 보강] `awaitTermination` 로직을 신규 이식하여, 현재 물리적으로 쓰기(Write) 작업 중인 `.geo` 바이너리 파일 헤더가 깨져 스토리지 부패가 일어나지 않도록 커널 스레드의 강제 종료를 최대 5초간 안전하게 유예시킵니다.
    // [2. 영문 상세 주석]
    // [Termination Dynamics Stage] Safely reclaims the background rendering thread pool upon OS shutdown.
    // 💡 [Graceful Teardown Plumbing Reinforced] Newly transplanted `awaitTermination` logic to safely delay the forced termination of kernel threads for up to 5 seconds to prevent storage corruption caused by the header of the `.geo` binary file currently being physically written breaking.

    /**
     * [생명주기 종결] OS 종료 시 스레드 격벽 자원 회수 (L5 생명주기 관제탑)
     */
    public void shutdownOrchestrator() {
        if (renderBulkheadThreadPool != null) {
            // 더 이상의 신규 베이킹 큐 제출(Submit)을 물리적으로 차단
            renderBulkheadThreadPool.shutdown();
            try {
                // 현재 진행 중인 mmap 파일 I/O 직사(Direct Write) 작업이 반쪽짜리로 깨지지 않고 디스크에 안전하게 동기화(Flush) 완료될 수 있도록 스레드 강제 중단을 5초간 유예
                if (!renderBulkheadThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.warning("   ├─ [L5 오케스트레이터 자원 회수 경보] GEO 렌더링 격벽 스레드가 5초 유예 시간 내에 I/O를 종료하지 않아 데이터 유실을 감수하고 강제 전원 절단(shutdownNow)을 집행합니다.");
                    renderBulkheadThreadPool.shutdownNow();
                } else {
                    logger.info("   ├─ [L5 오케스트레이터 자원 회수 완료] GEO 에셋 오케스트레이터의 렌더링 스레드 격벽(Bulkhead)이 시스템 메모리에서 안전하게 해제(Release)되었습니다.");
                }
            } catch (InterruptedException e) {
                renderBulkheadThreadPool.shutdownNow();
                Thread.currentThread().interrupt(); // 인터럽트 플래그 복구
            }
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 및 시스템 아키텍처 (Theoretical Philosophy & Engineering Principles)]
 * 
 * 1. 런타임 제어의 역전(IoC: Inversion of Control)과 메모리 맵(mmap) 생명주기의 완벽한 통제:
 * 이번 V6.1 리메이크 아키텍처 수술의 핵심은 상위 오케스트레이터(L5 계층)가 시스템 파이프라인 '배관공'의 역할을 완벽하게 수행하게 되었다는 점입니다.
 * 낡은 구세대 코드에서는 422131(프로젝터) 모듈이 거대한 `ByteBuffer` 힙 객체를 반환(Return)하면, 이를 422502(이 클래스)가 다시 받아 순차 반복문(for-loop)을 돌리며 
 * 422132(워커)에 매개변수로 밀어 넣었습니다. 이는 논리적으로 모듈화되어 예뻐 보일지언정, 물리적 하드웨어 단에서는 무의미한 거대 배열 객체 힙 할당과 CPU 메모리 버스(Memory Bus) 사이클을 극한으로 낭비하는 끔찍한 병목 안티 패턴이었습니다.
 * 
 * 수복된 엔진에서는 L5 마스터 관제탑이 `A0_DT_42_422132_GEO_바이너리_베이킹_워커`를 `try-with-resources` 블록으로 안전하게 감싸서 팩토리 생성한 뒤, 
 * 이 워커 포인터 자체를 하위 계층인 `방사형_프로젝터` 모듈의 렌더링 API 매개변수로 그대로 밀어 넣어 주입(Inject)해 버립니다.
 * 프로젝터는 수학 연산이 끝난 찰나의 좌표값을 어딘가의 컬렉션이나 힙 버퍼에 임시로 담아둘 필요조차 없이, 주입받은 워커를 통해 
 * 즉시 디스크 표면(OS 페이지 캐시)으로 쏴버립니다(Direct Dump). 
 * L5 오케스트레이터는 오직 워커 자원의 생성과 안전한 회수(Lifecycle)만을 전담 관제하고 데이터 좌표의 실제 흐름(Flow) 자체에는 일절 개입하지 않는, 완벽하고 우아한 제어의 역전(IoC) 및 제로-카피(Zero-Copy) 파이프라인이 성취되었습니다.
 * 
 * 2. L5 판옵티콘 통제와 스레드 고립 방어 전략 (Thread Bulkheading):
 * 거대한 여객선의 밑바닥에 구멍이 나도 배 전체가 침몰하지 않도록 공간을 막는 선박의 '격벽(Bulkhead)' 방어 패턴의 정수입니다.
 * 3D 그래픽을 빚어내기 위한 기하학 삼각함수 베이킹은 CPU의 부동소수점 연산기(ALU) 레지스터를 극한으로 쥐어짜는 매우 무거운 연산 작업입니다.
 * 만약 L4 계층 내부에서 무분별하게 렌더링 워커 스레드를 생성하도록 방치하거나 기존 코어 스레드 풀을 공유하게 둔다면, 그래픽을 렌더링하는 동안 
 * 시스템 본연의 목적인 AI의 추론 지능(TDQI)은 정지하고 데이터 아카이브 적재 I/O는 멈춰버리는 자원 기아(Resource Starvation) 및 시스템 교착 상태에 빠집니다.
 * 본 L5 오케스트레이터는 직접 `Thread.MIN_PRIORITY`라는 OS 스케줄링 최하위 레벨을 물리적으로 강제 부여받은 '단일(Single) 격벽 스레드 풀'을 백그라운드에 창조합니다.
 * 사용자 모니터의 3D 렌더링 화면이 다소 버벅거리며 프레임이 떨어지는 한이 있더라도, 코어 서버의 본질인 AI 추론 연산망과 DB 파이프라인 I/O 처리 트랜잭션에는 절대적 우선순위와 하드웨어 자원을 무조건 양보하는 지독한 백엔드 중심적 권력 통제 구조를 구현했습니다.
 * 
 * 3. 원자적 락(Atomic Lock)을 통한 폭주 방어와 지연 기동 최적화 (Lazy Initialization):
 * 성급한 사용자가 UI 화면을 마구 드래그하거나 "시각화 새로고침" 버튼을 연타할 때, 무거운 3D 렌더링 엔진이 중복으로 여러 개 돌아가 시스템 램(RAM)을 터뜨리고 크래시를 내는 것을 원천 방어하기 위해,
 * `AtomicBoolean.compareAndSet(false, true)` 단 한 줄의 하드웨어 마법으로 그 어떠한 무수한 렌더링 폭주 요청도 락 획득 실패 시 스레드 대기열 생성 없이 $O(1)$ 속도로 차갑게 튕겨냅니다(Drop).
 * 또한, 사용자가 시각화 창을 실제로 띄우기 전까지는 렌더링 백그라운드 스레드 풀과 무거운 기하학 프로젝터 엔진 클래스가 힙 메모리에 단 1바이트의 쓰레기 공간도 차지하지 않도록 
 * 정통적인 이중 검사 잠금(DCL: Double-Checked Locking) 아키텍처 패턴을 통한 극강의 지연 기동(Lazy Initialization)을 적용했습니다. 
 * 이는 "진짜 자원이 필요한 그 최초의 찰나"에만 렌더링 물리 엔진 객체를 JVM 공간에 창세(Materialize)하는 극단적이고 결벽적인 메모리 자원 최적화 설계의 결정체입니다.
 * 
 * 4. 안전 강하 대기 파이프라인 (Graceful Teardown)의 필연적 당위성:
 * 호스트 운영체제(OS)가 SIGTERM 종료 시그널을 받을 때, 백그라운드 하위 스레드 풀을 급하다고 무자비하게 `shutdownNow()`로 강제 파괴해 버리면, 
 * 디스크 I/O 스트림 중에 있던 `.geo` 기하학 바이너리 파일의 헤더(Header)나 메타데이터가 영구적으로 손상되거나 반쪽짜리 크기의 찌꺼기 파일이 스토리지에 덩그러니 남아 치명적인 인프라 부패(Corruption)를 유발합니다. 
 * `awaitTermination(5s)` 규격은 현재 연산 작업 중인 파일 포인터의 OS 커널 램 플러시(Flush)가 끊기지 않고 물리 디스크 바닥에 온전히 끝마쳐질 수 있도록, 스레드 참수를 5초간 강제로 물리적 유예(Delay)시키는 시스템의 마지막 방어막입니다.
 * =============================================================================
 */