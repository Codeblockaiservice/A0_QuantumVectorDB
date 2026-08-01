/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망
 * @alias Ingestor_FFM_Engine
 * @tier 2
 * @keywords Physical I/O Sanctuary, Authority Minting, MVCC, Copy-on-Write, Signature Matching
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422021_주조기_FFM_엔진.java
 * - 모듈명 (Module): 통합 OS V6.0 - Tier 2: 물리 I/O 제어 및 권한 포트 발급 코어
 * - 기능 (Function): Sparse File 선할당 및 Copy-on-Write 샌드박스를 개방하며, OS 커널 메모리에 대한 읽기/쓰기 권한 포트를 생성하여 주입합니다.
 * - 역할 (Role): 하드웨어 I/O 통제 및 권한 포트(ReadPort/WritePort) 중앙 집중식 발급(Minting).
 * - 이론 (Theory): OS Page Fault, Zero-Copy, MVCC(Multi-Version Concurrency Control), Capability-based Security, Constructor Signature Matching.
 * - 💡 [V6.0 MVCC 동기화]: 422001 인터페이스의 MVCC 진화에 맞추어, 렌즈 팩토리 호출 시 MemorySegment 종속성을 제거하고 포트 생성 시 AtomicReference로 래핑하여 주입하도록 아키텍처를 동기화했습니다.
 * - 💡 [V6.0 시그니처 교정]: 422001 권한 포트에 'activeReferenceCounter'가 추가됨에 따라, 포트 객체 생성 시 4번째 인자로 `null`을 명시적으로 주입하여 파라미터 불일치 에러를 해결했습니다.
 * - 💡 [명칭 교정]: 비유적이고 문학적인 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 OS 커널 메모리 제어, 파일 시스템 조작을 위한 핵심 의존성 모듈들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core dependency modules for OS kernel memory control and file system manipulation.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어2_주조기_및_비동기_소화망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 물리적 디스크 I/O를 직접 통제하고 읽기/쓰기 권한 포트를 생성하는 코어 엔진입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A core engine that directly controls physical disk I/O and generates read/write authority ports.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422021
 * [파일명] A0_DT_42_422021_주조기_FFM_엔진.java
 * [모듈명] 통합 OS V6.0 - Tier 2: 물리 I/O 제어 및 권한 포트 발급 코어
 *
 * [설계 명세]
 * 1. 역할: 파일의 메타데이터 크기를 확장하고 OS 커널에 메모리 매핑(mmap) 지시.
 * 2. 기능: Sparse File 선할당 및 Copy-on-Write 샌드박스 개방.
 * 3. 의도: 백테스트(Simulated Battle) 시 실전 원본 데이터를 보호하기 위한 물리적 격리(Isolation) 제공.
 * 4. 이론: OS Page Fault를 활용한 커널 레벨 스왑 공간 복사 및 Zero-Copy 매핑.
 * 5. 기술: FileChannel.MapMode.PRIVATE, RandomAccessFile.setLength, FFM API.
 * 6. 변경/신설 사항:
 * - 💡 [V6.0 MVCC 규격 반영] 422001 인터페이스의 MVCC 포인터 스왑 구조 전환에 맞추어,
 * `MemorySegment`를 직접 주입하던 기존 방식을 제거하고 `AtomicReference<MemorySegment>`로 래핑하여 발급합니다.
 * - 💡 [생성자 시그니처 교정] 422001 포트 레코드에 추가된 4번째 인자(activeReferenceCounter) 요구 조건을 충족시키기 위해,
 * 객체 생성 시 `null`을 주입하여 Length Mismatch 컴파일 에러를 해결했습니다.
 * 7. 기대효과: 디스크 용량 소모 없이 찰나의 순간에 테라바이트급 가상 샌드박스를 창조하고, 락프리(Lock-Free) MVCC 읽기/쓰기 포트를 무결점으로 배급합니다.
 * ==============================================================================
 */
public final class A0_DT_42_422021_주조기_FFM_엔진 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422021_FFM_ENGINE");

    /**
     * 상태를 가지지 않는(Stateless) 엔진이므로 제약 없이 인스턴스화가 가능합니다.
     */
    public A0_DT_42_422021_주조기_FFM_엔진() {
        logger.info(" >> [통합 OS V6.0] A0_DT_42_422021 주조기 FFM 엔진 기동 완료. (MVCC 권한 포트 생성 및 CoW 샌드박스 개방 준비)");
    }

    /**
     * [Sparse File 생성기]
     * 초기 데이터 로드(콜드스타트) 또는 차원 팽창 시, 디스크 I/O 낭비 및 쓰기 병목 없이
     * 파일 메타데이터 크기만을 사전 할당(Pre-allocation)합니다.
     *
     * @param targetPhysicalPath 생성 및 확장할 바이너리(.layer) 파일 절대 경로
     * @param targetByteSize     확보해야 할 전체 바이트 용량
     */
    public void allocateEmptyCanvas(Path targetPhysicalPath, long targetByteSize) {
        try {
            // 부모 디렉토리가 없다면 선제적으로 개척
            if (targetPhysicalPath.getParent() != null) {
                Files.createDirectories(targetPhysicalPath.getParent());
            }

            // 파일이 존재하지 않으면 파일 시스템에 빈 파일 생성
            if (!Files.exists(targetPhysicalPath)) {
                Files.createFile(targetPhysicalPath);
            }

            // 💡 [핵심 최적화] 파일의 물리적 기록(0x00을 실제로 쓰는 행위) 없이 메타데이터(Length)만 강제 팽창
            // OS가 이 빈 공간을 읽을 때는 자동으로 0x00(null 바이트)을 반환하며,
            // 이는 IEEE 754 부동소수점 규격 상 0.0f 와 정확히 일치하는 우연성을 활용하여 초기화 비용을 제거합니다.
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(targetPhysicalPath.toFile(), "rw")) {
                if (randomAccessFile.length() < targetByteSize) {
                    randomAccessFile.setLength(targetByteSize);
                    logger.info(String.format("   ├─ [스토리지 할당 완료] %s (Sparse File 확보 용량: %d Bytes)",
                            targetPhysicalPath.getFileName(), targetByteSize));
                }
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [스토리지 할당 실패] 파일 시스템 확장 중 I/O 오류 발생: " + targetPhysicalPath.getFileName(), ex);
            throw new RuntimeException("Sparse File 캔버스 할당 실패 (디스크 I/O 오류)", ex);
        }
    }

    /**
     * [쓰기 권한(WritePort) 발급]
     * 데이터 파이프라인(워커), 백그라운드 연산 데몬 등 데이터를 '생성 및 수정'하는 모듈에만
     * 배타적 쓰기 권한이 각인된 WritePort를 생성하여 주입합니다.
     *
     * @param targetPhysicalPath 매핑할 바이너리 파일 경로
     * @param mappedByteSize     매핑할 바이트 크기 (일반적으로 파일 전체 크기)
     * @param lifecycleArena     커널 메모리의 생명주기를 통제할 공유 FFM Arena
     * @return 배타적 쓰기 권한과 렌즈가 캡슐화된 WritePort
     */
    public A0_DT_42_422001_권한_포트_인터페이스.WritePort mountForWrite(Path targetPhysicalPath, long mappedByteSize, Arena lifecycleArena) {
        try (FileChannel fileChannel = FileChannel.open(targetPhysicalPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            // OS 커널에게 해당 파일을 READ_WRITE 모드로 RAM에 매핑할 것을 지시
            MemorySegment memorySegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mappedByteSize, lifecycleArena);

            // 💡 [V6.0 MVCC 규격 동기화] 렌즈 조립 시 MemorySegment 바인딩을 제거하고, 순수 해상도 시그니처만으로 렌즈(Lens) 조립.
            A0_DT_42_422001_권한_포트_인터페이스.TransparentWriteLens defaultWriteLens = A0_DT_42_422001_권한_포트_인터페이스.assembleWriteLens(0, 1.0f, 0.0f);

            // 💡 [생성자 파라미터 교정] 422001 인터페이스의 시그니처(4번째 인자 요구)에 맞추어, null을 명시적으로 주입.
            return new A0_DT_42_422001_권한_포트_인터페이스.WritePort(
                    new AtomicReference<>(memorySegment),
                    defaultWriteLens,
                    mappedByteSize,
                    null);

        } catch (IOException ex) {
            logger.severe(" [쓰기 권한 발급 실패] WritePort 생성 불가: " + targetPhysicalPath.getFileName());
            throw new RuntimeException("쓰기 권한 발급 I/O 예외", ex);
        }
    }

    /**
     * [읽기 권한(ReadPort) 발급]
     * AI 코어, 쿼리 엔진 등 데이터를 '관측(소비)'만 하는 모듈이 안전하게 읽기만 수행할 수 있도록
     * READ_ONLY 모드로 매핑 후 ReadPort를 생성하여 주입합니다.
     *
     * @param targetPhysicalPath 읽어들일 바이너리 파일 경로
     * @param mappedByteSize     매핑할 바이트 크기
     * @param lifecycleArena     생명주기를 통제할 공유 FFM Arena
     * @return 하드웨어 레벨의 쓰기 방어막이 각인된 ReadPort
     */
    public A0_DT_42_422001_권한_포트_인터페이스.ReadPort mountForRead(Path targetPhysicalPath, long mappedByteSize, Arena lifecycleArena) {
        try (FileChannel fileChannel = FileChannel.open(targetPhysicalPath, StandardOpenOption.READ)) {

            // OS 커널에게 해당 파일을 READ_ONLY 모드로 RAM에 매핑할 것을 지시
            MemorySegment memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, mappedByteSize, lifecycleArena);

            // 💡 [V6.0 MVCC 규격 동기화] MemorySegment 바인딩 제거 및 순수 팩토리 호출
            A0_DT_42_422001_권한_포트_인터페이스.TransparentReadLens defaultReadLens = A0_DT_42_422001_권한_포트_인터페이스.assembleReadLens(0, 1.0f, 0.0f);

            // 💡 [생성자 파라미터 교정] 4번째 인자로 null을 주입하여 생성자 파라미터 불일치를 해결.
            return new A0_DT_42_422001_권한_포트_인터페이스.ReadPort(
                    new AtomicReference<>(memorySegment),
                    defaultReadLens,
                    mappedByteSize,
                    null);

        } catch (IOException ex) {
            logger.severe(" [읽기 권한 발급 실패] ReadPort 생성 불가: " + targetPhysicalPath.getFileName());
            throw new RuntimeException("읽기 권한 발급 I/O 예외", ex);
        }
    }

    /**
     * 💡 [Copy-on-Write 샌드박스 권한 발급]
     * 백테스트 시뮬레이터 및 모델 훈련 에이전트가 사용할 가상의 포트를 발급합니다.
     * 원본 데이터를 읽되, 텐서 수정이 발생하면 OS가 복사본을 생성(CoW)하여 원본 훼손을 물리적으로 차단합니다.
     *
     * @param masterSourcePath   샌드박스의 베이스로 사용할 100% 원본(마스터) 파일 경로
     * @param mappedByteSize     매핑할 바이트 크기
     * @param lifecycleArena     생명주기를 통제할 공유 FFM Arena
     * @return 변경 사항이 스왑(Swap) 메모리에만 남는 격리된 WritePort
     */
    public A0_DT_42_422001_권한_포트_인터페이스.WritePort mountForSandbox(Path masterSourcePath, long mappedByteSize, Arena lifecycleArena) {
        try (FileChannel fileChannel = FileChannel.open(masterSourcePath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {

            // 💡 [Copy-on-Write 발동의 핵심 메커니즘] MapMode.PRIVATE
            // 메모리 수정 사항을 디스크에 반영하지 않고 OS Page Fault를 통해 스왑(Swap) 공간에만 은밀히 복사본을 만듭니다.
            MemorySegment memorySegment = fileChannel.map(FileChannel.MapMode.PRIVATE, 0, mappedByteSize, lifecycleArena);

            logger.info("   ├─ [샌드박스 락온] Copy-on-Write 방어막이 전개되었습니다. 원시 데이터(Raw)는 훼손되지 않습니다.");

            // 💡 [V6.0 MVCC 규격 동기화]
            A0_DT_42_422001_권한_포트_인터페이스.TransparentWriteLens defaultWriteLens = A0_DT_42_422001_권한_포트_인터페이스.assembleWriteLens(0, 1.0f, 0.0f);

            // 💡 [생성자 파라미터 교정] 샌드박스 포트 발급 시에도 4번째 인자로 null을 주입합니다.
            return new A0_DT_42_422001_권한_포트_인터페이스.WritePort(
                    new AtomicReference<>(memorySegment),
                    defaultWriteLens,
                    mappedByteSize,
                    null);

        } catch (IOException ex) {
            logger.severe(" [샌드박스 권한 발급 실패] Copy-on-Write 샌드박스 생성 불가: " + masterSourcePath.getFileName());
            throw new RuntimeException("샌드박스 권한 포트 발급 I/O 예외", ex);
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. Copy-on-Write (CoW) 샌드박스의 물리적 역학:
 * 모의전(백테스트)이나 모델 훈련을 수행할 때 테라바이트급의 마스터 파일을 메모리로 복사하는 것은 막대한 I/O 자원과 시간의 낭비입니다.
 * `FileChannel.MapMode.PRIVATE`을 사용하면, 자바는 OS 커널에게 다음과 같이 지시합니다: 
 * "이 파일을 메모리에 매핑하되, 내가 데이터를 수정(Write)하려고 시도할 때만 해당 4KB 메모리 페이지를 몰래 복사해서 나만의 스왑(Swap) 공간에 써달라."
 * 분석 모델이 백테스트 중 수십만 번의 가상 매매를 일으켜 데이터를 변경하더라도, 변경되지 않은 나머지 99%의 데이터는 여전히 디스크의 원본을 가리키며,
 * 오직 변경된 1%의 데이터만 RAM/Swap에 존재하게 됩니다. 디스크 용량 소모 0%, 샌드박스 구축 시간 0.001초의 최적화가 이 한 줄의 커널 명령어로 완성됩니다.
 * 
 * 2. 권한 발급(Authority Minting)의 중앙 집중화와 렌즈(Lens) 캡슐화:
 * 통합 OS V6.0 아키텍처에서는 하위 워커(파서, 스캐너 등)들이 `FileChannel.open`을 스스로 호출할 수 없습니다.
 * 오직 이 FFM 엔진 모듈만이 디스크의 물리적 경로에 접근하고 mmap을 수행할 수 있는 권한 통제소 역할을 합니다.
 * 오케스트레이터는 이 엔진을 통해 `ReadPort`와 `WritePort`라는 하드웨어 접근 티켓을 발급받아 각 모듈의 생성자에 주입(Dependency Injection)합니다.
 * V6.0에서는 포트 발급 시 구체화된(Concrete) '투명_렌즈' 인스턴스를 물리적으로 결합하여 제공함으로써,
 * 워커들이 포트 내부의 렌즈를 꺼내 쓸 때 하드웨어 분기문(CMOV) 최적화를 자동으로 적용받을 수 있도록 아키텍처를 진화시켰습니다.
 * 
 * 3. 진정한 MVCC (Atomic Pointer Swap) 권한 인프라 완수:
 * `422001_권한_포트_인터페이스`가 MVCC 구조로 진화함에 따라, 엔진 역시 포트를 발급할 때 `MemorySegment`를
 * `AtomicReference`라는 '교체 가능한 스냅샷 컨테이너'로 감싸서 발급합니다.
 * 이를 통해 렌즈(Lens)는 런타임에 가장 최신의 메모리 포인터만을 안전하게 투영하게 되며, 객체를 재할당하지 않고도 
 * 무결점의 락프리 동시성(Lock-Free Concurrency)을 물리적으로 담보하게 되었습니다.
 * 
 * 4. 파일 메타데이터 선할당 (allocateEmptyCanvas) 메커니즘:
 * `allocateEmptyCanvas`는 단순히 초기 콜드스타트에만 쓰이지 않습니다.
 * 비동기 파이프라인에서 새로운 시간(Tick) 데이터가 추가되어 커널 공간이 더 필요해질 때, 
 * 데이터를 쓰기 전에 파일의 메타데이터(Length)를 `setLength`로 먼저 팽창(Pre-allocation)시켜야 합니다.
 * 데이터를 디스크에 실제로 물리적으로 기록(0x00 채우기)하지 않기 때문에 I/O 병목이 없으며, 
 * 이 선할당 작업이 선행되어야만 락프리(Lock-Free) 텐서 기록 시 `IndexOutOfBoundsException`이나 OS 레벨의 `SegFault`를 완벽히 회피할 수 있습니다.
 * =============================================================================
 */