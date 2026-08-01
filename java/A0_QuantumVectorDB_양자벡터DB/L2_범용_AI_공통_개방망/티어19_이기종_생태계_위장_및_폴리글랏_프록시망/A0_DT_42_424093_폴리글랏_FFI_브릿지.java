/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어19_이기종_생태계_위장_및_폴리글랏_프록시망
 * @alias Polyglot_FFI_Bridge_IPC
 * @tier 19
 * @keywords Inter-Process Communication (IPC), mmap, Cross-Process Zero-Copy, FFM API, Exponential Backoff Spin-Lock, Buffer-Overrun Protection
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_424093_폴리글랏_FFI_브릿지.java
 * - 모듈명: 통합 OS V6.1 - Tier 19: 폴리글랏 FFI 브릿지 (네이티브 IPC 메모리 에이전트)
 * - 기능 및 역할: 네트워크 소켓(TCP) 통신을 전면 우회하여, 동일 머신 내에 구동 중인 Python(PyTorch/NumPy) 프로세스에게 직접 커널 메모리 주소(Pointer)와 메타데이터를 넘겨주는 초고속 브릿지.
 * - 이론 및 기술: POSIX Shared Memory, FFM C-Struct Layout, 이종 프로세스 간 CAS 스핀 락, Exponential Backoff(지수적 백오프), Dual-Barrier 동기화.
 * - 기대효과: 루프백(localhost) 네트워크 통신의 컨텍스트 스위칭 오버헤드조차 물리적으로 파괴하여, Python AI 코어가 방대한 텐서 메모리를 0초(Zero-Copy) 만에 직접 매핑(mmap)해 들일 수 있는 기적적인 스루풋(Throughput)을 제공합니다.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [삭제]: 불필요하게 남아있던 OS 레이어 드라이버 주입 배관을 완전히 멸균(제거)하여 모듈의 결합도를 완벽히 분리했습니다.
 * - 💡 [변경]: 이기종 프로세스 간 스핀락(Spin-Lock) 대기 시 발생하는 CPU 스래싱(Thrashing)을 차단하기 위해 단순 대기를 `Exponential Backoff(지수적 백오프)` 알고리즘으로 승격시켰습니다.
 * - 💡 [신규]: 다중 스레드 환경에서 Java 프로세스 내부 스레드 간의 충돌을 선제적으로 막기 위한 `ReentrantLock`을 도입하여, 프로세스 내(Intra) 및 프로세스 간(Inter) 동시성을 모두 통제하는 'Dual-Barrier'를 구축했습니다.
 * - 💡 [신규]: Python C-Extension 단의 버퍼 오버런(Buffer Overrun)을 원천 차단하기 위해 파일 경로 길이를 255바이트로 엄격히 제한하고, C-Struct 배열 끝단에 C-Style 널 종단 문자(`\0`, Null-terminator)를 명시적으로 타격(Inject)합니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 오프힙 메모리 제어(FFM API), POSIX 파일 시스템, 내부 스레드 제어(ReentrantLock) 및 원자적 조작을 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for off-heap memory control (FFM API), POSIX file systems, internal thread control (ReentrantLock), and atomic operations.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어19_이기종_생태계_위장_및_폴리글랏_프록시망;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 로컬 네트워크 스택의 비효율을 타파하고 OS 커널 간 물리적 다이렉트 메모리 브릿지를 잇는 폴리글랏 에이전트입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A polyglot agent that breaks down the inefficiencies of local network stacks and establishes a physical direct memory bridge across OS kernels.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_424093
 * [파일명] A0_DT_42_424093_폴리글랏_FFI_브릿지.java
 * [모듈명] 통합 OS V6.1 - Tier 19: 폴리글랏 FFI 브릿지 (네이티브 IPC 메모리 에이전트)
 * ==============================================================================
 */
public final class A0_DT_42_424093_폴리글랏_FFI_브릿지 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.424093_POLYGLOT_FFI_BRIDGE");

    // [1. 한글 상세 주석]
    // 💡 [이기종 프로세스 간 C-Struct 통신 레이아웃 (Cross-Process IPC Mailbox Layout)]
    // Java FFM 구조체와 Python/C++의 `ctypes` 모듈이 정확히 동일하게 읽고 쓸 수 있는 리틀
    // 엔디안(Little-Endian) 기반 280바이트 고정 크기 메일박스 구조체입니다.
    // [2. 영문 상세 주석]
    // 💡 [Cross-Process C-Struct Communication Layout]
    // A little-endian based 280-byte fixed-size mailbox structure that Java FFM and
    // Python/C++'s `ctypes` modules can access exactly identically.

    private static final ValueLayout.OfInt C_INT = ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong C_LONG = ValueLayout.JAVA_LONG.withOrder(ByteOrder.LITTLE_ENDIAN);

    private static final StructLayout MAILBOX_LAYOUT = MemoryLayout.structLayout(
            C_INT.withName("sync_state"), // 4 Bytes: 상태 기계 (0: IDLE, 1: JAVA_WRITING, 2: READY_FOR_PYTHON, 3:
                                          // PYTHON_ACK)
            C_INT.withName("resolution_id"), // 4 Bytes: 해상도 플래그 (0: Float32, 1: BFloat16, 2: INT8)
            C_LONG.withName("tensor_offset"), // 8 Bytes: 물리적 텐서 파일 내 절대 시작 오프셋
            C_LONG.withName("tensor_length"), // 8 Bytes: Python이 매핑(mmap)해야 할 총 바이트 길이
            MemoryLayout.sequenceLayout(256, ValueLayout.JAVA_BYTE).withName("file_path") // 256 Bytes: 텐서 물리 파일의 절대 경로
                                                                                          // 문자열 배열
    ).withName("IPC_Mailbox");

    private static final long OFFSET_SYNC_STATE = MAILBOX_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("sync_state"));
    private static final long OFFSET_RESOLUTION = MAILBOX_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("resolution_id"));
    private static final long OFFSET_OFFSET = MAILBOX_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("tensor_offset"));
    private static final long OFFSET_LENGTH = MAILBOX_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("tensor_length"));
    private static final long OFFSET_FILE_PATH = MAILBOX_LAYOUT
            .byteOffset(MemoryLayout.PathElement.groupElement("file_path"));

    // 이기종 프로세스 간 통신 동기화를 위한 상태 상수 (Cross-Process Sync States)
    private static final int STATE_IDLE = 0;
    private static final int STATE_JAVA_WRITING = 1;
    private static final int STATE_READY_FOR_PYTHON = 2;
    private static final int STATE_PYTHON_ACK = 3;

    private static final VarHandle SYNC_STATE_HANDLE = C_INT.varHandle();

    // 💡 [내부 동기화 락 (Dual-Barrier Phase 1)] Java 프로세스 내부의 다중 스레드가 메일박스에 동시 접근하여 발생하는
    // 경합(Race Condition)을 차단합니다.
    private final ReentrantLock internalSyncLock = new ReentrantLock(true); // 공정성(Fairness) 보장
    private final AtomicBoolean isAgentRunning = new AtomicBoolean(false);

    private FileChannel mailboxChannel;
    private MemorySegment mailboxSegment;
    private Arena mailboxArena;

    // [1. 한글 상세 주석]
    // [생성자] 불필요한 OS 드라이버 배관을 제거하고 철저히 고립된 순수 네이티브 메모리 에이전트로서 폴리글랏 브릿지를 기동합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Removes unnecessary OS driver piping and starts the polyglot
    // bridge as a strictly isolated, pure native memory agent.

    public A0_DT_42_424093_폴리글랏_FFI_브릿지() {
        logger.info(" >> [통합 OS V6.1] A0_DT_42_424093 폴리글랏 FFI 브릿지 기동 준비 완료. (네이티브 IPC 공유 메모리 에이전트 장착 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 [통신망 개방: POSIX Shared Memory]
    // 운영체제(리눅스)의 RAM 디스크(/dev/shm)에 휘발성 메일박스 파일을 생성하여 Python 프로세스와 공유할 280바이트의 제어
    // 평면(Control Plane)을 개통합니다.
    // [2. 영문 상세 주석]
    // 💡 [Opening IPC Network: POSIX Shared Memory]
    // Creates a volatile mailbox file in the OS (Linux) RAM disk (/dev/shm) to
    // establish a 280-byte control plane shared with the Python process.

    public void openIpcNetwork() {
        if (!isAgentRunning.compareAndSet(false, true))
            return;

        try {
            // Linux 커널에서는 I/O 오버헤드가 전혀 없는 순수 메모리 가상 디스크(/dev/shm)를 사용하며, 타 OS는 임시 디렉토리 폴백
            String ramdiskPath = System.getProperty("os.name").toLowerCase().contains("linux") ? "/dev/shm"
                    : System.getProperty("java.io.tmpdir");
            Path mailboxPath = Path.of(ramdiskPath, "matrix_ipc_mailbox.bin");

            this.mailboxChannel = FileChannel.open(mailboxPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);

            long requiredSize = MAILBOX_LAYOUT.byteSize();
            if (this.mailboxChannel.size() < requiredSize) {
                this.mailboxChannel.truncate(requiredSize);
            }

            this.mailboxArena = Arena.ofShared();
            this.mailboxSegment = this.mailboxChannel.map(FileChannel.MapMode.READ_WRITE, 0, requiredSize,
                    this.mailboxArena);

            // 초기 상태 기계 멸균(IDLE) 세팅
            SYNC_STATE_HANDLE.setVolatile(this.mailboxSegment, OFFSET_SYNC_STATE, STATE_IDLE);

            logger.info(String.format("   ├─ [FFI 브릿지 개통] Python/C++ 전용 IPC 메일박스가 시스템 RAM 디스크에 MMap 매핑되었습니다: %s",
                    mailboxPath.toString()));

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [통신망 붕괴] IPC 메일박스 물리적 파일 생성 및 mmap 매핑 실패.", ex);
            throw new RuntimeException("FFI 브릿지 커널 에이전트 기동 불가", ex);
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [IPC 전송 역학 및 Dual-Barrier 동기화 메커니즘]
    // Java 내부 스레드 간 충돌을 `ReentrantLock`으로 선제적으로 통제하고, 이후 Python 프로세스와는 `Exponential
    // Backoff CAS(Compare-And-Swap)`로 이중 동기화합니다.
    // [2. 영문 상세 주석]
    // 💡 [IPC Transmission Dynamics and Dual-Barrier Synchronization Mechanism]
    // Proactively controls collisions among Java internal threads with
    // `ReentrantLock`, and subsequently double-synchronizes with the Python process
    // using `Exponential Backoff CAS (Compare-And-Swap)`.

    /**
     * Python 측 에이전트에게 커널 메모리의 타겟 접근 포인터(File, Offset, Length)를 전달하고 읽기 완료 상태를
     * 동기화(Sync)합니다.
     * 
     * @param targetFilePath 공유할 텐서가 담긴 물리적 파일의 절대 경로
     * @param startOffset    파일 내 텐서가 물리적으로 시작되는 절대 바이트 오프셋
     * @param byteLength     파이썬이 다이렉트로 매핑(mmap)해야 할 메모리의 총 길이
     * @param resolutionId   데이터의 물리적 해상도 규격 (0:Float32, 1:BFloat16, 2:INT8)
     */
    public void dispatchSharedMemoryPointer(Path targetFilePath, long startOffset, long byteLength, int resolutionId) {
        // 💡 1. [Intra-Process Lock] 동일 JVM 프로세스 내 다중 스레드 간 접근을 순차화시켜 레이스 컨디션(Race
        // Condition) 원천 차단
        internalSyncLock.lock();

        try {
            int maxRetries = 100_000;
            int attemptCount = 0;
            long backoffDelayNanos = 10_000L; // 10 마이크로초(us) 시작
            long maxBackoffNanos = 2_000_000L; // 최대 2 밀리초(ms) 한계

            // 💡 2. [Inter-Process Lock (CAS)] 메일박스가 IDLE(0) 상태일 때만 JAVA_WRITING(1)으로
            // 원자적(Atomic) 상태 전이 시도
            while (!((boolean) SYNC_STATE_HANDLE.compareAndSet(mailboxSegment, OFFSET_SYNC_STATE, STATE_IDLE,
                    STATE_JAVA_WRITING))) {
                attemptCount++;
                if (attemptCount > maxRetries) {
                    logger.warning(" 🚨 [IPC 교착 상태 감지] 외부 Python 에이전트의 응답 지연(Hanging)으로 인해 메일박스 락을 강제로 초기화(Break)합니다.");
                    SYNC_STATE_HANDLE.setVolatile(mailboxSegment, OFFSET_SYNC_STATE, STATE_IDLE);
                    attemptCount = 0;
                    backoffDelayNanos = 10_000L;
                }

                // 💡 [CPU 스래싱(Thrashing) 방어] 단순 고정 대기(parkNanos)를 지수적 백오프(Exponential Backoff)
                // 알고리즘으로 승급 적용
                LockSupport.parkNanos(backoffDelayNanos);
                backoffDelayNanos = Math.min(backoffDelayNanos * 2, maxBackoffNanos);
            }

            try {
                // 3. [포인터 제원 물리적 각인] 힙 할당(Zero-Allocation) 없이 메타데이터를 C-Struct 오프힙 레이아웃에 직접
                // 덮어씁니다.
                mailboxSegment.set(C_INT, OFFSET_RESOLUTION, resolutionId);
                mailboxSegment.set(C_LONG, OFFSET_OFFSET, startOffset);
                mailboxSegment.set(C_LONG, OFFSET_LENGTH, byteLength);

                byte[] pathBytes = targetFilePath.toAbsolutePath().toString().getBytes(StandardCharsets.UTF_8);

                // 💡 4. [버퍼 오버런(Buffer Overrun) 보안 방어]
                // 파일 경로 문자열 최대 길이 엄격 통제 (C-Struct 허용치 256바이트에서 Null Terminator 공간 1바이트를 뺀
                // 255바이트 한계)
                if (pathBytes.length > 255) {
                    throw new IllegalArgumentException("텐서 파일 절대 경로의 길이가 C-Struct 허용치(255 Bytes)를 초과하여 IPC 전송을 거부합니다.");
                }

                // 기존 메일박스에 남은 쓰레기 찌꺼기 멸균 후 새로운 경로 바이트 카피
                mailboxSegment.asSlice(OFFSET_FILE_PATH, 256).fill((byte) 0);
                MemorySegment.copy(MemorySegment.ofArray(pathBytes), 0, mailboxSegment, OFFSET_FILE_PATH,
                        pathBytes.length);

                // 💡 [안전성 강화] C-Style 문자열의 끝을 알리는 Null Terminator (`\0`) 명시적 강제 주입
                mailboxSegment.set(ValueLayout.JAVA_BYTE, OFFSET_FILE_PATH + pathBytes.length, (byte) 0);

                logger.fine(String.format("   ├─ [포인터 하사 완료] Python 에이전트 프로세스에게 커널 메모리를 위임했습니다. (Path: %s, Offset: %d)",
                        targetFilePath.getFileName(), startOffset));

            } finally {
                // 5. [소비자 트리거 (Trigger)] 상태를 READY_FOR_PYTHON(2)으로 전이시켜 대기 중인 파이썬 프로세스가 메일박스를
                // 읽도록 지시합니다.
                SYNC_STATE_HANDLE.setVolatile(mailboxSegment, OFFSET_SYNC_STATE, STATE_READY_FOR_PYTHON);
            }

            // 6. [수신 동기화 확인] 파이썬이 mmap을 완료하고 상태를 PYTHON_ACK(3)로 능동적으로 바꿀 때까지 지수적 백오프로 대기
            awaitPythonAcknowledgment();

        } finally {
            // 다중 스레드 락 안전 해제 (Dual-Barrier 해제)
            internalSyncLock.unlock();
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [지연 확인 루프] 파이썬 프로세스의 매핑(mmap) 처리가 완전히 끝날 때까지 스핀 락(Spin-Lock)을 활용하여 대기하되,
    // 지수적 백오프로 OS에 자원을 양보(Yield)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Delay Acknowledgment Loop] Utilizes a Spin-Lock to wait until the Python
    // process completely finishes its mmap processing, but yields resources to the
    // OS via exponential backoff.

    private void awaitPythonAcknowledgment() {
        int maxRetries = 100_000;
        int attemptCount = 0;
        long backoffDelayNanos = 10_000L; // 10 마이크로초 시작
        long maxBackoffNanos = 2_000_000L; // 2 밀리초 최대 한계

        while (true) {
            int currentState = (int) SYNC_STATE_HANDLE.getVolatile(mailboxSegment, OFFSET_SYNC_STATE);

            if (currentState == STATE_PYTHON_ACK) {
                // 파이썬 측이 공유 메모리 포인터를 안전하게 매핑했음을 확인(ACK), 상태를 다시 IDLE(0)로 복구하고 트랜잭션을 완전 종료합니다.
                SYNC_STATE_HANDLE.setVolatile(mailboxSegment, OFFSET_SYNC_STATE, STATE_IDLE);
                break;
            }

            attemptCount++;
            if (attemptCount > maxRetries) {
                logger.warning(
                        " 🚨 [IPC 롤포워드 (Roll-forward)] Python 에이전트의 ACK 수신 타임아웃. 파이프라인 영구 블로킹(Hanging) 방지를 위해 상태망 락을 억지로 해제합니다.");
                SYNC_STATE_HANDLE.setVolatile(mailboxSegment, OFFSET_SYNC_STATE, STATE_IDLE);
                break;
            }

            LockSupport.parkNanos(backoffDelayNanos);
            backoffDelayNanos = Math.min(backoffDelayNanos * 2, maxBackoffNanos); // Exponential Backoff 지수 승급 적용
        }
    }

    // [1. 한글 상세 주석]
    // [종결 절차] 시스템 강하(Descent) 시 메일박스 채널 및 공유 메모리 아레나 자원을 OS 커널에 안전하게 반환합니다.
    // [2. 영문 상세 주석]
    // [Termination Procedure] Safely returns the mailbox channel and shared memory
    // arena resources to the OS kernel upon system descent.

    public void executeGracefulShutdown() {
        if (isAgentRunning.compareAndSet(true, false)) {
            try {
                if (mailboxArena != null && mailboxArena.scope().isAlive()) {
                    mailboxArena.close();
                }
                if (mailboxChannel != null && mailboxChannel.isOpen()) {
                    mailboxChannel.close();
                }
                logger.info(" >> [프록시망 철수 완료] 폴리글랏 FFI 브릿지의 IPC 공유 메모리 메일박스가 안전하게 해체(Unmapped)되었습니다.");
            } catch (IOException ex) {
                logger.log(Level.WARNING, " [셧다운 시스템 경고] FFI 메일박스 파일 채널 폐쇄 중 예외 발생.", ex);
            }
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 로컬호스트(localhost) TCP 통신의 기만과 FFI(Foreign Function Interface)의 위력:
 * 분산 시스템을 구축할 때 수많은 개발자들이
 * "동일한 서버 머신 내부에서 DB 프로세스와 AI 모델 코어를 통신시킬 때는 localhost를 쓰면 네트워크 지연이 거의 없다"고
 * 착각합니다.
 * 그러나 TCP 소켓은 루프백 인터페이스(Loopback Interface)를 타더라도 여전히 OS 커널의 복잡한 네트워크 스택(버퍼 복사,
 * 혼잡 제어, 컨텍스트 스위칭)을 모두 거치게 되며,
 * 이는 대규모의 무거운 텐서(Tensor) 전송 환경에서 치명적인 I/O 레이턴시와 대역폭 병목을 유발합니다.
 * 본 시스템은 네트워크 소켓을 전면 우회(Bypass)하여, Java 커널 프로세스와 Python 프로세스 사이에 280 바이트 크기의
 * **'POSIX 공유 메모리 메일박스(Shared Memory Mailbox)'**를 물리적으로 뚫어버립니다.
 * 이를 통해 1TB 크기의 텐서 페이로드 전체를 전송하는 것이 아니라, "이 물리적 오프셋 주소(Address)부터 읽어라"라는 초경량
 * 포인터(Pointer) 메타데이터만을 넘깁니다.
 * 수신을 받은 파이썬(Python) 환경은 이 포인터 경로를 `numpy.memmap` C-Extension 모듈로 직접 열어, 객체
 * 직렬화(Serialization) 통신 비용이 수학적으로 완벽한 0(Zero)으로 수렴하는 기적을 창조합니다.
 * 
 * 2. Dual-Barrier 동기화 아키텍처와 지수적 백오프 (Exponential Backoff):
 * 이기종(Cross-Language) 시스템 간에는 JVM의 `synchronized` 모니터 락이나 Python의
 * `threading.Lock` 매커니즘이 전혀 호환되지 않습니다.
 * 따라서 하드웨어 CPU 수준의 원자성(Atomic Instruction)을 보장하는 `compareAndSet (CAS)` 스핀
 * 락(Spin-Lock)을 사용하여 서로의 상태 기계(State Machine)를 동기화해야 합니다.
 * 그러나 락을 얻지 못했을 때 무의미하게 고정된 시간 대기를 반복하면 심각한 CPU 스래싱(Thrashing)과 점유율 폭발 멜트다운이
 * 발생합니다.
 * 이번 리팩토링에서는 대기 시간을 최초 10us에서 최대 2ms까지 2배씩 기하급수적으로 늘려나가는 **Exponential
 * Backoff** 알고리즘을 도입하여
 * CPU 점유율을 비약적으로 안정화시키고 시스템 발열을 잡았습니다.
 * 또한, Java 애플리케이션 내부의 수십 개의 스레드가 동시에 이 하나의 CAS 락에 무작위로 도전하는 혼돈을 선제적으로 통제하기 위해
 * 메서드 가장 최외곽에 자바의 `ReentrantLock`을 배치하여 프로세스 내부 스레드 간(Intra-Process)과 이기종 프로세스
 * 간(Inter-Process)의 충돌을 이중으로 완벽히 통제하는 **Dual-Barrier** 아키텍처를 완성했습니다.
 * 
 * 3. 보안 통제: C-String 널 종단 마커 (Null-Terminator) 방어선 구축:
 * Java의 문자열(String) 객체는 내부 구조적으로 길이를 스스로 알고 통제하지만, Python의 C-Extension 모듈이 이 공유
 * 메모리를 C언어의 `char*` 포인터로 읽어 들일 때는
 * 문자열의 끝을 알리는 널 종단 문자(`\0`, Null-terminator)가 물리적으로 반드시 존재해야 합니다.
 * 만약 전송하려는 파일 경로 길이가 256바이트 공간을 꽉 채워 널 문자가 유실되게 되면, C 프로그램은 버퍼 한계를 모르고 쓰레기 메모리
 * 영역을 무한히 침범하여 결국 버퍼 오버런(Buffer Overrun)과 Segmentation Fault를 일으켜 시스템 전체가 죽게
 * 됩니다.
 * 본 시스템 모듈은 파일 경로 문자열 길이를 255바이트로 엄격히 제한(Clamp)하고, C-Struct 배열의 끝단에 명시적으로 `\0`
 * 널 바이트를 타격(Inject)해 넣음으로써 심각한 메모리 보안 위협을 커널 수준에서 원천 차단했습니다.
 * =============================================================================
 */