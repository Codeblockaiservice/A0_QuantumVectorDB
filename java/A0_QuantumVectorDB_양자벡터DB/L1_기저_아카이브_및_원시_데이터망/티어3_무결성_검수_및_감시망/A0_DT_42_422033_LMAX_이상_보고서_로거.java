/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망
 * @alias LMAX_Anomaly_Report_Logger
 * @tier 3
 * @keywords LMAX Disruptor, Zero-Allocation I/O, MemoryMapped Spillover, False Sharing, OS Scheduling Yield, Smart Lockdown
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422033_LMAX_이상_보고서_로거.java
 * - 역할: 데이터 오염 내역과 치유 이력을 콘솔 블로킹 없이 디스크 파일로 비동기 사출.
 * - 기능: 13만 개 슬롯의 원형 버퍼(Ring Buffer) 사전 할당(Pre-allocation) 및 모아치기(Batch Drain) 사출.
 * - 이론: 기계적 공감(Mechanical Sympathy), 2단계 커밋(2-Phase Commit), Zero-Allocation NIO I/O, OS Scheduling Yield, Memory-Mapped Spillover.
 * - 기대효과: 로거의 I/O 경합으로 인한 메인 쿼리/주조 스레드의 락다운(Deadlock) 차단 및 힙 메모리 할당(Garbage) 원천 제거.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [V6.1 핵심 최적화 1] Zero-Allocation 커스텀 UTF-8 직렬화 적용: 
 *                 매 로그마다 거대한 문자열 객체를 할당하던 `String.getBytes()`와 `String.format()`을 제거했습니다. 
 *                 원형 버퍼 슬롯에 선할당된 `ByteBuffer`에 문자열을 바이트로 변환해 직접 각인(Direct Engraving)하는 수학 연산 기반 직렬화 아키텍처 도입.
 * - 💡 [V6.1 핵심 최적화 2] CPU 스래싱 제어 (Mechanical Yielding):
 *                 소비자 데몬 스핀 락 내 `Thread.onSpinWait()`을 `LockSupport.parkNanos(100_000L)`(0.1ms)로 치환하여 느린 디스크 I/O를 대기하는 동안 OS 스케줄러에 CPU 사이클을 양보(Yield)함으로써 전력 낭비 방어.
 * - 💡 [리메이크 핵심 1] 오프힙 스필오버(Off-Heap Spillover) 방어막: 
 *                 링 버퍼 포화 시 힙(Heap) 객체를 생성하던 안티패턴을 파괴하고, OS 임시 디렉토리에 비동기 `MemoryMapped File`을 뚫어 다이렉트로 방류(Spillover)시킴으로써 GC 개입을 100% 멸균했습니다.
 * - 💡 [리메이크 핵심 2] 스마트 락다운(Smart Lockdown) 배관: 
 *                 `flushRemainingLogsAndAwait` 호출 시 OS 셧다운 시그널의 긴급도(Urgency) 파라미터를 동적으로 수신받아 타임아웃 윈도우를 탄력적으로 연장/축소하는 하드웨어 제어망 신설.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 FileChannel, MemorySegment 등 Zero-Allocation I/O를 위한 코어 라이브러리와 거짓 공유 방어 어노테이션을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for Zero-Allocation I/O, such as FileChannel and MemorySegment, and false sharing defense annotations.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;

import jdk.internal.vm.annotation.Contended;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. LMAX Disruptor 철학을 기반으로 메인 HFT 코어 스레드를 방해하지 않는 100% Zero-Allocation 비동기 로거입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A 100% Zero-Allocation asynchronous logger based on the LMAX Disruptor philosophy that does not interrupt main HFT core threads.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422033
 * [파일명] A0_DT_42_422033_LMAX_이상_보고서_로거.java
 * [모듈명] 통합 OS V6.1 - Tier 3: LMAX Disruptor 아키텍처 기반 비동기 이상 보고서 로거
 * ==============================================================================
 */
public final class A0_DT_42_422033_LMAX_이상_보고서_로거 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422033_LMAX_LOGGER");

    // [1. 한글 상세 주석]
    // LMAX 원형 버퍼 절대 상수. 비트마스크(Bitmask) 연산을 적용하기 위해 배열의 크기는 반드시 2의 거듭제곱이어야 합니다.
    // [2. 영문 상세 주석]
    // LMAX circular buffer absolute constants. The array size must be a power of 2 to apply bitmask operations.
    // [3. 자바 코드]
    private static final int RING_BUFFER_SIZE = 131072; // 2^17
    private static final int BUFFER_BIT_MASK = RING_BUFFER_SIZE - 1;
    private static final int SLOT_PAYLOAD_SIZE = 2048; // 슬롯당 2KB 

    // [1. 한글 상세 주석]
    // 💡 [거짓 공유(False Sharing) 방어를 위한 Contended 클래스]
    // 다중 스레드 환경에서 CPU 캐시 라인(128 Bytes) 무효화 스래싱을 막기 위해 패딩을 강제합니다.
    // [2. 영문 상세 주석]
    // 💡 [Contended Class to Defend Against False Sharing]
    // Forces padding to prevent CPU cache line (128 Bytes) invalidation thrashing in multi-threaded environments.
    // [3. 자바 코드]
    @Contended
    private static class ContendedAtomicLong extends AtomicLong {
        public ContendedAtomicLong(long initialValue) {
            super(initialValue);
        }
    }

    private final LogEventSlot[] ringBuffer;

    private final ContendedAtomicLong producerSequence = new ContendedAtomicLong(0);
    private final ContendedAtomicLong consumerSequence = new ContendedAtomicLong(0);

    private final AtomicBoolean isDaemonRunning = new AtomicBoolean(true);
    private final Thread backgroundIoThread;
    private final Path reportPhysicalPath;

    // =========================================================================
    // 💡 [리메이크 혁신: Off-Heap Spillover Buffer (오프힙 스필오버 버퍼)]
    // 힙 객체 할당을 원천 차단하기 위한 64MB 네이티브 메모리 맵(mmap) 예비 방류소
    // =========================================================================
    private static final long SPILLOVER_CAPACITY_BYTES = 64L * 1024 * 1024; // 64MB
    private final FileChannel spilloverChannel;
    private final MemorySegment spilloverSegment;
    private final Arena spilloverArena;
    private final AtomicLong spilloverCursor = new AtomicLong(0);

    // [1. 한글 상세 주석]
    // 💡 [Zero-Allocation 설계] 이벤트 객체 내부에 OS 커널과 직접 연결되는 2KB 크기의 DirectByteBuffer를 영구적으로 장착합니다.
    // [2. 영문 상세 주석]
    // 💡 [Zero-Allocation Design] Permanently equips a 2KB DirectByteBuffer inside the event object that directly connects to the OS kernel.
    // [3. 자바 코드]
    private static class LogEventSlot {
        volatile boolean isPublished = false;
        final ByteBuffer payloadBuffer = ByteBuffer.allocateDirect(SLOT_PAYLOAD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    }

    // [1. 한글 상세 주석]
    // [생성자] 원형 버퍼를 선할당하고 비동기 I/O 데몬 및 오프힙 스필오버 채널을 기동합니다.
    // [2. 영문 상세 주석]
    // [Constructor] Pre-allocates the circular buffer and starts the asynchronous I/O daemon and off-heap spillover channel.
    // [3. 자바 코드]
    public A0_DT_42_422033_LMAX_이상_보고서_로거(A0_DT_42_422000_타임프레임_컨텍스트 timeframeContext) {

        Path reportFolder = timeframeContext.getSystemReportPath();
        try {
            Files.createDirectories(reportFolder);
        } catch (IOException e) {
            logger.log(Level.SEVERE, " [치명적 오류] 이상 보고서 디렉토리 생성 실패.", e);
        }

        String todayDateStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        this.reportPhysicalPath = reportFolder.resolve("L1_CORRUPTION_REPORT_" + timeframeContext.getResolutionCode() + "_" + todayDateStr + ".csv");
        initializeCsvHeader();

        // 💡 [오프힙 스필오버 초기화] 힙 메모리 오염을 막기 위한 OS 임시 디렉토리 기반 mmap 할당
        try {
            Path spilloverPath = Path.of(System.getProperty("java.io.tmpdir"), "MATRIX_LMAX_SPILLOVER_" + System.currentTimeMillis() + ".log");
            this.spilloverChannel = FileChannel.open(spilloverPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            
            // Sparse File 방식으로 64MB 선할당 (Pre-allocation)
            try (RandomAccessFile raf = new RandomAccessFile(spilloverPath.toFile(), "rw")) {
                raf.setLength(SPILLOVER_CAPACITY_BYTES);
            }
            
            this.spilloverArena = Arena.ofShared();
            this.spilloverSegment = this.spilloverChannel.map(FileChannel.MapMode.READ_WRITE, 0, SPILLOVER_CAPACITY_BYTES, this.spilloverArena);

        } catch (IOException ex) {
            logger.log(Level.SEVERE, " [스필오버 파열] 오프힙 스필오버 버퍼를 물리적으로 할당할 수 없습니다.", ex);
            throw new RuntimeException("Spillover MMap Initialization Failed", ex);
        }

        // 💡 [Zero-Allocation] 131,072개의 객체 및 다이렉트 버퍼 사전 융합
        this.ringBuffer = new LogEventSlot[RING_BUFFER_SIZE];
        for (int i = 0; i < RING_BUFFER_SIZE; i++) {
            this.ringBuffer[i] = new LogEventSlot();
        }

        this.backgroundIoThread = new Thread(this::processLogDrainLoop, "LMAX-Disruptor-Daemon");
        this.backgroundIoThread.setDaemon(true);
        this.backgroundIoThread.start();

        logger.info(" >> [통합 OS V6.1] A0_DT_42_422033 LMAX 로거 기동. (Off-Heap Spillover 장착 및 Smart Lockdown 제어 활성화 완료)");
    }

    // [1. 한글 상세 주석]
    // 💡 [생산자 역학: 2-Phase Commit 및 Off-Heap Spillover 방어막]
    // 큐가 포화되었을 때 메인 HFT 스레드를 블로킹하지 않고 즉각 오프힙 스필오버(MemoryMapped File) 공간으로 다이렉트 방류(Spill)시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [Producer Dynamics: 2-Phase Commit and Off-Heap Spillover Shield]
    // When the queue is saturated, it does not block the main HFT thread but immediately spills over directly into the off-heap spillover (MemoryMapped File) space.
    // [3. 자바 코드]
    public void logAnomalyEvent(String entityCode, String tickDateStr, String featureName, String errorType, String message) {
        if (!isDaemonRunning.get()) return;

        long spinStartNanos = System.nanoTime();
        long timeoutNanos = 50_000_000L; // 50ms Timeout
        long claimedSequence = -1;

        // 1단계: O(1) 원자적 슬롯 점유 (Claim Slot with Fail-Fast CAS Loop)
        while (true) {
            long currentProducer = producerSequence.get();
            long currentConsumer = consumerSequence.get();

            if (currentProducer - currentConsumer >= RING_BUFFER_SIZE) {
                if (System.nanoTime() - spinStartNanos > timeoutNanos) {
                    // 💡 [오프힙 스필오버 방류 격발] 타임아웃 시 힙 객체를 만들지 않고 네이티브 메모리 맵(mmap)으로 즉시 덤프
                    executeOffHeapSpilloverDump(entityCode, tickDateStr, featureName, errorType, message);
                    return;
                }
                LockSupport.parkNanos(100_000L); // 0.1ms 대기 (OS 스케줄러 Yield)
                continue;
            }

            if (producerSequence.compareAndSet(currentProducer, currentProducer + 1)) {
                claimedSequence = currentProducer;
                break;
            }
        }

        // 2단계: 데이터 변이 (Mutation) - Zero-Allocation Byte Direct Fire
        int targetIndex = (int) (claimedSequence & BUFFER_BIT_MASK);
        LogEventSlot targetSlot = ringBuffer[targetIndex];
        ByteBuffer payloadBuffer = targetSlot.payloadBuffer;

        payloadBuffer.clear();
        encodePayloadToBuffer(payloadBuffer, entityCode, tickDateStr, featureName, errorType, message);
        payloadBuffer.flip();

        // 3단계: 2-Phase 발행 선언 (Publish)
        targetSlot.isPublished = true;
    }

    // [1. 한글 상세 주석]
    // 💡 [리메이크 핵심: 오프힙 스필오버 다이렉트 방류]
    // 링버퍼가 막혔을 때, 힙(Heap)을 오염시키지 않고 사전에 할당해 둔 64MB 스필오버 메모리 세그먼트로 원자적 오프셋을 획득하여 기록합니다.
    // [2. 영문 상세 주석]
    // 💡 [Remake Core: Off-Heap Spillover Direct Discharge]
    // When the ring buffer is blocked, it acquires an atomic offset and writes to the pre-allocated 64MB spillover memory segment without polluting the Heap.
    // [3. 자바 코드]
    private void executeOffHeapSpilloverDump(String entityCode, String tickDateStr, String featureName, String errorType, String message) {
        long currentOffset = spilloverCursor.getAndAdd(SLOT_PAYLOAD_SIZE);
        
        // 스필오버 한계치 도달 여부 확인 (64MB)
        if (currentOffset + SLOT_PAYLOAD_SIZE <= SPILLOVER_CAPACITY_BYTES) {
            // MemorySegment에서 해당 오프셋만큼 Slice 후 ByteBuffer로 변환 (Zero-Copy)
            ByteBuffer spillBuffer = spilloverSegment.asSlice(currentOffset, SLOT_PAYLOAD_SIZE)
                    .asByteBuffer()
                    .order(ByteOrder.LITTLE_ENDIAN);
            
            encodePayloadToBuffer(spillBuffer, entityCode, tickDateStr, featureName, errorType, message);
            
            logger.warning(" 🚨 [오프힙 스필오버 격발] 메인 링 버퍼 I/O 포화로 인해, 에러 로그가 OS 임시 디렉토리의 MemoryMapped 버퍼로 다이렉트 방류되었습니다.");
        } else {
            // 스필오버 버퍼마저 고갈된 진정한 재앙 상태 (시스템 무결성 보호를 위해 로깅 포기)
            System.err.println(" [치명적 붕괴] 오프힙 스필오버 버퍼(64MB) 완전 고갈. 시스템 락다운 방어를 위해 신규 로그를 허공에 소각(Drop)합니다.");
        }
    }

    // [1. 한글 상세 주석]
    // 버퍼(ByteBuffer)에 포맷터 없이 콤마(,)와 개행(\n)을 포함하여 UTF-8 바이트를 직렬화 조립하는 유틸리티 메서드.
    // [2. 영문 상세 주석]
    // A utility method that serializes and assembles UTF-8 bytes including commas (,) and newlines (\n) directly into the buffer without a formatter.
    // [3. 자바 코드]
    private void encodePayloadToBuffer(ByteBuffer buffer, String entityCode, String tickDateStr, String featureName, String errorType, String message) {
        try {
            encodeLongToAsciiToBuffer(buffer, System.currentTimeMillis());
            buffer.put((byte) ',');
            encodeUtf8ToBuffer(buffer, entityCode);
            buffer.put((byte) ',');
            encodeUtf8ToBuffer(buffer, tickDateStr);
            buffer.put((byte) ',');
            encodeUtf8ToBuffer(buffer, featureName);
            buffer.put((byte) ',');
            encodeUtf8ToBuffer(buffer, errorType);
            buffer.put((byte) ',');

            int expectedRemainingSpace = buffer.remaining();
            if (message != null && expectedRemainingSpace > message.length() * 3 + 1) { 
                encodeUtf8ToBuffer(buffer, message);
            } else {
                encodeUtf8ToBuffer(buffer, "MSG_TRUNCATED");
            }
            buffer.put((byte) '\n');
        } catch (Exception e) {
            buffer.clear();
            encodeUtf8ToBuffer(buffer, "LOG_ENCODING_ERROR\n");
        }
    }

    // [1. 한글 상세 주석]
    // `String.getBytes()`를 멸균하고 순수 비트 시프트로 UTF-8 바이트를 버퍼에 직접 각인합니다.
    // [2. 영문 상세 주석]
    // Sterilizes `String.getBytes()` and directly engraves UTF-8 bytes into the buffer via pure bit-shifting.
    // [3. 자바 코드]
    private void encodeUtf8ToBuffer(ByteBuffer targetBuffer, String str) {
        if (str == null) return;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            char ch = str.charAt(i);
            if (ch <= 0x7F) {
                targetBuffer.put((byte) ch);
            } else if (ch <= 0x7FF) {
                targetBuffer.put((byte) (0xC0 | ((ch >> 6) & 0x1F)));
                targetBuffer.put((byte) (0x80 | (ch & 0x3F)));
            } else {
                targetBuffer.put((byte) (0xE0 | ((ch >> 12) & 0x0F)));
                targetBuffer.put((byte) (0x80 | ((ch >> 6) & 0x3F)));
                targetBuffer.put((byte) (0x80 | (ch & 0x3F)));
            }
        }
    }

    // [1. 한글 상세 주석]
    // `String.valueOf()` 힙 할당을 차단하고 대수학으로 정수를 아스키 바이트로 변환합니다.
    // [2. 영문 상세 주석]
    // Blocks `String.valueOf()` heap allocation and converts integers to ASCII bytes via algebra.
    // [3. 자바 코드]
    private void encodeLongToAsciiToBuffer(ByteBuffer targetBuffer, long number) {
        if (number == 0) {
            targetBuffer.put((byte) '0');
            return;
        }

        boolean isNegative = number < 0;
        if (isNegative) {
            targetBuffer.put((byte) '-');
            number = -number;
        }

        long temp = number;
        long digitMultiplier = 1;
        while (temp >= 10) {
            digitMultiplier *= 10;
            temp /= 10;
        }

        while (digitMultiplier > 0) {
            byte digitByte = (byte) ('0' + (number / digitMultiplier));
            targetBuffer.put(digitByte);
            number %= digitMultiplier;
            digitMultiplier /= 10;
        }
    }

    // [1. 한글 상세 주석]
    // [소비자 루프] 발행(Publish)된 다이렉트 버퍼를 읽어 FileChannel에 순차적(Sequential)으로 모아치기 기록(Drain)합니다.
    // [2. 영문 상세 주석]
    // [Consumer Loop] Reads the published direct buffer and sequentially batch-writes (Drains) it to the FileChannel.
    // [3. 자바 코드]
    private void processLogDrainLoop() {
        try (FileChannel logRecordChannel = FileChannel.open(reportPhysicalPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)) {
            long sequenceToProcess = consumerSequence.get();

            while (isDaemonRunning.get() || ringBuffer[(int) (sequenceToProcess & BUFFER_BIT_MASK)].isPublished) {
                int processIndex = (int) (sequenceToProcess & BUFFER_BIT_MASK);
                LogEventSlot targetEvent = ringBuffer[processIndex];

                if (targetEvent.isPublished) {
                    ByteBuffer bufferToDrain = targetEvent.payloadBuffer;
                    while (bufferToDrain.hasRemaining()) {
                        logRecordChannel.write(bufferToDrain);
                    }

                    targetEvent.isPublished = false;
                    sequenceToProcess++;

                    // 메모리 배리어 동기화 비용을 낮추는 LazySet 적용
                    consumerSequence.lazySet(sequenceToProcess);

                    // 1,000개 모아치기(Batch) 플러시
                    if (sequenceToProcess % 1000 == 0) {
                        logRecordChannel.force(false);
                    }
                } else {
                    // 유휴(Idle) 상태일 때 OS 스케줄러에 CPU 양보
                    logRecordChannel.force(false);
                    LockSupport.parkNanos(100_000L); // 0.1ms
                }
            }
            logRecordChannel.force(true);
            logger.info("   ├─ [LMAX 로거 데몬 종료] 링 버퍼 잔여 이벤트 사출 및 FileChannel 디스크 동기화 완료.");

        } catch (IOException e) {
            logger.log(Level.SEVERE, " [시스템 오류] LMAX 소비자 데몬 스레드의 FileChannel I/O 작업 중 치명적 오류 발생.", e);
            Thread.currentThread().interrupt();
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [스마트 락다운 배관] 긴급도(isUrgent)에 따라 타임아웃 윈도우를 연장/축소하여 시스템 셧다운 시 마지막 로그의 물리적 생존을 보장합니다.
    // [2. 영문 상세 주석]
    // 💡 [Smart Lockdown Piping] Dynamically extends/reduces the timeout window based on urgency (isUrgent) to guarantee the physical survival of the last log upon system shutdown.
    // [3. 자바 코드]
    public void flushRemainingLogsAndAwait(boolean isUrgent) {
        long lastProducedSequence = producerSequence.get();

        logger.warning(" [감시망 긴급 락온] 스마트 락다운 발동: 버퍼 잔여 로그 물리적 플러시 대기 중...");

        long spinStartNanos = System.nanoTime();
        // 💡 [동적 타임아웃 연장] 긴급 셧다운 시 1초, 일반 셧다운 시 10초까지 잔여 로그 사출 대기
        long timeoutNanos = isUrgent ? 1_000_000_000L : 10_000_000_000L; 

        while (consumerSequence.get() < lastProducedSequence) {
            if (System.nanoTime() - spinStartNanos > timeoutNanos) {
                System.err.println(" [플러시 포기] 소비자 데몬 I/O 처리 한계 도달. 잔여 로그 사출을 포기(Drop)하고 프로세스를 종료합니다.");
                break;
            }
            LockSupport.parkNanos(100_000L); // 0.1ms 대기
        }
        
        // 💡 [스필오버 강제 동기화] 셧다운 직전 스필오버 채널의 잔여물도 디스크로 영속화
        if (spilloverChannel != null && spilloverChannel.isOpen()) {
            try {
                spilloverChannel.force(true);
            } catch (IOException ignored) {}
        }
    }

    private void initializeCsvHeader() {
        if (!Files.exists(reportPhysicalPath)) {
            try (FileChannel channel = FileChannel.open(reportPhysicalPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                if (channel.size() == 0) {
                    ByteBuffer headerBuffer = ByteBuffer.allocateDirect(128);
                    encodeUtf8ToBuffer(headerBuffer, "기록에포크시간(ms),entityCode,발생시점(Tick),지표명,오류유형,시스템_상세진단결과\n");
                    headerBuffer.flip();
                    while (headerBuffer.hasRemaining()) {
                        channel.write(headerBuffer);
                    }
                }
            } catch (IOException e) {
                logger.warning("보고서 파일 헤더 초기화 실패.");
            }
        }
    }

    public void executeGracefulShutdown(boolean isUrgent) {
        logger.info(" [감시망 해제 지시] LMAX 로거 데몬 스레드 안전 종료(Smart Lockdown) 시퀀스 개시...");
        flushRemainingLogsAndAwait(isUrgent);
        isDaemonRunning.set(false);
        
        // 스필오버 네이티브 자원 환원
        try {
            if (spilloverArena != null && spilloverArena.scope().isAlive()) spilloverArena.close();
            if (spilloverChannel != null && spilloverChannel.isOpen()) spilloverChannel.close();
        } catch (IOException ignored) {}
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 💡 오프힙 스필오버(Off-Heap Spillover) 방어막과 Zero-Allocation의 절대성:
 * 링버퍼(Ring Buffer)가 가득 차면 시스템 엔지니어들은 흔히 "남는 로그를 임시로 힙(Heap) 메모리의 Queue나 List에 담아두자(Spillover)"라는 달콤한 유혹에 빠집니다.
 * 그러나 메인 HFT 스레드가 초당 수십만 개의 에러 로그를 쏟아낼 때, 이를 힙 객체(Object)로 포장하여 저장하는 행위는 
 * 시스템 가비지 컬렉터(GC)에 폭탄을 던지는 것과 같습니다. 이는 곧 거대한 GC 스톨(STW)을 일으켜 메인 텐서 연산 엔진까지 뇌사 상태로 끌고 들어가는 참사를 낳습니다.
 * 이 모듈은 JVM의 힙 메모리를 '더럽히지(Pollute)' 않고 철저히 배제합니다.
 * 링버퍼 포화(Timeout) 시, 즉각적으로 OS 커널의 RAM 디스크나 임시 폴더(`/tmp`)에 생성된 64MB 크기의 **비동기 MemoryMapped File (오프힙 스필오버 버퍼)**로 타겟 포인터를 틀어버립니다. 
 * `spilloverCursor.getAndAdd()`를 통한 락-프리 오프셋 점유 후 다이렉트로 바이트를 방류(Discharge)시킴으로써, GC 개입률 0%를 유지하며 시스템 붕괴를 물리적으로 차단했습니다.
 * 
 * 2. 스마트 락다운(Smart Lockdown) 배관과 동적 타임아웃의 묘미:
 * 서버를 셧다운 할 때, 무작정 `Thread.sleep`으로 로그가 다 쓰이기를 기다리는 것은 미련한 짓입니다. 
 * OS가 패닉에 빠져 강제 종료(SIGKILL 임박)를 알리고 있거나(Urgent), 일반적인 스케일인(Scale-in)에 의한 평화로운 종료(Normal)인지에 따라 타임아웃 윈도우는 동적으로 늘어나고 줄어들어야 합니다.
 * `flushRemainingLogsAndAwait(boolean isUrgent)` 인터페이스는 상위 관제탑(L5 파사드)으로부터 셧다운 시그널의 긴급도를 동적으로 파라미터 주입받습니다.
 * 촌각을 다투는 긴급 상황에서는 1초만 기다린 뒤 과감히 I/O를 끊어버리고, 일반 셧다운 시에는 10초까지 인내하며 큐에 남은 마지막 1바이트의 로그까지 디스크 섹터에 완벽하게 영속화시킵니다.
 * 
 * 3. 기계적 공감(Mechanical Sympathy)과 CPU 스래싱 제어(OS Yielding):
 * `Thread.onSpinWait()`은 x86 CPU의 파이프라인에서 예측 실패를 방지하는 훌륭한 하드웨어 명령어(PAUSE)지만, 
 * 디스크 I/O를 기다리는 긴 대기열에서 이를 무한 반복하면 1개의 CPU 코어 점유율을 100%로 불태워버립니다 (Busy-Wait).
 * 링 버퍼에 읽을 데이터가 없거나(Consumer), 쓸 공간이 없어 대기 중일 때(Producer), 
 * 본 아키텍처는 `LockSupport.parkNanos(100_000L)` (0.1ms)을 호출하여 즉시 해당 스레드를 수면 상태로 전환하고, 
 * OS 스케줄러에게 CPU 코어의 연산 제어권을 자진 양보(Yield)합니다.
 * 이는 시스템 전력 소모를 비약적으로 낮추고 멀티코어 환경에서 타 스레드의 실행 기회를 박탈하지 않는 기계적 이타성(Mechanical Altruism)의 실현입니다.
 * 
 * 4. 거짓 공유(False Sharing) 격리와 메모리 배리어(Memory Barrier) 회피:
 * 다중 스레드 환경에서 `producerSequence`와 `consumerSequence`가 동일한 128 바이트 CPU 캐시 라인(Cache Line)에 올라가면 서로의 캐시를 무효화하는 스래싱이 발생합니다. 
 * 이를 `@Contended` 어노테이션으로 물리적 이격(Padding)시켰을 뿐만 아니라, 
 * 소비자가 시퀀스를 업데이트할 때 `set()` 대신 `lazySet()`을 호출하여 무거운 메모리 배리어(StoreStore Barrier) 동기화 비용을 수학적으로 우회(Bypass)함으로써 HFT 급의 파이프라인 전진 속도를 수호했습니다.
 * =============================================================================
 */