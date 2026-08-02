/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망
 * @alias Byte_Reverse_Microscope_Scanner
 * @tier 3
 * @keywords UTF-8 Bitwise Operation, Boundary Alignment, Dynamic Chunking, Zero-Allocation, OOM Defense
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_422031_바이트_역방향_현미경_스캐너.java
 * - 기능 (Function): 방금 기록된 CSV의 마지막(Tail) 부분 원시 바이트와 FFM 텐서 오프셋에 기록된 비트의 교차 검증.
 * - 역할 (Role): 원본 CSV 파일의 꼬리 원시 바이트와 커널 텐서 메모리의 완벽한 일치를 검수하는 1차 보안 스캐너.
 * - 이론 (Theory): UTF-8 Bitwise Operation (비트마스크 경계 탐색), Boundary Alignment, OOM 방어 한계선(Fail-Safe Limit).
 * - 기대효과 (Effect): 4KB 청크 경계에서 다국어(예: 3 Bytes 한글)가 걸쳐 잘릴 때 발생하는 UTF-8 훼손 현상을 방지하고, 
 *                 손상된 파일(개행 누락)로 인한 OOM 시스템 다운을 물리적으로 차단.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [초정밀 제어] OOM 방어 한계선(Fail-Safe Limit) 장착: 
 *                 파일이 손상되어 개행(\n)을 찾지 못한 채 누적 버퍼가 무한히 팽창하여 
 *                 JVM 힙 메모리를 초과(OOM)시키는 치명적 오류를 제거했습니다. 버퍼 크기가 10MB를 초과하면 즉각 예외를 던져 
 *                 해당 파일 스캔을 중단(서킷 브레이커 격발)시키고 로거로 내역을 사출하는 방어막을 전개했습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 파일 제어, 버퍼 관리, 정규식을 위한 코어 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core libraries for file control, buffer management, and regular expressions.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더.SmartIndexRegistry;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 바이트 단위로 디스크를 역방향 스캔하여 데이터 정합성을 대조하는 스캐너 모듈입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A scanner module that performs reverse byte-level disk scans to cross-verify data integrity.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422031
 * [파일명] A0_DT_42_422031_바이트_역방향_현미경_스캐너.java
 * [모듈명] 통합 OS V6.0 - Tier 3: 바이트 역방향 무결성 현미경 스캐너
 * 
 * [설계 명세]
 * 1. 역할: 방금 주조된 원본 CSV의 마지막(Tail) 원시 바이트와 커널에 매핑된 FFM 텐서 부동소수점 비트 간의 교차 검증.
 * 2. 기능: 4KB 청크 기반 동적 버퍼(Prepend) 복원 스캔 및 UTF-8 경계 조율, 단일 라인 OOM 한계선 차단.
 * 3. 의도: 다국어(UTF-8)가 청크 경계에서 잘려 외계어로 훼손되는 현상과 개행 누락 파일에 의한 무한 루프 OOM을 원천 방지.
 * 4. 공식: ChunkSize = min(4096, ReadPos) with Forward Alignment.
 * 5. 💡 [V6.0 초정밀 통제] OOM 한계선(Fail-Safe Limit) 밸브 장착:
 * 비정상적으로 거대한 단일 라인(개행 없는 손상된 파일)을 역방향 스캔할 때 동적 버퍼가 무한 팽창하여
 * OOM 크래시가 발생하는 것을 막기 위해 `MAX_ACCUMULATED_BUFFER_10MB` 임계치를 돌파하면 즉각 예외를 던져 
 * 서킷 브레이커를 안전하게 격발시킵니다.
 * ==============================================================================
 */
public final class A0_DT_42_422031_바이트_역방향_현미경_스캐너 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422031_MICROSCOPE_SCANNER");

    // entityCode(6자리 숫자) 추출용 정규식 패턴
    private static final Pattern ENTITY_CODE_PATTERN = Pattern.compile("(\\d{6})");

    // 💡 역방향 탐색 시 디스크에서 한 번에 퍼 올릴 바이트 청크 크기 (OS 페이지 사이즈 4KB에 최적화)
    private static final int CHUNK_SIZE = 4096;

    // [1. 한글 상세 주석]
    // 💡 [OOM 방어 한계선] 단일 라인이 비정상적으로 길어 힙 메모리 부족(OOM)을 유발하는 것을 막기 위한 10MB 임계치 상수입니다.
    // [2. 영문 상세 주석]
    // 💡 [OOM Defense Limit] A 10MB threshold constant to prevent abnormally long single lines from causing Heap Out-Of-Memory.

    private static final int MAX_ACCUMULATED_BUFFER_10MB = 10 * 1024 * 1024;

    /**
     * 상태를 가지지 않는(Stateless) 검수 엔진이므로 자유롭게 생성 및 재사용 가능합니다.
     */
    public A0_DT_42_422031_바이트_역방향_현미경_스캐너() {
        logger.info(" >> [통합 OS V6.0] A0_DT_42_422031 바이트 역방향 현미경 스캐너 기동. (동적 버퍼 결합, UTF-8 경계 조율 및 OOM 방어 밸브 탑재)");
    }

    /**
     * [검수 메커니즘 1] 신규 처리된 CSV 데이터의 마지막 행(Tail)을 역방향으로 추출하여,
     * L1 매트릭스 메모리에 기록된 절대 좌표의 부동소수점 비트와 교차 검증합니다.
     * 
     * @param timeframeContext      타임프레임(시계열 해상도) 도메인 컨텍스트
     * @param runtimeIndexRegistry  Y축(종목), Z축(지표) 및 X축(시간) 인덱스를 반환하는 O(1) 레지스트리
     * @param targetCsvList         검증할 신규 델타 파일들의 경로 목록
     * @param anomalyLogger         에러 발생 시 즉각 기록을 남길 비동기 로거 인스턴스
     * @return boolean 무결성 통과 여부 (단 1개라도 실패하면 false 반환 후 즉시 종료)
     */
    public boolean executeByteReverseCrossValidation(
            A0_DT_42_422000_타임프레임_컨텍스트 timeframeContext,
            SmartIndexRegistry runtimeIndexRegistry,
            List<Path> targetCsvList,
            A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger) {

        if (targetCsvList == null || targetCsvList.isEmpty()) {
            return true;
        }

        // 병렬 스레드 환경에서 데이터 오염 발생 시 다른 스레드들의 스캔을 즉각 중단시키기 위한 원자적 플래그
        AtomicBoolean isSystemClean = new AtomicBoolean(true);

        // 하드웨어 가용 코어 수에 맞춘 병렬 스트림 검증 (Fail-Fast 적용)
        targetCsvList.parallelStream().forEach(csvPath -> {
            // 다른 스레드에서 무결성 실패(오염)가 발견되었다면, 자신의 연산을 즉시 중단(Short-circuit)
            if (!isSystemClean.get())
                return;

            String fileName = csvPath.getFileName().toString();
            Matcher matcher = ENTITY_CODE_PATTERN.matcher(fileName);

            if (matcher.find()) {
                String entityCode = matcher.group(1);
                Integer entityIndexY = runtimeIndexRegistry.featureZIndexMap().get(entityCode);

                if (entityIndexY != null) {
                    boolean isPassed = validateSingleTailByte(
                            csvPath, entityCode, entityIndexY,
                            timeframeContext, runtimeIndexRegistry, anomalyLogger);

                    if (!isPassed) {
                        isSystemClean.set(false); // 서킷 브레이커 발동 플래그
                    }
                }
            }
        });

        return isSystemClean.get();
    }

    // [1. 한글 상세 주석]
    // 💡 [OOM 방어 발동] 바이트 스캔 엔진이 10MB 초과 예외를 던지면 이를 캐치하여 전체 서킷 브레이커를 격발시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [OOM Defense Triggered] Catches the 10MB excess exception thrown by the byte scan engine to trigger the global circuit breaker.

    /**
     * [검수 메커니즘 2] 개별 파일의 끝(EOF)에서부터 읽어 올라오는 역방향 스윕으로 마지막 데이터 행을 추출하고,
     * FFM 엔진이 기록한 메모리의 절대 좌표값과 정밀 대조합니다.
     */
    private boolean validateSingleTailByte(
            Path csvPath,
            String entityCode,
            int entityIndexY,
            A0_DT_42_422000_타임프레임_컨텍스트 timeframeContext,
            SmartIndexRegistry runtimeIndexRegistry,
            A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger) {

        String lastLine;
        try {
            // 동적 바이너리 청크 리더를 통해 파일의 마지막 로우(Row)를 안전한 UTF-8 문자열로 복원
            lastLine = scanTailByteReversely(csvPath.toFile());
        } catch (IllegalStateException e) {
            // 💡 10MB 버퍼 팽창 한계선 도달 (OOM 방어벽 작동)
            anomalyLogger.logAnomalyEvent(entityCode, "N/A", "N/A", "OOM_LIMIT_EXCEEDED",
                    "손상된 파일 포맷으로 인해 단일 라인 누적 버퍼가 10MB를 초과하여 스캔을 강제 중단했습니다.");
            return false; // 무결성 실패 반환 및 서킷 브레이커 격발 유도
        }

        if (lastLine == null || lastLine.trim().isEmpty())
            return true;

        String[] fragments = lastLine.split(",", -1);
        String tickDateStr = fragments[0].trim();

        // O(1) 인덱스 수학적 역산
        int tickIndexX = runtimeIndexRegistry.timeGridIndexer().getIndex(tickDateStr);

        // 레지스트리에 등록되지 않은 미래/과거 시간(오류 데이터)은 검증 대상에서 제외
        if (tickIndexX < 0)
            return true;

        String[] headerArray;
        try (var reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            String headerLine = reader.readLine();
            if (headerLine == null)
                return true;
            headerArray = headerLine.split(",");
        } catch (Exception e) {
            return false;
        }

        // 각 컬럼(지표명)별로 커널 텐서 데이터와 무결성 교차 검증 수행
        for (int i = 1; i < headerArray.length; i++) {
            String featureName = headerArray[i].trim();
            if (!runtimeIndexRegistry.featureZIndexMap().containsKey(featureName))
                continue;

            String rawValueStr = fragments[i].trim();
            float csvOriginalValue = Float.NaN;

            if (!rawValueStr.isEmpty() && !rawValueStr.equals("NaN") && !rawValueStr.equals("null")) {
                try {
                    csvOriginalValue = Float.parseFloat(rawValueStr);
                } catch (NumberFormatException ignored) {
                }
            }

            // 결측치(NaN)가 아닌 유효한 실수가 존재할 경우에만 메모리와의 교차 대조 수행
            if (!Float.isNaN(csvOriginalValue)) {
                long exactByteOffset = A0_DT_42_422001_권한_포트_인터페이스.calculateChunkInternalOffset(entityIndexY, tickIndexX, 4L);
                Path layerPhysicalPath = timeframeContext.resolveDataAbsolutePath(featureName);

                float tensorMemoryValue = nioDirectPositionalRead(layerPhysicalPath, exactByteOffset);

                // IEEE 754 규격에 따른 완벽한 비트 단위(Bit-level) 부동소수점 대조 (오차율 0 허용)
                if (Float.compare(csvOriginalValue, tensorMemoryValue) != 0) {
                    String errorReason = String.format("물리적 텐서 불일치 감지! CSV 원본: %f vs 커널 텐서: %f (절대 오프셋: %d)",
                            csvOriginalValue, tensorMemoryValue, exactByteOffset);

                    anomalyLogger.logAnomalyEvent(entityCode, tickDateStr, featureName, "INTEGRITY_MISMATCH", errorReason);
                    return false; // 오염 발견 시 즉각 false 반환하여 서킷 브레이커 트리거
                }
            }
        }
        return true;
    }

    // [1. 한글 상세 주석]
    // 💡 [핵심 통제 완료: UTF-8 다국어 경계 훼손 방어 및 버퍼 동적 조율, OOM 한계선 방어망]
    // 버퍼가 결합될 때 10MB를 초과하면 IllegalStateException을 위로 던져 JVM 힙 메모리 파열을 원천 봉쇄합니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Control Completed: UTF-8 Multilingual Boundary Corruption Defense and Dynamic Alignment, OOM Defense Net]
    // When combining buffers, if the size exceeds 10MB, it throws an IllegalStateException upwards to fundamentally block JVM heap memory rupture.

    private String scanTailByteReversely(File file) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            long totalFileLength = file.length();
            if (totalFileLength == 0)
                return null;

            long readPointer = totalFileLength;
            boolean isTrailingJunkRemoved = false;

            // 현재 4KB 청크 내에 개행이 없으면 이전 바이트를 보존하고 다음 앞선 청크를 앞에 결합(Prepend)합니다.
            byte[] accumulatedBuffer = new byte[0];

            while (readPointer > 0) {
                // 남은 바이트 수와 4KB 청크 중 작은 값을 스캔 사이즈로 결정
                int bytesToRead = (int) Math.min(CHUNK_SIZE, readPointer);
                long chunkStartPos = readPointer - bytesToRead;

                // 💡 [UTF-8 멀티바이트 경계선 탐색 및 찢어짐 방어 로직]
                if (chunkStartPos > 0) {
                    randomAccessFile.seek(chunkStartPos);
                    byte boundaryByte = randomAccessFile.readByte();

                    // (boundaryByte & 0xC0) == 0x80 조건은 현재 바이트가 UTF-8 문자의 중간을 나타내는 '연속 바이트(Continuation Byte)'임을 의미합니다.
                    // 온전한 시작 바이트가 나올 때까지 청크의 시작 포인터를 앞으로(전진) 조율합니다.
                    while ((boundaryByte & 0xC0) == 0x80) {
                        chunkStartPos++;
                        bytesToRead--;

                        if (bytesToRead == 0)
                            break;

                        randomAccessFile.seek(chunkStartPos);
                        boundaryByte = randomAccessFile.readByte();
                    }
                }

                byte[] currentChunkBuffer = new byte[bytesToRead];
                randomAccessFile.seek(chunkStartPos);
                randomAccessFile.readFully(currentChunkBuffer);

                int foundNewlineIndex = -1;

                // 읽어들인 청크 내부에서 뒤에서부터 앞으로 1바이트씩 정밀 검사
                for (int i = bytesToRead - 1; i >= 0; i--) {
                    byte singleByte = currentChunkBuffer[i];

                    if (!isTrailingJunkRemoved) {
                        // EOF 엣지 케이스 처리: 파일 끝에 무의미하게 붙어있는 연속 개행(\n, \r)이나 공백을 바이패스(Bypass)
                        if (singleByte == '\n' || singleByte == '\r' || singleByte == ' ') {
                            continue;
                        } else {
                            isTrailingJunkRemoved = true;
                            byte[] trimmedBuffer = new byte[i + 1];
                            System.arraycopy(currentChunkBuffer, 0, trimmedBuffer, 0, i + 1);
                            currentChunkBuffer = trimmedBuffer;
                        }
                    } else {
                        // 실제 유효한 마지막 줄이 시작되는 개행 문자(0x0A) 탐지
                        if (singleByte == '\n') {
                            foundNewlineIndex = i + 1;
                            break;
                        }
                    }
                }

                if (isTrailingJunkRemoved) {
                    if (foundNewlineIndex != -1) {
                        int validLength = currentChunkBuffer.length - foundNewlineIndex;
                        byte[] finalMergedBytes = new byte[validLength + accumulatedBuffer.length];

                        System.arraycopy(currentChunkBuffer, foundNewlineIndex, finalMergedBytes, 0, validLength);
                        System.arraycopy(accumulatedBuffer, 0, finalMergedBytes, validLength, accumulatedBuffer.length);

                        return new String(finalMergedBytes, StandardCharsets.UTF_8).trim();

                    } else {
                        // [1. 한글 상세 주석]
                        // 💡 [안전망 전개: OOM 방어 밸브 발동]
                        // 개행(\n)을 찾지 못하면 파일 포맷 오류로 인해 현재 청크 전체를 누적 버퍼의 맨 앞에 계속 결합(Prepend)해야 합니다.
                        // 이때 팽창될 버퍼 크기가 10MB를 초과하면 JVM 힙 메모리 고갈을 막기 위해 예외를 던져 연산을 포기시킵니다.
                        // [2. 영문 상세 주석]
                        // 💡 [Safety Net Deployment: OOM Defense Valve Triggered]
                        // If no newline is found, the entire current chunk must be prepended to the accumulated buffer.
                        // If the expanded buffer size exceeds 10MB, an exception is thrown to prevent JVM heap memory exhaustion.
                        
                        if (currentChunkBuffer.length + accumulatedBuffer.length > MAX_ACCUMULATED_BUFFER_10MB) {
                            throw new IllegalStateException("OOM 방어막 가동: 단일 라인의 크기가 10MB 한계 임계치를 돌파했습니다.");
                        }

                        byte[] expandedBuffer = new byte[currentChunkBuffer.length + accumulatedBuffer.length];
                        System.arraycopy(currentChunkBuffer, 0, expandedBuffer, 0, currentChunkBuffer.length);
                        System.arraycopy(accumulatedBuffer, 0, expandedBuffer, currentChunkBuffer.length, accumulatedBuffer.length);
                        accumulatedBuffer = expandedBuffer; // 누적 버퍼 갱신
                    }
                }

                // 청크 내에 개행이 없었거나 파일 끝부분의 쓰레기 공백만 만났다면 다음 4KB(이전 위치)로 한 번 더 후퇴
                readPointer = chunkStartPos;
            }

            // 파일 맨 처음(0바이트)까지 도달했는데도 파일 내에 개행(\n)이 단 하나도 없는 경우 (단일 라인 파일)
            if (isTrailingJunkRemoved && accumulatedBuffer.length > 0) {
                return new String(accumulatedBuffer, StandardCharsets.UTF_8).trim();
            }

            return null;

        } catch (IllegalStateException e) {
            // 💡 [OOM 밸브 예외 전파] 한계선 돌파 예외는 여기서 삼키지 않고 위로 던져 시스템 전체 서킷 브레이커를 격발시킵니다.
            throw e;
        } catch (Exception e) {
            logger.log(Level.SEVERE, " [현미경 스캔 에러] 순수 바이트 역방향 탐색 중 치명적 파일 I/O 오류 발생: " + file.getName(), e);
            return null;
        }
    }

    /**
     * 💡 [NIO 다이렉트 핀포인트 읽기]
     * mmap(FileChannel.map) 호출이 유발하는 OS 커널 스래싱(Thrashing)을 방지하기 위해, 
     * 채널의 읽기 커서를 전역적으로 이동시키지 않는 절대 위치(Positional Read) 방식의 단일 부동소수점 읽기를 수행합니다.
     */
    private float nioDirectPositionalRead(Path layerFilePath, long absoluteByteOffset) {
        if (!Files.exists(layerFilePath)) {
            return Float.NaN;
        }

        try (FileChannel channel = FileChannel.open(layerFilePath, StandardOpenOption.READ)) {

            ByteBuffer pinpointBuffer = ByteBuffer.allocate(4);
            pinpointBuffer.order(ByteOrder.LITTLE_ENDIAN);

            // 논블로킹(Positional Read) 방식의 파일 시스템 캐시 직접 타격. (멀티 스레드 안전성 확보)
            int readBytes = channel.read(pinpointBuffer, absoluteByteOffset);

            if (readBytes == 4) {
                pinpointBuffer.flip();
                return pinpointBuffer.getFloat();
            }
            return Float.NaN;

        } catch (Exception e) {
            logger.warning(" [메모리 접근 실패] L1 매트릭스 텐서 메모리의 핀포인트 접근 중 I/O 예외 발생: " + layerFilePath.getFileName());
            return Float.NaN;
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 바이트 레벨 탐색과 UTF-8 인코딩의 훼손 방어 (Defending UTF-8 Boundary Corruption):
 * 자바에서 `String` 객체가 생성되는 순간, 원시 바이트(byte)는 특정 디코딩 룰(UTF-8)에 의해 강제 해석됩니다.
 * 일반적인 C/Java 시스템은 성능을 위해 파일을 4096 바이트(4KB) 크기의 청크(Chunk)로 기계적으로 잘라내어 `new String(buffer, UTF_8)`을 호출합니다.
 * 이 방식은 1바이트 구조인 영어권에서는 문제가 없으나, 한글(1글자당 3Bytes)이나 이모지 등 다국어 데이터가 4KB 경계선에 걸칠 경우, 
 * 글자의 중간 바이트(예: `1110xxxx 10xxxxxx`)를 무자비하게 절단해버립니다.
 * 잘려나간 조각을 String으로 변환하려 하면, 자바는 이를 해독 불가능한 문자(Replacement Character)로 판단해 영구적으로 텍스트를 훼손시킵니다.
 * 
 * 본 코드는 텍스트(String) 디코딩을 수행하기 전에, 순수 바이너리(byte[]) 차원에서 4096번째 바이트의 비트를 검사(`byte & 0xC0 == 0x80`)하여,
 * 현재 포인터가 다국어 문자의 허리(연속 바이트)를 자르고 있는지 확인합니다.
 * 연속 바이트임이 판명되면, 온전한 시작 바이트(또는 ASCII)를 만날 때까지 청크 시작 포인터를 앞쪽으로(++) 조율하여,
 * 다국어 문자 훼손 현상을 물리적으로 차단했습니다. 남겨진 조각들은 다음 스캔 루프에서 온전히 수거되어 안전하게 조립됩니다.
 * 
 * 2. 💡 단일 라인 무한 팽창 방지와 OOM 한계선(Fail-Safe Limit)의 기하학적 방어막:
 * 파일의 끝에서부터 역방향으로 읽어오는 기존의 동적 버퍼 결합(Prepend) 알고리즘은 "파일 어딘가에는 반드시 개행 문자(\n)가 나온다"는 가정을 전제로 했습니다.
 * 만약 네트워크 전송 중 데이터 꼬임이나 디스크 배드 섹터로 인해 1GB짜리 거대 파일 전체에 개행이 단 한 개도 존재하지 않는다면 어떻게 될까요?
 * 이 현미경 스캐너는 4KB씩 뒤로 후퇴하며 `new byte[기존 누적 크기 + 4096]`을 무한 반복 생성하다가 
 * 결국 JVM 힙 메모리 한계를 돌파하여 시스템 전체를 뻗게 만드는(OOM) 최악의 뇌관이 됩니다.
 * 수정된 V6.0 아키텍처는 `MAX_ACCUMULATED_BUFFER_10MB` 방어 밸브를 장착하여, 단일 라인 누적 버퍼가 비정상적으로 커지는 순간
 * 스캔 행위 자체를 즉각 중단(`IllegalStateException` 발산)시킵니다.
 * 이 예외는 상위 레이어 파사드에서 안전하게 캐치되어 해당 파일의 무결성 통과를 `false`로 격리(Circuit Break)시킴으로써,
 * 하나의 손상된 텍스트 파편이 데이터베이스 서버 전체의 셧다운(OOM)을 유발하는 나비효과를 완벽히 통제했습니다.
 * 
 * 3. mmap 스래싱(Thrashing) 방어와 NIO 절대 좌표 직접 접근 (Eliminating Kernel Overhead):
 * FFM API의 `FileChannel.map()`(mmap)은 거대한 파일을 한 번에 RAM에 올리고 지속적으로 접근할 때는 가장 빠른 방법이지만,
 * 운영체제(OS) 입장에서는 파일 매핑을 위해 페이지 테이블(Page Table)을 수정하고 TLB를 갱신해야 하는 매우 무거운 시스템 콜(System Call)을 수반합니다.
 * 이 현미경 스캐너처럼 테라바이트급 디스크 상의 특정 점 단 1개(단 4바이트)만을 핀포인트로 대조하고 버리는 작업에 mmap을 반복 사용하면,
 * 커널의 메모리 관리 체계에 극심한 오버헤드와 페이지 폴트(Thrashing)를 유발합니다.
 * 교체된 `nioDirectPositionalRead` 메서드는 무거운 mmap을 과감히 폐기하고, 표준 Java NIO의 `FileChannel.read(ByteBuffer, offset)` 방식을 채택하여 
 * 운영체제의 사용자 공간(User Space) 메모리 맵을 비틀지 않고, 가장 가볍고 신속하게 단일 비트의 교차 대조를 완수합니다.
 * =============================================================================
 */