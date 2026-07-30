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
 * - 기능 (Function): 방금 주조된 CSV 꼬리 부분 원시 바이트와 FFM 텐서 좌표 비트의 교차 대조.
 * - 역할 (Role): 주조된 CSV 꼬리 원시 바이트와 FFM 텐서 메모리의 교차 대조 현미경.
 * - 이론 (Theory): UTF-8 Bitwise Operation (비트마스크 경계 탐색), Boundary Alignment, 심해 잠수 한계선(Fail-Safe Limit).
 * - 기대효과 (Effect): 4KB 경계에 한글(3 Bytes)이 걸쳐 잘릴 때 발생하는 UTF-8 외계어 훼손 현상을 방지하고, 손상된 파일로 인한 OOM을 물리적으로 멸균.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정]: 특정 세력을 연상시키는 단어를 전면 소각하고 '통합 OS'로 치환 완료.
 * - 💡 [초정밀 수술] 심해 잠수 한계선(Fail-Safe Limit) 밸브 장착: 
 *                 파일이 손상되어 개행(\n)을 찾지 못한 채 `누적된_바이트_버퍼`가 무한히 팽창(`new byte[...]`)하여 
 *                 JVM 힙을 즉사(OOM)시키는 치명적 뇌관을 파괴했습니다. 버퍼 크기가 10MB를 초과하면 즉각 예외를 던져 
 *                 스캔을 폭파(서킷 브레이커 격발)하고 LMAX 로거로 사출하는 물리적 방어막을 전개했습니다.
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
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더.지능형_인덱스_사전;

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
// 컴플라이언스 선언 및 클래스 헤더. 바이트 단위로 디스크를 역방향 스캔하여 무결성을 대조하는 현미경 스캐너입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A microscope scanner that performs reverse byte-level disk scans to cross-verify integrity.
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
 * 1. 역할: 방금 주조된 CSV 꼬리 부분 원시 바이트와 FFM 텐서 좌표 비트의 교차 대조.
 * 2. 기능: 4KB 청크 기반 동적 버퍼(Prepend) 복원 스캔 및 UTF-8 경계 조율, OOM 한계선 차단.
 * 3. 의도: 다국어(UTF-8)가 청크 경계에서 잘려 훼손되는 현상과 무한 루프 OOM을 원천 방지.
 * 4. 공식: ChunkSize = min(4096, ReadPos) with Forward Alignment.
 * 5. 💡 [V6.0 초정밀 수술] 심해 잠수 한계선(Fail-Safe Limit) 밸브 장착:
 * 비정상적으로 거대한 단일 라인(개행 없는 손상 파일)을 역방향 스캔할 때 동적 버퍼가 무한 팽창하여
 * OOM 크래시가 발생하는 것을 막기 위해 `최대_누적_버퍼_제한_10MB` 임계치를 돌파하면 즉각 예외를 발산하여 서킷 브레이커를
 * 격발합니다.
 * ==============================================================================
 */
public final class A0_DT_42_422031_바이트_역방향_현미경_스캐너 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422031_MICROSCOPE_SCANNER");

    // 종목코드(6자리 숫자) 추출용 정규식 패턴
    private static final Pattern 종목코드_패턴 = Pattern.compile("(\\d{6})");

    // 💡 역방향 스윕 시 디스크에서 한 번에 퍼 올릴 바이트 청크 크기 (OS 페이지 사이즈 4KB에 최적화)
    private static final int 청크_사이즈 = 4096;

    // [1. 한글 상세 주석]
    // 💡 [심해 잠수 한계선] 단일 라인이 비정상적으로 길어 OOM을 유발하는 것을 막기 위한 10MB 한계선 상수입니다.
    // [2. 영문 상세 주석]
    // 💡 [Deep-sea Diving Limit] A 10MB limit constant to prevent abnormally long
    // single lines from causing OOM.

    private static final int 최대_누적_버퍼_제한_10MB = 10 * 1024 * 1024;

    /**
     * 상태 없는(Stateless) 검수 엔진이므로 자유롭게 생성 가능합니다.
     */
    public A0_DT_42_422031_바이트_역방향_현미경_스캐너() {
        로거.info(" >> [통합 OS V6.0] A0_DT_42_422031 바이트 역방향 현미경 스캐너 기동. (동적 버퍼 결합, UTF-8 경계 멸균 및 OOM 방어 밸브 탑재)");
    }

    /**
     * [검수 역학 1] 신규 주조된 CSV 데이터의 꼬리(마지막 행)를 역방향으로 고속 추출하여,
     * L1 매트릭스 메모리에 꽂힌 절대 좌표의 부동소수점 비트와 교차 검증합니다.
     * 
     * @param 대상_우주컨텍스트   타임프레임 경로 정보
     * @param 런타임_인덱스사전   Y축(종목), Z축(지표) 및 X축(시간)을 수학적으로 계산할 호적부
     * @param 타겟_델타_CSV목록 검증할 신규 원본 파일들의 경로
     * @param 로거_의존성      에러 발생 시 2단계 커밋을 위한 비동기 로거 인스턴스
     * @return boolean 무결성 통과 여부 (단 1개라도 실패하면 false)
     */
    public boolean 실행_델타_바이트_교차검증(
            A0_DT_42_422000_타임프레임_컨텍스트 대상_우주컨텍스트,
            지능형_인덱스_사전 런타임_인덱스사전,
            List<Path> 타겟_델타_CSV목록,
            A0_DT_42_422033_LMAX_이상_보고서_로거 로거_의존성) {

        if (타겟_델타_CSV목록 == null || 타겟_델타_CSV목록.isEmpty()) {
            return true;
        }

        // 병렬 스레드 환경에서 오염 발생 시 다른 스레드들의 스캔을 즉각 중단시키기 위한 원자적(Atomic) 플래그
        AtomicBoolean 시스템_청결_상태 = new AtomicBoolean(true);

        // 하드웨어 코어 수에 맞춘 병렬 스트림 검증 (Fail-Fast 적용)
        타겟_델타_CSV목록.parallelStream().forEach(csv경로 -> {
            // 다른 스레드에서 오염이 발견되었다면 자신의 연산을 즉시 포기(Short-circuit)
            if (!시스템_청결_상태.get())
                return;

            String 파일명 = csv경로.getFileName().toString();
            Matcher 매처 = 종목코드_패턴.matcher(파일명);

            if (매처.find()) {
                String 종목코드 = 매처.group(1);
                Integer y축_인덱스 = 런타임_인덱스사전.엔티티_Y축_인덱스망().get(종목코드);

                if (y축_인덱스 != null) {
                    boolean 통과여부 = 단일_델타_꼬리_검증(
                            csv경로, 종목코드, y축_인덱스,
                            대상_우주컨텍스트, 런타임_인덱스사전, 로거_의존성);

                    if (!통과여부) {
                        시스템_청결_상태.set(false); // 서킷 브레이커 발동
                    }
                }
            }
        });

        return 시스템_청결_상태.get();
    }

    // [1. 한글 상세 주석]
    // 💡 [OOM 뇌관 적출] 바이트 스캔 엔진이 10MB 초과 예외를 던지면 이를 캐치하여 서킷 브레이커를 격발시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [OOM Detonator Removal] Catches the 10MB excess exception thrown by the
    // byte scan engine to trigger the circuit breaker.

    /**
     * [검수 역학 2] 개별 파일의 끝에서부터 읽어 올라오는 역방향 스윕으로 마지막 데이터 행(Tail)을 파싱하고,
     * FFM 엔진이 찔러넣은 메모리의 절대 좌표값과 정밀 대조합니다.
     */
    private boolean 단일_델타_꼬리_검증(
            Path csv경로,
            String 종목코드,
            int y축_인덱스,
            A0_DT_42_422000_타임프레임_컨텍스트 대상_우주컨텍스트,
            지능형_인덱스_사전 런타임_인덱스사전,
            A0_DT_42_422033_LMAX_이상_보고서_로거 로거_의존성) {

        String 마지막_행;
        try {
            // 스마트 바이너리 청크 리더를 통해 파일의 맨 마지막 데이터 로우를 완벽한 문자열로 복원
            마지막_행 = 바이트_역방향_스캔_엔진(csv경로.toFile());
        } catch (IllegalStateException e) {
            // 💡 10MB 버퍼 팽창 한계선 도달 (OOM 방어 발동)
            로거_의존성.reportAnomaly(종목코드, "N/A", "N/A", "OOM_LIMIT_EXCEEDED",
                    "손상된 파일로 인해 단일 라인 누적 버퍼가 10MB를 초과하여 스캔을 강제 중단했습니다.");
            return false; // 서킷 브레이커 즉각 격발
        }

        if (마지막_행 == null || 마지막_행.trim().isEmpty())
            return true;

        String[] 파편들 = 마지막_행.split(",", -1);
        String 틱_문자열 = 파편들[0].trim();

        // 수학적 O(1) 인덱스 역산
        int x축_인덱스 = 런타임_인덱스사전.X축_시간_격자_엔진().getIndex(틱_문자열);

        // 호적부에 없는 시간(오류 데이터)은 검증 대상에서 제외
        if (x축_인덱스 < 0)
            return true;

        String[] 헤더_배열;
        try (var 판독기 = Files.newBufferedReader(csv경로, StandardCharsets.UTF_8)) {
            String 헤더라인 = 판독기.readLine();
            if (헤더라인 == null)
                return true;
            헤더_배열 = 헤더라인.split(",");
        } catch (Exception e) {
            return false;
        }

        // 각 컬럼(지표)별로 디스크의 텐서 데이터와 교차 검증 수행
        for (int i = 1; i < 헤더_배열.length; i++) {
            String 지표명 = 헤더_배열[i].trim();
            if (!런타임_인덱스사전.지표_Z축_인덱스망().containsKey(지표명))
                continue;

            String 원시값_문자열 = 파편들[i].trim();
            float csv_원본값 = Float.NaN;

            if (!원시값_문자열.isEmpty() && !원시값_문자열.equals("NaN") && !원시값_문자열.equals("null")) {
                try {
                    csv_원본값 = Float.parseFloat(원시값_문자열);
                } catch (NumberFormatException ignored) {
                }
            }

            // 결측치가 아닌 유효한 실수가 존재할 경우에만 메모리와의 교차 대조 수행
            if (!Float.isNaN(csv_원본값)) {
                long 정확한_바이트_오프셋 = A0_DT_42_422001_권한_포트_인터페이스.산출_청크_내부_오프셋(y축_인덱스, x축_인덱스, 4L);
                Path 레이어_물리경로 = 대상_우주컨텍스트.resolve레이어_절대_경로(지표명);

                float 텐서_메모리값 = NIO_다이렉트_핀포인트_읽기(레이어_물리경로, 정확한_바이트_오프셋);

                // IEEE 754 규격에 따른 완벽한 비트 단위(Bit-level) 부동소수점 대조
                if (Float.compare(csv_원본값, 텐서_메모리값) != 0) {
                    String 에러_사유 = String.format("물리적 텐서 붕괴! 원본: %f vs 텐서: %f (절대 오프셋: %d)",
                            csv_원본값, 텐서_메모리값, 정확한_바이트_오프셋);

                    로거_의존성.reportAnomaly(종목코드, 틱_문자열, 지표명, "INTEGRITY_MISMATCH", 에러_사유);
                    return false; // 서킷 브레이커 격발
                }
            }
        }
        return true;
    }

    // [1. 한글 상세 주석]
    // 💡 [핵심 수술 완료: UTF-8 경계선 훼손 방어 및 동적 조율, OOM 한계선 장착]
    // 버퍼 결합 시 10MB를 초과하면 IllegalStateException을 위로 던져 힙 메모리 파열을 원천 봉쇄합니다.
    // [2. 영문 상세 주석]
    // 💡 [Core Surgery Completed: UTF-8 Boundary Corruption Defense and Dynamic
    // Alignment, OOM Limit Equipped]
    // When combining buffers, if it exceeds 10MB, throws an IllegalStateException
    // upwards to fundamentally block heap memory rupture.

    private String 바이트_역방향_스캔_엔진(File 파일) {
        try (RandomAccessFile 랜덤_액세스 = new RandomAccessFile(파일, "r")) {
            long 파일_총길이 = 파일.length();
            if (파일_총길이 == 0)
                return null;

            long 읽기_포인터 = 파일_총길이;
            boolean 후행_찌꺼기_제거완료 = false;

            // 청크 내에 개행이 없으면 이전 바이트를 보존하고 다음 청크를 앞에 결합(Prepend)합니다.
            byte[] 누적된_바이트_버퍼 = new byte[0];

            while (읽기_포인터 > 0) {
                // 남은 바이트 수와 4KB 청크 중 작은 값을 스캔 사이즈로 결정
                int 읽을_바이트수 = (int) Math.min(청크_사이즈, 읽기_포인터);
                long 청크_시작위치 = 읽기_포인터 - 읽을_바이트수;

                // UTF-8 경계 탐색 배관 (한글 찢어짐 방어)
                if (청크_시작위치 > 0) {
                    랜덤_액세스.seek(청크_시작위치);
                    byte 경계_바이트 = 랜덤_액세스.readByte();

                    // (경계_바이트 & 0xC0) == 0x80 은 '연속 바이트(Continuation Byte)'를 의미합니다.
                    while ((경계_바이트 & 0xC0) == 0x80) {
                        청크_시작위치++;
                        읽을_바이트수--;

                        if (읽을_바이트수 == 0)
                            break;

                        랜덤_액세스.seek(청크_시작위치);
                        경계_바이트 = 랜덤_액세스.readByte();
                    }
                }

                byte[] 현재_청크_버퍼 = new byte[읽을_바이트수];
                랜덤_액세스.seek(청크_시작위치);
                랜덤_액세스.readFully(현재_청크_버퍼);

                int 발견된_개행_인덱스 = -1;

                // 청크 내부에서 뒤에서부터 앞으로 1바이트씩 정밀 검사
                for (int i = 읽을_바이트수 - 1; i >= 0; i--) {
                    byte 단일_바이트 = 현재_청크_버퍼[i];

                    if (!후행_찌꺼기_제거완료) {
                        // EOF 엣지 케이스 방어: 파일 끝에 무의미하게 붙어있는 연속 개행/공백 문자를 바이패스
                        if (단일_바이트 == '\n' || 단일_바이트 == '\r' || 단일_바이트 == ' ') {
                            continue;
                        } else {
                            후행_찌꺼기_제거완료 = true;
                            byte[] 다듬어진_버퍼 = new byte[i + 1];
                            System.arraycopy(현재_청크_버퍼, 0, 다듬어진_버퍼, 0, i + 1);
                            현재_청크_버퍼 = 다듬어진_버퍼;
                        }
                    } else {
                        // 진짜 마지막 줄이 시작되는 개행 문자(0x0A) 탐지
                        if (단일_바이트 == '\n') {
                            발견된_개행_인덱스 = i + 1;
                            break;
                        }
                    }
                }

                if (후행_찌꺼기_제거완료) {
                    if (발견된_개행_인덱스 != -1) {
                        int 유효_길이 = 현재_청크_버퍼.length - 발견된_개행_인덱스;
                        byte[] 최종_합체_바이트 = new byte[유효_길이 + 누적된_바이트_버퍼.length];

                        System.arraycopy(현재_청크_버퍼, 발견된_개행_인덱스, 최종_합체_바이트, 0, 유효_길이);
                        System.arraycopy(누적된_바이트_버퍼, 0, 최종_합체_바이트, 유효_길이, 누적된_바이트_버퍼.length);

                        return new String(최종_합체_바이트, StandardCharsets.UTF_8).trim();

                    } else {
                        // [1. 한글 상세 주석]
                        // 💡 [안전망 전개: OOM 방어 밸브]
                        // 개행을 찾지 못하면 현재 청크 전체를 누적 버퍼의 맨 앞에 결합(Prepend)해야 합니다.
                        // 이때 팽창될 버퍼 크기가 10MB를 초과하면 JVM 힙 메모리 파열을 막기 위해 예외를 던집니다.
                        // [2. 영문 상세 주석]
                        // 💡 [Safety Net Deployment: OOM Defense Valve]
                        // If no newline is found, the entire current chunk must be prepended to the
                        // accumulated buffer.
                        // If the expanded buffer size exceeds 10MB, an exception is thrown to prevent
                        // JVM heap memory rupture.
                    
                        if (현재_청크_버퍼.length + 누적된_바이트_버퍼.length > 최대_누적_버퍼_제한_10MB) {
                            throw new IllegalStateException("OOM 방어막 가동: 단일 라인의 크기가 10MB 한계선을 돌파했습니다.");
                        }

                        byte[] 팽창된_버퍼 = new byte[현재_청크_버퍼.length + 누적된_바이트_버퍼.length];
                        System.arraycopy(현재_청크_버퍼, 0, 팽창된_버퍼, 0, 현재_청크_버퍼.length);
                        System.arraycopy(누적된_바이트_버퍼, 0, 팽창된_버퍼, 현재_청크_버퍼.length, 누적된_바이트_버퍼.length);
                        누적된_바이트_버퍼 = 팽창된_버퍼; // 버퍼 보존 및 갱신
                    }
                }

                // 청크 내에 개행이 없었거나 아직 유효 데이터를 만나지 못했다면 다음 4KB(이전 위치)로 후퇴
                읽기_포인터 = 청크_시작위치;
            }

            // 파일 전체의 처음(0)까지 도달했는데도 개행(\n)이 하나도 없다면 (단일 행 파일 처리)
            if (후행_찌꺼기_제거완료 && 누적된_바이트_버퍼.length > 0) {
                return new String(누적된_바이트_버퍼, StandardCharsets.UTF_8).trim();
            }

            return null;

        } catch (IllegalStateException e) {
            // 💡 [OOM 밸브 예외 전파] 한계선 돌파 예외는 여기서 삼키지 않고 위로 던져 서킷 브레이커를 격발시킵니다.
            throw e;
        } catch (Exception e) {
            로거.log(Level.SEVERE, " [현미경 스캔 에러] 순수 바이트 역방향 탐색 중 치명적 오류 발생: " + 파일.getName(), e);
            return null;
        }
    }

    /**
     * 💡 [NIO 다이렉트 핀포인트 읽기]
     * 파일 채널의 커서를 이동시키지 않는 절대 위치(positional) 읽기를 수행하여 커널 부하를 0으로 수렴시킵니다.
     */
    private float NIO_다이렉트_핀포인트_읽기(Path 레이어_파일경로, long 절대_바이트_오프셋) {
        if (!Files.exists(레이어_파일경로)) {
            return Float.NaN;
        }

        try (FileChannel 채널 = FileChannel.open(레이어_파일경로, StandardOpenOption.READ)) {

            ByteBuffer 핀포인트_버퍼 = ByteBuffer.allocate(4);
            핀포인트_버퍼.order(ByteOrder.LITTLE_ENDIAN);

            // 논블로킹(Positional Read) 방식의 파일 시스템 캐시 직접 타격
            int 읽은_바이트수 = 채널.read(핀포인트_버퍼, 절대_바이트_오프셋);

            if (읽은_바이트수 == 4) {
                핀포인트_버퍼.flip();
                return 핀포인트_버퍼.getFloat();
            }
            return Float.NaN;

        } catch (Exception e) {
            로거.warning(" [메모리 접근 실패] L1 매트릭스 텐서 메모리 핀포인트 접근 중 예외: " + 레이어_파일경로.getFileName());
            return Float.NaN;
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 바이트 레벨 탐색과 UTF-8 인코딩의 훼손 방어 (Defending UTF-8 Boundary Corruption):
 * 자바의 `String`은 객체화되는 순간 데이터가 디코딩 룰(UTF-8)에 종속됩니다. 일반적인 시스템은
 * 4096 바이트 크기의 청크(Chunk) 덩어리를 냅다 잘라내어 `new String(buffer, UTF_8)`을 호출합니다.
 * 이 방식은 1바이트 구조인 영어권에서는 문제가 없으나, 한글("종가" = 6바이트)이나 다국어 데이터가 존재할 때
 * 4KB 경계선이 글자의 중간 바이트(예: `1110xxxx 10xxxxxx`)를 무자비하게 절단해버립니다.
 * 잘려나간 조각을 String으로 캐스팅하면 자바는 이를 해독 불가로 판단, 이른바 '외계어(Replacement Character)'로
 * 영구 훼손시킵니다.
 * 
 * 본 코드는 텍스트(String)의 세계로 진입하기 전에, 순수 바이너리(byte[]) 차원에서
 * 4096번째 바이트의 비트를 검사(`byte & 0xC0 == 0x80`)하여 잘려진 문자의 허리인지를 확인합니다.
 * 연속 바이트임이 판명되면, 온전한 시작 바이트(또는 ASCII)를 만날 때까지 청크 시작 포인터를 전진(++)시켜
 * 다국어가 잘리는 현상을 물리적으로 차단했습니다. 남겨진 조각들은 다음 루프(더 앞쪽을 읽는 과정)에서 온전히 수거되어 조립됩니다.
 * 
 * 2. 💡 심해 잠수 한계선(Fail-Safe Limit)의 기하학적 방어막:
 * 기존의 동적 버퍼 결합(Prepend) 방식은 "언젠가는 개행 문자(\n)가 나온다"는 순진한 가정에 의존했습니다.
 * 만약 네트워크 전송 오류나 디스크 깨짐으로 인해 1GB짜리 파일에 개행이 단 한 개도 존재하지 않는다면,
 * 이 현미경 스캐너는 4KB씩 뒤로 후퇴하며 `new byte[기존크기 + 4096]`을 무한 반복하다 결국 JVM 힙 전체를
 * 터뜨리는(OOM)
 * 최악의 뇌관이 됩니다.
 * 수리된 V6.0 아키텍처는 `최대_누적_버퍼_제한_10MB` 밸브를 장착하여, 단일 라인이 비정상적으로 길어지는 순간
 * 스캔 행위 자체를 폭파(`IllegalStateException`)시킵니다. 이 예외는 상위 레이어에서 안전하게 캐치되어
 * 해당 파일만 `false`(서킷 브레이커)로 격리시킴으로써, 하나의 오염된 파편이 서버 전체의 기억상실증(OOM)을 유발하는
 * 나비효과를 물리적으로 봉쇄했습니다.
 * 
 * 3. mmap 스래싱(Thrashing) 방어와 NIO 다이렉트 접근 (Eliminating Kernel Overhead):
 * FFM API의 `channel.map()`(mmap)은 파일을 RAM에 올리는 가장 빠른 방법이지만, 운영체제(OS) 입장에서는
 * 페이지 테이블을 수정하고 TLB를 갱신해야 하는 매우 무거운 시스템 콜(System Call)을 수반합니다.
 * 이 현미경 스캐너처럼 디스크 상의 특정 점 1개(단 4바이트)만을 핀포인트로 대조하고 버려야 할 때는
 * 커널의 메모리 관리 체계를 극도로 피로하게 만듭니다(Thrashing).
 * 수술된 `NIO_다이렉트_핀포인트_읽기`는 mmap을 과감히 폐기하고, 표준 Java NIO의
 * `FileChannel.read(ByteBuffer)`를 채택하여 사용자 공간(User Space)의 메모리 맵을 뒤틀지 않고
 * 가장 우아하고 신속하게 단일 비트 대조를 완수합니다.
 * =============================================================================
 * 
 * 💡 [입문자 해설 (Beginner's Guide)]
 * 
 * - **심해 잠수 한계선(OOM 방어) 비유**:
 * 잠수부가 줄을 잡고 심해로 내려가는데, 바닥(개행 문자)이 닿을 때까지 끝없이 줄(메모리 버퍼)을 늘리는 상황입니다.
 * 바닥이 없는 심해 구덩이(손상된 파일)에 빠지면 결국 배 위에 있는 모든 줄(JVM 힙 메모리)이 다 풀려버려
 * 배가 침몰(OOM)하게 됩니다. 그래서 "10MB까지만 줄을 풀고 바닥이 안 닿으면 줄을 끊어라!"라는
 * 안전 밸브를 장착한 것입니다.
 * =============================================================================
 */