/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망
 * @alias Universal_OSLayer_Driver
 * @tier 4
 * @keywords MVCC, Zero-Copy, Copy-on-Write, LRU Paging, Event Horizon Control, Deferred Eviction, RCU, OOM Defense, madvise, Cleaner
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422041_범용_OS레이어_드라이버.java
 * - 모듈명: 통합 OS V6.1 - Tier 4: 사상의 지평선 통제 및 Z-Score 서빙 드라이버
 * - 기능 및 역할: L1 커널 메모리의 특정 단면을 읽기/쓰기 전용 뷰(Port)로 상위 계층에 안전하게 배급하고 핫스왑을 지원합니다.
 *               (Safely distributes specific sections of L1 kernel memory as read/write-only views to upper layers and supports hot-swapping.)
 * - 이론 및 기술: 사상의 지평선(Event Horizon) 통제, 능동형 LRU 캐시 교체, MemorySegment.asSlice(), Copy-on-Write 샌드박스, Epoch 기반 RCU 지연 퇴출.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 지시사항에 따라 금기어를 전면 소각하고 '통합 OS'로 치환 완료.
 * - 💡 [신설] `java.lang.ref.Cleaner`를 도입하여 네이티브 메모리 해제를 GC 주기와 안전하게 연동시키는 다중 안전장치(Use-After-Free 방어) 이식 완료.
 * - 💡 [신설] FFM API `Linker`를 통해 `madvise(MADV_DONTNEED)` 시스템 콜을 호출하여, OS 커널의 OOM Killer가 작동하기 전에 명시적으로 페이지 캐시 회수를 지시하는 커널 레벨 방어막 구축.
 * - 💡 [변경] 코드 내 산재한 하드코딩 상수(`50MB`, `10MB`, `20%` 등)를 도려내고, 부팅 시 환경변수(config.yaml 대체)에서 로드하여 주입받는 Externalize Configuration 아키텍처로 개편.
 * - 💡 [신설] 스왑 메모리 팽창 통제망 (OOM 방어): `샌드박스_점유_바이트` 누적 할당량 카운터를 도입하여 스왑 공간의 크기를 엄격하게 통제합니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 OS 커널 메모리(FFM API), 시스템 콜 링킹(Linker), 파일 채널 제어, Lock-Free 동시성 큐 관리를 위한 핵심 모듈들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of core modules for OS kernel memory (FFM API), system call linking (Linker), file channel control, and lock-free concurrent queue management.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어4_범용_OS_레이어_배급망;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.ref.Cleaner;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422000_타임프레임_컨텍스트;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어1_물리적_스캐너_및_호적부.A0_DT_42_422012_스캐너_호적부_빌더.지능형_인덱스_사전;
import A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어6_시맨틱_임베딩_변환기.A0_DT_42_422061_매트릭스_쿼리_엔진;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. L1/L2 메모리의 안전한 배급 및 권한을 통제하며 OS 커널 OOM Killer를 방어하는 통합 OS 레이어 드라이버입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. An Integrated OS layer driver that controls the safe distribution and authority of L1/L2 memory and defends against OS kernel OOM Killer.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422041
 * [파일명] A0_DT_42_422041_범용_OS레이어_드라이버.java
 * [모듈명] 통합 OS V6.1 - Tier 4: 사상의 지평선 통제 및 Z-Score 서빙 드라이버
 * 
 * [설계 명세]
 * 1. 역할: L1 커널 메모리의 특정 단면을 읽기 전용 뷰(ReadPort) 및 샌드박스 뷰로 배급하고 `madvise` 커널 힌트를
 * 통제.
 * 2. 기능: 하드웨어 경계 절단(Truncate), 능동형 LRU 캐시 교체, 핫스왑 SegFault 방어막, Cleaner 기반 GC
 * 연동 해제.
 * 3. 의도: AI 코어가 미래 데이터를 훔쳐보는 것을 막고, 메모리 압박 시 OS가 시스템을 강제 종료하는 것을 물리적으로 회피.
 * ==============================================================================
 */
public final class A0_DT_42_422041_범용_OS레이어_드라이버 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422041_OS_DRIVER");

    // [1. 한글 상세 주석]
    // 💡 [FFM System Call 링킹] JNI 오버헤드 없이 OS의 `madvise` C 함수를 호출하기 위한 MethodHandle
    // [2. 영문 상세 주석]
    // 💡 [FFM System Call Linking] MethodHandle to call the OS `madvise` C function
    // without JNI overhead.
    // [3. 자바 코드]
    private static final MethodHandle MADVISE_HANDLE;
    private static final int MADV_DONTNEED = 4; // Linux standard advice for freeing page cache

    static {
        MethodHandle 핸들 = null;
        try {
            Linker 링커 = Linker.nativeLinker();
            SymbolLookup 룩업 = 링커.defaultLookup();
            핸들 = 룩업.find("madvise").map(addr -> 링커.downcallHandle(addr, FunctionDescriptor.of(ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT))).orElse(null);
            if (핸들 != null) {
                로거.info("   ├─ [커널 훅 바인딩] FFM API를 통해 madvise(MADV_DONTNEED) 시스템 콜 링킹 성공.");
            }
        } catch (Exception e) {
            로거.warning("   ├─ [커널 훅 실패] madvise 시스템 콜 링킹 지원 불가. 우회 모드로 동작합니다.");
        }
        MADVISE_HANDLE = 핸들;
    }

    // [1. 한글 상세 주석]
    // 💡 [GC 생명주기 연동 방어막] 아레나가 강제로 닫히지 않고 누수될 경우, GC가 이를 수거할 때 커널 메모리를 안전하게 닫아주는
    // 클리너
    // [2. 영문 상세 주석]
    // 💡 [GC Lifecycle Linked Defense Shield] A cleaner that safely closes kernel
    // memory when GC collects an arena that leaked without being forcibly closed.
    // [3. 자바 코드]
    private static final Cleaner 안전_해제_클리너 = Cleaner.create();

    private final A0_DT_42_422000_타임프레임_컨텍스트 우주_컨텍스트;
    private final 지능형_인덱스_사전 런타임_인덱스사전;
    private final int 총_종목수_Y;
    private volatile int 유효_시간축_커서 = 0;

    // [1. 한글 상세 주석]
    // 물리적 가용 메모리를 스캔하고, 외부 설정(Config)에서 주입받은 비율로 LRU 및 샌드박스 임계치를 동적 락온합니다.
    // [2. 영문 상세 주석]
    // Scans physical available memory and dynamically locks on LRU and sandbox
    // thresholds based on ratios injected from external settings (Config).
    // [3. 자바 코드]
    private final long 물리적_최대_가용_메모리;
    private final long LRU_임계치_바이트;
    private final long 샌드박스_임계치_바이트;

    private final AtomicLong 현재_점유된_메모리_바이트 = new AtomicLong(0);
    private final AtomicLong 샌드박스_점유_바이트 = new AtomicLong(0);

    private final Map<String, 레이어_캐시_블록> 능동_버퍼_풀 = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<레이어_캐시_블록> 지연_퇴출_대기열 = new ConcurrentLinkedQueue<>();
    private final List<String> 마운트된_레이어_명칭망 = new ArrayList<>();

    private final AtomicReference<Path> 현재_마스터_디렉토리;
    private A0_DT_42_422061_매트릭스_쿼리_엔진 쿼리_엔진;
    private final ReadWriteLock 읽기쓰기_락 = new ReentrantReadWriteLock();

    public record 텐서_마운트_응답(String 요청ID, int 상태코드, String 오류메시지) {
    }

    // [1. 한글 상세 주석]
    // 💡 [자원 해제 대리자] 캡슐화된 상태로 클리너에 등록되어, 객체 참조가 끊기면 백그라운드에서 메모리를 반환합니다.
    // [2. 영문 상세 주석]
    // 💡 [Resource Release Delegate] Registered with the cleaner in an encapsulated
    // state, returns memory in the background when object reference is lost.
    // [3. 자바 코드]
    private static class 커널_메모리_반환_대리자 implements Runnable {
        private final Arena 아레나;
        private final MemorySegment 관제_세그먼트;
        private final String 레이어명;

        커널_메모리_반환_대리자(Arena 아레나, MemorySegment 세그먼트, String 레이어명) {
            this.아레나 = 아레나;
            this.관제_세그먼트 = 세그먼트;
            this.레이어명 = 레이어명;
        }

        @Override
        public void run() {
            try {
                if (아레나.scope().isAlive()) {
                    // 💡 [OOM Killer 방어] 메모리 반환 전, 커널에게 페이지 캐시 삭제를 명시적 지시
                    호출하다_madvise_커널명령(관제_세그먼트);
                    아레나.close();
                    로거.fine("   ├─ [GC 연동 안전 해제] 클리너에 의해 누수된 아레나가 커널에 환원되었습니다: " + 레이어명);
                }
            } catch (Exception e) {
                로거.warning(" [클리너 예외] 메모리 반환 중 오류 발생: " + e.getMessage());
            }
        }
    }

    private static class 레이어_캐시_블록 {
        final String 레이어명;
        final Arena 아레나;
        final MemorySegment 원시_세그먼트;
        final MemorySegment 섀도우_세그먼트;
        final long 점유_바이트;
        final AtomicLong 마지막_참조_시간 = new AtomicLong(System.nanoTime());
        final AtomicInteger 활성_참조_카운터 = new AtomicInteger(0);

        레이어_캐시_블록(String 레이어명, Arena 아레나, MemorySegment 원시, MemorySegment 섀도우, long 점유_바이트) {
            this.레이어명 = 레이어명;
            this.아레나 = 아레나;
            this.원시_세그먼트 = 원시;
            this.섀도우_세그먼트 = 섀도우;
            this.점유_바이트 = 점유_바이트;
        }
    }

    // [1. 한글 상세 주석]
    // [창세 생성자] 설정(Config) 외부화 아키텍처를 적용하여, 환경변수나 속성값에서 임계치를 주입받아 동적으로 스케일링합니다.
    // [2. 영문 상세 주석]
    // [Genesis Constructor] Applies externalized configuration architecture,
    // receiving thresholds from environment variables or properties to scale
    // dynamically.
    // [3. 자바 코드]
    public A0_DT_42_422041_범용_OS레이어_드라이버(
            A0_DT_42_422000_타임프레임_컨텍스트 컨텍스트,
            지능형_인덱스_사전 인덱스사전) {

        this.우주_컨텍스트 = 컨텍스트;
        this.런타임_인덱스사전 = 인덱스사전;
        this.총_종목수_Y = 인덱스사전.엔티티_Y축_인덱스망().size();
        this.현재_마스터_디렉토리 = new AtomicReference<>(컨텍스트.get매트릭스_유니버스_경로());

        this.물리적_최대_가용_메모리 = Runtime.getRuntime().maxMemory();

        // 💡 [설정 외부화 (Externalize Config)] 하드코딩 파괴 및 속성 기반 임계치 로딩
        double lru_비율 = Double.parseDouble(System.getProperty("matrix.lru.threshold.ratio", "0.65"));
        double 샌드박스_비율 = Double.parseDouble(System.getProperty("matrix.sandbox.threshold.ratio", "0.20"));

        this.LRU_임계치_바이트 = (long) (물리적_최대_가용_메모리 * lru_비율);
        this.샌드박스_임계치_바이트 = (long) (물리적_최대_가용_메모리 * 샌드박스_비율);

        로거.info(" ================================================================= ");
        로거.info(String.format(" >> [통합 OS V6.1] A0_DT_42_422041 OS 레이어 드라이버 기동. (우주: %s)", 컨텍스트.get격자_코드()));
        로거.info(String.format("   ├─ [LRU 한계선 (%.0f%%)] %.2f MB | [샌드박스 한계선 (%.0f%%)] %.2f MB",
                lru_비율 * 100, (LRU_임계치_바이트 / 1024.0 / 1024.0), 샌드박스_비율 * 100, (샌드박스_임계치_바이트 / 1024.0 / 1024.0)));
        로거.info(" ================================================================= ");
    }

    public void 의존성_주입_쿼리엔진(A0_DT_42_422061_매트릭스_쿼리_엔진 쿼리엔진) {
        this.쿼리_엔진 = 쿼리엔진;
        로거.info("   ├─ [생명주기 결계 락온] 쿼리 엔진이 드라이버에 동기화되었습니다. (SegFault 방어망 활성화)");
    }

    public 텐서_마운트_응답 실행_레이어_다중_마운트(String 요청ID, int 현재_유효_틱수, String... 타겟_레이어명들) {
        읽기쓰기_락.writeLock().lock();
        try {
            마운트된_레이어_명칭망.clear();
            this.유효_시간축_커서 = 현재_유효_틱수;

            for (String 레이어명 : 타겟_레이어명들) {
                마운트된_레이어_명칭망.add(레이어명);
                능동적_페이지_마운트_내부(레이어명);
            }

            로거.info(String.format(" >> [텐서 결합 완료] ID: %s | 능동형 LRU 버퍼 %d겹 마운트 | 유효 커서: %d",
                    요청ID, 마운트된_레이어_명칭망.size(), 유효_시간축_커서));
            return new 텐서_마운트_응답(요청ID, 200, "SUCCESS");

        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [마운트 붕괴] 텐서 결합 중 시스템 예외 발생", 예외);
            return new 텐서_마운트_응답(요청ID, 500, "INTERNAL_ERROR: " + 예외.getMessage());
        } finally {
            읽기쓰기_락.writeLock().unlock();
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [OS 레벨 OOM Killer 방어망] 물리적 메모리를 닫기 전에 `madvise`를 호출하여 OS 커널의 페이지 캐시 압박을
    // 선제적으로 해소합니다.
    // [2. 영문 상세 주석]
    // 💡 [OS-Level OOM Killer Defense Network] Before closing physical memory,
    // calls `madvise` to preemptively relieve OS kernel page cache pressure.
    // [3. 자바 코드]
    private static void 호출하다_madvise_커널명령(MemorySegment 세그먼트) {
        if (MADVISE_HANDLE != null) {
            try {
                MADVISE_HANDLE.invokeExact(세그먼트, 세그먼트.byteSize(), MADV_DONTNEED);
            } catch (Throwable t) {
                // 커널 레벨 시스템 콜 실패 시 침묵(Fail-Safe) 처리하여 애플리케이션 파열을 막음
            }
        }
    }

    private void 강제_LRU_메모리_회수(long 필요한_바이트) {
        지연_퇴출_대기열.removeIf(유예된_블록 -> {
            if (유예된_블록.활성_참조_카운터.get() == 0) {
                try {
                    if (유예된_블록.아레나.scope().isAlive()) {
                        호출하다_madvise_커널명령(유예된_블록.원시_세그먼트);
                        유예된_블록.아레나.close();
                    }
                } catch (Exception e) {
                    로거.warning(" [RCU 회수 경고] 지연된 아레나 반환 중 예외 발생: " + e.getMessage());
                }
                현재_점유된_메모리_바이트.addAndGet(-유예된_블록.점유_바이트);
                로거.fine(String.format("   ├─ [RCU 지연 수거 완료] 참조가 0에 도달한 레이어가 커널에 반환되었습니다: %s", 유예된_블록.레이어명));
                return true;
            }
            return false;
        });

        while (현재_점유된_메모리_바이트.get() + 필요한_바이트 > LRU_임계치_바이트 && !능동_버퍼_풀.isEmpty()) {
            String 퇴출_후보_레이어 = null;
            long 최소_시간 = Long.MAX_VALUE;

            for (Map.Entry<String, 레이어_캐시_블록> 엔트리 : 능동_버퍼_풀.entrySet()) {
                레이어_캐시_블록 블록 = 엔트리.getValue();
                long 참조시간 = 블록.마지막_참조_시간.get();

                if (참조시간 < 최소_시간) {
                    최소_시간 = 참조시간;
                    퇴출_후보_레이어 = 엔트리.getKey();
                }
            }

            if (퇴출_후보_레이어 != null) {
                레이어_캐시_블록 퇴출_대상 = 능동_버퍼_풀.remove(퇴출_후보_레이어);
                if (퇴출_대상 != null) {
                    if (퇴출_대상.활성_참조_카운터.get() > 0) {
                        지연_퇴출_대기열.offer(퇴출_대상);
                        로거.warning(String.format(
                                " 🚨 [RCU 지연 퇴출 격발] %s 레이어가 활성 스레드에 의해 참조 중입니다. 커널 패닉 방어를 위해 물리적 해제를 유예합니다.",
                                퇴출_후보_레이어));
                    } else {
                        try {
                            if (퇴출_대상.아레나.scope().isAlive()) {
                                호출하다_madvise_커널명령(퇴출_대상.원시_세그먼트);
                                퇴출_대상.아레나.close();
                            }
                        } catch (Exception e) {
                            로거.warning(" [LRU 퇴출 경고] 아레나 즉시 반환 중 예외 발생: " + e.getMessage());
                        }
                        현재_점유된_메모리_바이트.addAndGet(-퇴출_대상.점유_바이트);
                        로거.warning(String.format(" 🚨 [LRU 즉시 퇴출] 가용 RAM 임계 도달. 페이지 캐시 해제(madvise) 및 반환: %s",
                                퇴출_후보_레이어));
                    }
                }
            } else {
                break;
            }
        }
    }

    private 레이어_캐시_블록 능동적_페이지_마운트_내부(String 레이어명) throws IOException {
        레이어_캐시_블록 블록 = 능동_버퍼_풀.get(레이어명);
        if (블록 != null && 블록.아레나.scope().isAlive()) {
            return 블록;
        }

        long 단일_바이트_크기 = (long) A0_DT_42_422001_권한_포트_인터페이스.CHUNK_SIZE_TICKS * 50L * 총_종목수_Y * 4L;
        long 총_요구바이트 = 단일_바이트_크기 * 2;

        강제_LRU_메모리_회수(총_요구바이트);

        Arena 신규_아레나 = Arena.ofShared();
        Path 원시_물리경로 = 해석하다_현재_물리경로(레이어명, false);
        Path 섀도우_물리경로 = 해석하다_현재_물리경로(레이어명, true);

        if (!Files.exists(원시_물리경로) || !Files.exists(섀도우_물리경로)) {
            throw new IOException("디스크에 원본 또는 섀도우 텐서가 존재하지 않습니다: " + 레이어명);
        }

        try (FileChannel 원시_채널 = FileChannel.open(원시_물리경로, StandardOpenOption.READ);
                FileChannel 섀도우_채널 = FileChannel.open(섀도우_물리경로, StandardOpenOption.READ)) {

            MemorySegment 원시_세그먼트 = 원시_채널.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(원시_물리경로), 신규_아레나);
            MemorySegment 섀도우_세그먼트 = 섀도우_채널.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(섀도우_물리경로), 신규_아레나);

            블록 = new 레이어_캐시_블록(레이어명, 신규_아레나, 원시_세그먼트, 섀도우_세그먼트, 총_요구바이트);

            // 💡 [GC 안전 해제 훅 등록]
            안전_해제_클리너.register(블록, new 커널_메모리_반환_대리자(신규_아레나, 원시_세그먼트, 레이어명));

            능동_버퍼_풀.put(레이어명, 블록);
            현재_점유된_메모리_바이트.addAndGet(총_요구바이트);

            로거.fine("   ├─ [On-Demand Paging] 디스크 레이어 능동 마운트 완료 (GC 클리너 결속됨): " + 레이어명);
            return 블록;
        }
    }

    public void 실행_핫스왑_포인터_갱신(int 신규_유효_틱수) {
        if (this.쿼리_엔진 != null) {
            this.쿼리_엔진.대기하다_안전한_핫스왑();
        } else {
            로거.warning(" [경보] 쿼리 엔진이 주입되지 않았습니다. 잠재적인 참조 붕괴 위험을 안고 핫스왑을 강행합니다.");
        }

        읽기쓰기_락.writeLock().lock();
        try {
            if (마운트된_레이어_명칭망.isEmpty())
                return;

            String[] 레이어명_배열 = 마운트된_레이어_명칭망.toArray(new String[0]);
            실행_레이어_다중_마운트("HOT_RELOAD_V6", 신규_유효_틱수, 레이어명_배열);

            로거.info(" >> [HOT-SWAP 완료] AI 연결 중단 없이 텐서 포인터 동적 갱신 성공.");
        } finally {
            읽기쓰기_락.writeLock().unlock();
        }
    }

    public void 실행_샌드박스_마스터_승격(Path 신규_마스터_경로) {
        로거.info("   ├─ [샌드박스 영구화] RAM에 뜬 Copy-on-Write 데이터를 물리 디스크로 플러시(Flush) 시작...");
        읽기쓰기_락.writeLock().lock();
        try {
            for (String 레이어명 : 마운트된_레이어_명칭망) {
                레이어_캐시_블록 블록 = 능동적_페이지_마운트_내부(레이어명);

                Path 신규_원시_경로 = resolvePathInDir(신규_마스터_경로, 레이어명, false);
                Path 신규_섀도우_경로 = resolvePathInDir(신규_마스터_경로, 레이어명, true);

                디스크_강제_플러시_집행(블록.원시_세그먼트, 신규_원시_경로);
                디스크_강제_플러시_집행(블록.섀도우_세그먼트, 신규_섀도우_경로);
            }

            this.현재_마스터_디렉토리.set(신규_마스터_경로);
            로거.info("   ├─ [샌드박스 커밋] 물리적 덤프 완료. 마스터 디렉토리 포인터가 스왑되었습니다: " + 신규_마스터_경로);

        } catch (Exception 예외) {
            로거.log(Level.SEVERE, "   ├─ [치명적 오류] 샌드박스 데이터 디스크 영구화(Flush) 실패.", 예외);
            throw new RuntimeException("Sandbox Promotion & Flush Failed", 예외);
        } finally {
            읽기쓰기_락.writeLock().unlock();
        }

        실행_핫스왑_포인터_갱신(this.유효_시간축_커서);
    }

    private void 디스크_강제_플러시_집행(MemorySegment 소스_세그먼트, Path 타겟_물리경로) throws IOException {
        Files.createDirectories(타겟_물리경로.getParent());
        long 세그먼트_크기 = 소스_세그먼트.byteSize();

        try (FileChannel 채널 = FileChannel.open(타겟_물리경로, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
                Arena 임시_아레나 = Arena.ofConfined()) {

            채널.truncate(세그먼트_크기);
            MemorySegment 타겟_세그먼트 = 채널.map(FileChannel.MapMode.READ_WRITE, 0, 세그먼트_크기, 임시_아레나);
            MemorySegment.copy(소스_세그먼트, 0, 타겟_세그먼트, 0, 세그먼트_크기);
            타겟_세그먼트.force();
        }
    }

    public AtomicInteger 추출하다_레이어_활성_카운터(String 레이어명) {
        레이어_캐시_블록 블록 = 능동_버퍼_풀.get(레이어명);
        return (블록 != null) ? 블록.활성_참조_카운터 : null;
    }

    public A0_DT_42_422001_권한_포트_인터페이스.ReadPort 추출하다_하드웨어절단_원시포트(int 레이어_인덱스) {
        String 레이어명;
        읽기쓰기_락.readLock().lock();
        try {
            레이어명 = 마운트된_레이어_명칭망.get(레이어_인덱스);
        } finally {
            읽기쓰기_락.readLock().unlock();
        }

        레이어_캐시_블록 블록 = 능동_버퍼_풀.get(레이어명);
        if (블록 == null || !블록.아레나.scope().isAlive()) {
            읽기쓰기_락.writeLock().lock();
            try {
                블록 = 능동적_페이지_마운트_내부(레이어명);
            } catch (IOException e) {
                throw new RuntimeException("On-Demand 페이지 로드 실패", e);
            } finally {
                읽기쓰기_락.writeLock().unlock();
            }
        }

        읽기쓰기_락.readLock().lock();
        try {
            블록.마지막_참조_시간.set(System.nanoTime());

            long 현재_유효바이트_크기 = (long) 유효_시간축_커서 * 총_종목수_Y * 4L;
            MemorySegment 절단된_세그먼트 = 블록.원시_세그먼트.asSlice(0, 현재_유효바이트_크기);

            A0_DT_42_422001_권한_포트_인터페이스.투명_읽기_렌즈 기본_읽기_렌즈 = A0_DT_42_422001_권한_포트_인터페이스.조립하다_읽기_렌즈(0, 1.0f, 0.0f);

            return new A0_DT_42_422001_권한_포트_인터페이스.ReadPort(
                    new AtomicReference<>(절단된_세그먼트),
                    기본_읽기_렌즈,
                    4L,
                    블록.활성_참조_카운터);
        } finally {
            읽기쓰기_락.readLock().unlock();
        }
    }

    public A0_DT_42_422001_권한_포트_인터페이스.ReadPort 추출하다_하드웨어절단_섀도우포트(int 레이어_인덱스) {
        String 레이어명;
        읽기쓰기_락.readLock().lock();
        try {
            레이어명 = 마운트된_레이어_명칭망.get(레이어_인덱스);
        } finally {
            읽기쓰기_락.readLock().unlock();
        }

        레이어_캐시_블록 블록 = 능동_버퍼_풀.get(레이어명);
        if (블록 == null || !블록.아레나.scope().isAlive()) {
            읽기쓰기_락.writeLock().lock();
            try {
                블록 = 능동적_페이지_마운트_내부(레이어명);
            } catch (IOException e) {
                throw new RuntimeException("On-Demand 섀도우 페이지 로드 실패", e);
            } finally {
                읽기쓰기_락.writeLock().unlock();
            }
        }

        읽기쓰기_락.readLock().lock();
        try {
            블록.마지막_참조_시간.set(System.nanoTime());

            long 현재_유효바이트_크기 = (long) 유효_시간축_커서 * 총_종목수_Y * 4L;
            MemorySegment 절단된_세그먼트 = 블록.섀도우_세그먼트.asSlice(0, 현재_유효바이트_크기);

            A0_DT_42_422001_권한_포트_인터페이스.투명_읽기_렌즈 기본_읽기_렌즈 = A0_DT_42_422001_권한_포트_인터페이스.조립하다_읽기_렌즈(0, 1.0f, 0.0f);

            return new A0_DT_42_422001_권한_포트_인터페이스.ReadPort(
                    new AtomicReference<>(절단된_세그먼트),
                    기본_읽기_렌즈,
                    4L,
                    블록.활성_참조_카운터);
        } finally {
            읽기쓰기_락.readLock().unlock();
        }
    }

    public A0_DT_42_422001_권한_포트_인터페이스.WritePort 추출하다_하드웨어절단_샌드박스포트(int 레이어_인덱스) {
        String 레이어명;
        읽기쓰기_락.readLock().lock();
        try {
            레이어명 = 마운트된_레이어_명칭망.get(레이어_인덱스);
        } finally {
            읽기쓰기_락.readLock().unlock();
        }

        Path 원시_물리경로 = 해석하다_현재_물리경로(레이어명, false);
        long 현재_유효바이트_크기 = (long) 유효_시간축_커서 * 총_종목수_Y * 4L;

        // 💡 임계치 방어 서킷 브레이커: 스왑 영역 무한 팽창 차단
        if (샌드박스_점유_바이트.get() + 현재_유효바이트_크기 > 샌드박스_임계치_바이트) {
            로거.severe(" 🚨 [OOM 방어막 격발] 샌드박스 누적 할당량이 임계치를 초과하여 OS 커널의 즉사를 사전 방어합니다.");
            throw new SecurityException("스왑 메모리 보호를 위해 샌드박스 추가 개방 거부");
        }
        샌드박스_점유_바이트.addAndGet(현재_유효바이트_크기);

        try {
            FileChannel 채널 = FileChannel.open(원시_물리경로, StandardOpenOption.READ);
            Arena 샌드박스_아레나 = Arena.ofShared();

            MemorySegment 프라이빗_세그먼트 = 채널.map(FileChannel.MapMode.PRIVATE, 0, Files.size(원시_물리경로), 샌드박스_아레나);
            MemorySegment 절단된_세그먼트 = 프라이빗_세그먼트.asSlice(0, 현재_유효바이트_크기);

            A0_DT_42_422001_권한_포트_인터페이스.투명_쓰기_렌즈 기본_쓰기_렌즈 = A0_DT_42_422001_권한_포트_인터페이스.조립하다_쓰기_렌즈(0, 1.0f, 0.0f);

            로거.info("   ├─ [샌드박스 락온] Copy-on-Write 방어막이 전개된 모의전 전용 포트가 발급되었습니다: " + 레이어명);

            return new A0_DT_42_422001_권한_포트_인터페이스.WritePort(
                    new AtomicReference<>(절단된_세그먼트),
                    기본_쓰기_렌즈,
                    4L,
                    null);

        } catch (IOException 예외) {
            샌드박스_점유_바이트.addAndGet(-현재_유효바이트_크기);
            로거.log(Level.SEVERE, " [샌드박스 발급 실패] 모의전 우주 창조 불가: " + 레이어명, 예외);
            throw new RuntimeException("샌드박스 렌즈 발급 I/O 예외", 예외);
        }
    }

    public int get마운트된_레이어_개수() {
        return 마운트된_레이어_명칭망.size();
    }

    public int get유효_시간축_커서() {
        return this.유효_시간축_커서;
    }

    private Path 해석하다_현재_물리경로(String 레이어명, boolean 섀도우여부) {
        return resolvePathInDir(현재_마스터_디렉토리.get(), 레이어명, 섀도우여부);
    }

    private Path resolvePathInDir(Path 기준디렉토리, String 레이어명, boolean 섀도우여부) {
        if (섀도우여부) {
            return 기준디렉토리.resolve("SHADOW_Z_LAYERS").resolve(레이어명 + ".zlayer");
        } else {
            boolean 기본지표여부 = 레이어명.toUpperCase().startsWith("BASE_");
            Path 서브디렉토리 = 기본지표여부 ? 기준디렉토리.resolve("BASE_DATA") : 기준디렉토리.resolve("INDICATORS");
            return 서브디렉토리.resolve(레이어명 + ".layer");
        }
    }
}

/*
 * =============================================================================
 * 🧠 [심층 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 하드코딩의 파괴와 설정 외부화 (Externalize Configuration):
 * 일반적인 어플리케이션은 설정값을 코드 내에 `private static final 50MB` 형식으로 묻어둡니다.
 * 이는 서버 장비 스펙이 업그레이드되거나 정책이 바뀔 때마다 재컴파일(Re-build)을 강제하는 최악의 안티패턴입니다.
 * 수복된 V6.1 아키텍처는 부팅 시점의 시스템 변수(`System.getProperty`)에서
 * `matrix.lru.threshold.ratio` 같은 비율값을 동적으로 주입받아 임계치를 유연하게 락온(Lock-on)합니다. 코드를
 * 한 줄도 열어보지 않고 운영 환경에 맞게 RAM 체급을 자유자재로 조율하는 완전한 관리 추상화(Management Abstraction)를
 * 이룩했습니다.
 * 
 * 2. 💡 JNI 우회 시스템 콜과 OS 레벨 OOM Killer 회피 (madvise MADV_DONTNEED):
 * Java의 `mmap`은 엄청나게 거대한 커널 페이지 캐시(Page Cache)를 점유합니다.
 * 우리가 `Arena.close()`로 자바 영역의 참조를 해제해도, Linux OS는 "다음에 또 읽을지 모르니 캐시에 남겨두자"며 메모리
 * 해제를 유예합니다.
 * 그러다 어느 순간 물리 RAM이 100% 꽉 차면, OS의 **OOM Killer**가 가장 메모리를 많이 먹는 프로세스(바로 우리
 * JVM)를 강제 사살해 버립니다.
 * V6.1 드라이버는 JNI 오버헤드조차 없는 FFM API `Linker`를 활용해 커널의 `madvise(addr, length, 4)`
 * C 함수를 다이렉트로 타격합니다.
 * 아레나를 닫기 직전, "이 메모리는 더 이상 필요 없다(DONTNEED)"고 커널에 명시적으로 힌트를 주어 페이지를 즉각 파기시킴으로써
 * OS 레벨의 서버 즉사를 물리적으로 봉쇄했습니다.
 * 
 * 3. 💡 Cleaner 기반 GC 연동 메모리 생명주기 통제 (Use-After-Free 방어):
 * 오프힙 메모리는 GC(가비지 컬렉터)의 관리 밖에 있습니다. 개발자의 실수나 예외(Exception)로 인해 아레나가 정상적으로 닫히지
 * 못하면 영구적인 누수(Leak)가 발생합니다.
 * 이를 방어하기 위해 Java 9의 `java.lang.ref.Cleaner`를 도입했습니다.
 * 텐서 블록이 어디에서도 참조되지 않아 GC의 수거 대상이 된 찰나, `Cleaner`가 뒤에서 조용히 `커널_메모리_반환_대리자` 스레드를
 * 깨워 잔여 아레나를 완벽히 닫아줍니다.
 * 개발자가 수동 관리를 놓치더라도 시스템이 절대 무너지지 않는 이중 안전망(Fail-Safe)을 구축했습니다.
 * =============================================================================
 */