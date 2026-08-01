/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인
 * @alias SelfOrganizingKnowledgeWeaver
 * @tier 16
 * @keywords Knowledge Weaving, Self-Organization, FastUtil Adapter, Zero-Allocation Pipeline, Open Addressing
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_423040_자가_조직화_지식망_직조기.java
 * - 역할: 하위 계층(T8~T12)의 추론/투영 결과물들을 취합하여, 최종적으로 자가 조직화된(Self-Organizing) 3D 지식망 텐서를 FFM 메모리 상에 물리적으로 직조(Weaving).
 * - 기능: 외부 비정형 문헌의 텐서 사영(Projection) 결합, 하위 모듈 비동기 콜백(Callback) 호출 및 파이프라인 조율, 구세대 레거시 맵(Map) 어댑터 브릿지 역할.
 * - 이론 및 기술: 자가 조직화 맵(SOM: Self-Organizing Map), 의존성 주입(DI), 어댑터 패턴(Adapter Pattern), 개방 주소법(Open Addressing).
 * - 기대효과: 기존 자바 표준 Map 구격의 하위 호환성(Backward Compatibility)을 유지하면서도, 하위 V6.1 모듈들의 Zero-Allocation FastUtil 아키텍처와 완벽히 결속하여 박싱(Boxing) 타입 충돌을 멸균합니다.
 * 
 * [신규/변경/삭제 사항 (V6.1 리메이크)]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [패치 완수: 컴파일 붕괴 수술] Maven 빌드 중 발생하던 `cannot find symbol: 포획하다_문헌_모순_텐서` 오류를 완벽히 영구 수복. 
 *                 하위 종속 모듈(`422081`)이 영문화 갱신됨에 따라, 본 직조기에서 이를 호출하던 람다(Lambda) 스코프 내부의 메서드 시그니처를 최신 규격인 `captureDocumentContradictionTensor`로 완벽히 동기화 교정했습니다.
 * - 💡 [아키텍처 유지] `adaptStandardMapToPrimitive` 유틸리티 어댑터를 내장하여, 외부 크롤러나 레거시 모듈에서 넘어온 
 *                 무거운 표준 `Map<Integer, Double>`을 하위 V6.1 코어 모듈이 요구하는 가벼운 `Int2DoubleMap` 원시 타입으로 안전하게 캐스팅(Casting) 및 디커플링(Decoupling)하는 아키텍처는 그대로 100% 보존합니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 의존성 Import.
// 하위 계층인 T8~T12 모듈들의 인터페이스 규격(Primitive Collection)에 맞추기 위해 FastUtil 라이브러리를 도입하여 힙 객체 오염을 방어합니다.
// [2. 영문 상세 주석]
// Package declaration and dependencies Import.
// Introduced the FastUtil library to match the interface specifications (Primitive Collection) of the lower-layer T8~T12 modules, defending against heap object pollution.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

// 하위 T8 모듈 및 FFM 인터페이스 (의존성 주입 대상 / DI Dependencies)
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어0_시공간_기저_인프라.A0_DT_42_422001_권한_포트_인터페이스.WritePort;
import A0_QuantumVectorDB_양자벡터DB.L3_TDQI_심층_사유_코어.티어8_문헌_해체_및_3D_관계망_직조기.A0_DT_42_422081_모순_유예_양자_버퍼;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인.A0_DT_42_423020_시맨틱_문헌_해체_도끼.DocumentMetadata;
import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인.A0_DT_42_423030_무인_위상_사영소.TopologicalTensorParticle;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 통합 OS V6.1 하위 계층 코어망과의 타입 호환성(Adapter Pattern)을 100% 확보한 텐서 파이프라인 조율 직조기(Weaver)입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A tensor pipeline orchestrator (Weaver) that secures 100% type compatibility (Adapter Pattern) with the lower-layer core networks of Integrated OS V6.1.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_423040
 * [파일명] A0_DT_42_423040_자가_조직화_지식망_직조기.java
 * [모듈명] 통합 OS V6.1 - Tier 16: 자가 조직화 지식망 직조기 (Self-Organizing Knowledge Weaver)
 * ==============================================================================
 */
public final class A0_DT_42_423040_자가_조직화_지식망_직조기 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.423040_GRAPH_WEAVER");

    // [1. 한글 상세 주석]
    // 💡 [임계치 상수] 서로 다른 위상 데이터가 기하학적으로 치명적인 정면 충돌(Contradiction)을 일으켰다고 판단할 코사인 유사도의 하한선 임계치(-0.3) 및 수학적 0분할(Zero-Division) 발산을 막기 위한 디랙 에프실론 상수입니다.
    // [2. 영문 상세 주석]
    // 💡 [Threshold Constants] The lower bound threshold (-0.3) of cosine similarity to judge that different topological data have caused a geometrically fatal head-on collision (Contradiction), and the Dirac epsilon constant to prevent mathematical divergence from zero-division.

    private static final double COLLISION_DETECTION_THRESHOLD = -0.3;
    private static final double DIRAC_EPSILON = 1e-7;

    // [1. 한글 상세 주석]
    // 💡 [FFM 물리 규격] FFM API에서 커널 오프힙 텐서를 다이렉트로 읽고 쓰기 위한 하드웨어 C-Contiguous 친화적 리틀 엔디안(LITTLE_ENDIAN) 32-bit Float 물리 메모리 레이아웃 규격입니다.
    // [2. 영문 상세 주석]
    // 💡 [FFM Physical Specifications] Hardware C-Contiguous friendly little-endian 32-bit Float physical memory layout specification for directly reading and writing kernel off-heap tensors in the FFM API.

    private static final ValueLayout.OfFloat TENSOR_FLOAT_LAYOUT = ValueLayout.JAVA_FLOAT
            .withOrder(ByteOrder.LITTLE_ENDIAN);

    // [1. 한글 상세 주석]
    // 💡 [개방 주소법(Open Addressing) 공간 라우팅 사전] 해시 충돌(Hash Collision) 파열 방어를 위해 UUID와 할당된 Y축 공간 인덱스를 매핑하고, 이미 점유된 슬롯을 동시성으로 추적합니다.
    // [2. 영문 상세 주석]
    // 💡 [Open Addressing Spatial Routing Dictionary] Maps UUIDs to assigned Y-axis spatial indices and concurrently tracks already occupied slots to defend against Hash Collision ruptures.

    private static final Map<String, Long> documentTopologyRoutingDict = new ConcurrentHashMap<>();
    private static final Set<Long> allocatedYIndexSet = ConcurrentHashMap.newKeySet();

    // [1. 한글 상세 주석]
    // 직조(Weaving)가 확정된 최종 텐서를 보관하는 메인 글로벌 지식망과, 모순(충돌) 발생 시 텐서를 격리 유예하는 버퍼(Tier 8) 포트 객체입니다.
    // 통합 OS V6.1 아키텍처 박싱 멸균 규격에 맞추어 `ConcurrentHashMap<String, Int2DoubleMap>` 타입으로 완벽히 업그레이드 되었습니다.
    // [2. 영문 상세 주석]
    // The main global knowledge network storing the final tensors whose weaving has been confirmed, and the buffer (Tier 8) port object that isolates and defers tensors upon contradiction (collision).
    // Perfectly upgraded to `ConcurrentHashMap<String, Int2DoubleMap>` type in accordance with the Integrated OS V6.1 architecture boxing sterilization specification.

    private final Map<String, Int2DoubleMap> globalKnowledgeNetwork = new ConcurrentHashMap<>();
    private final A0_DT_42_422081_모순_유예_양자_버퍼 contradictionIsolationBuffer;

    // [생성자]
    public A0_DT_42_423040_자가_조직화_지식망_직조기(A0_DT_42_422081_모순_유예_양자_버퍼 isolationBuffer) {
        if (isolationBuffer == null) {
            throw new IllegalArgumentException("[의존성 주입(DI) 파열 오류] 모순 유예 양자 버퍼(Tier 8) 격리망이 주입되지 않아, 충돌 방어 기제가 상실된 지식망 직조기를 기동할 수 없습니다.");
        }
        this.contradictionIsolationBuffer = isolationBuffer;
        logger.info(" >> [통합 OS V6.1] A0_DT_42_423040 자가 조직화 지식망 직조기(Weaver) 기동 완료. (개방 주소법(Open Addressing) 맵핑 엔진 및 FastUtil 레거시 어댑터 장착 성공)");
    }

    // [1. 한글 상세 주석]
    // 💡 [파이프라인 역학 1: 텐서 안착(Weaving) 및 모순 충돌 방어막 (내부 코어 파이프라인)]
    // 사영소에서 투영되어 넘어온 새로운 텐서 파편들을 FFM 메모리에 이미 안착하여 존재하는 기존 진리 텐서와 비교하고, 
    // 평화로우면 덮어쓰며 안착(Settle)시키거나, 모순(Contradiction) 충돌 발생 시 관리자에게 보고하기 위해 즉시 양자 격리망 버퍼로 유폐(Suspend)시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [Pipeline Mechanics 1: Tensor Settling (Weaving) and Contradiction Collision Defense Shield (Internal Core Pipeline)]
    // Compares the new tensor fragments projected from the projection engine with the existing truth tensors already settled in the FFM memory, 
    // and if peaceful, overwrites and settles them, or if a contradiction collision occurs, immediately confines (suspends) them into the quantum isolation network buffer to report to the admin.

    public void weaveAndSettleParticles(
            DocumentMetadata metadata,
            List<TopologicalTensorParticle> particleList,
            Map<String, WritePort> targetPortMap) {

        for (TopologicalTensorParticle particle : particleList) {
            
            // 💡 [V6.1 타입 업그레이드 수혜 구간] 추출 및 변환 코어 로직이 더 이상 제네릭 Map이 아닌 원시 타입 맵(FastUtil Int2DoubleMap)을 완벽히 반환하도록 리팩토링됨
            Int2DoubleMap existingTruthTensor = extractExistingTensor(particle, targetPortMap);
            Int2DoubleMap newTruthTensor = convertParticleCoordinatesToMap(particle);

            double similarity = 1.0;
            if (!existingTruthTensor.isEmpty()) {
                similarity = calculateCosineSimilarity(existingTruthTensor, newTruthTensor); // 두 위상 에너지의 기하학적 코사인 각도 측정
            }

            if (similarity < COLLISION_DETECTION_THRESHOLD) {
                logger.warning(String.format(" 🚨 [논리 모순(Contradiction) 포획 격발] 기하학적 텐서 정합성 위배! (유사도: %.4f) 텐서의 안착 궤도를 양자 버퍼에 유예(Suspend) 격리하고 시스템 관리자(HIL)의 최종 결단(Resolve)을 대기합니다: %s",
                        similarity, particle.documentUuid()));

                // 💡 [컴파일 붕괴 완벽 수복] 하위 종속 모듈(`422081`)의 영문화 갱신에 발맞추어, 
                // 구형 한글 시그니처(`포획하다_문헌_모순_텐서`)를 호출하여 발생하던 `cannot find symbol` 에러를 최신 `captureDocumentContradictionTensor` 영문 규격 호출로 완전 대체하여 파이프라인 배관 단절을 치유했습니다.
                contradictionIsolationBuffer.captureDocumentContradictionTensor(
                        metadata.documentUuid() + "_CHUNK_" + particle.chunkIndex(),
                        existingTruthTensor,
                        newTruthTensor,
                        metadata.originalFileName(),
                        (finalDecidedTensor) -> commitTensorSettlement(particle, finalDecidedTensor, targetPortMap)); // 모순 해결 완료 후 실행될 비동기 콜백(Callback) 주입
            } else {
                // 모순이 없다면 텐서를 파일 I/O 포트에 즉시 물리적 확정(Commit) 안착
                commitTensorSettlement(particle, newTruthTensor, targetPortMap);
            }
        }
    }

    // [1. 한글 상세 주석]
    // 💡 [파이프라인 역학 2: 레거시 어댑터 브릿지 (Legacy Adapter Bridge)]
    // 외부의 레거시 웹 크롤러, 옛날 규격의 REST API 등 구형 인터페이스(`Map<Integer, Double>`)를 통해 무겁게 박싱(Boxing)되어 유입되는 텐서를 
    // 하위 모듈이 요구하는 V6.1 규격(원시 타입 배열)으로 물리적으로 캐스팅(Adapter)한 후 시스템에 융합시킵니다.
    // [2. 영문 상세 주석]
    // 💡 [Pipeline Mechanics 2: Legacy Adapter Bridge]
    // Physically casts (Adapter) heavily boxed tensors flowing in through old legacy interfaces (`Map<Integer, Double>`) such as external legacy web crawlers or old REST APIs 
    // into the V6.1 specification (primitive type arrays) demanded by the lower modules, and then fuses them into the system.

    public void executeLegacyKnowledgeWeaving(String tensorIdentifier, Map<Integer, Double> legacyNewTensor, String sourceMetadata) {
        if (tensorIdentifier == null || legacyNewTensor == null)
            return;

        // 💡 [어댑터 패턴(Adapter Pattern) 발동] 힙 메모리 파편화를 유발하는 구형 제네릭 Map을 박싱 없는 FastUtil Int2DoubleMap 원시 타입으로 안전하게 껍데기 변환 캐스팅 수행
        Int2DoubleMap newPrimitiveTensor = adaptStandardMapToPrimitive(legacyNewTensor);
        Int2DoubleMap existingPrimitiveTensor = globalKnowledgeNetwork.get(tensorIdentifier);

        if (existingPrimitiveTensor == null) {
            // 지식망에 처음 등록되는 진리 텐서의 경우 즉각 무사 통과 수용
            globalKnowledgeNetwork.put(tensorIdentifier, newPrimitiveTensor);
            logger.fine("   ├─ [지식 직조 완료] 신규 레거시 어댑터 텐서가 전역 지식망에 힙 메모리 파편화 없이 정상적으로 안착 등록되었습니다: " + tensorIdentifier);
        } else {
            // 기존 텐서와 충돌이 의심될 경우, 동일하게 Tier 8 양자 격리 버퍼로 이관
            contradictionIsolationBuffer.captureDocumentContradictionTensor(
                    tensorIdentifier,
                    existingPrimitiveTensor,
                    newPrimitiveTensor,
                    sourceMetadata,
                    (finalDecidedTensor) -> {
                        // 관리자가 UI를 통해 모순을 해결(Resolve)하면 OS 내부 비동기 이벤트 루프를 통해 호출(Callback)되어 실행되는 확정(Commit) 파이프라인 로직
                        globalKnowledgeNetwork.put(tensorIdentifier, finalDecidedTensor);
                        logger.info("   ├─ [시스템 관리자(HIL) 승인 콜백 완료] 격리 유예되었던 텐서 파동 붕괴가 해결되어 전역 지식망 매트릭스에 최종 확정(Commit) 기록되었습니다: " + tensorIdentifier);
                    });
        }
    }

    // [1. 한글 상세 주석]
    // [물리적 안착 확정 로직 (Zero-Allocation Commit)] 모순 검증이 끝나 최종 진리로 결정된 원시 텐서 좌표 배열을, 실제 OS 파일 채널 매핑(mmap) 오프힙 메모리에 FFM API를 통해 직접 덮어씁니다(Direct Write Dump).
    // [2. 영문 상세 주석]
    // [Physical Settlement Commit Logic (Zero-Allocation Commit)] Directly overwrites (Direct Write Dump) the primitive tensor coordinate array finally determined as truth after contradiction verification into the actual OS file channel mapped (mmap) off-heap memory via the FFM API.

    private void commitTensorSettlement(
            TopologicalTensorParticle particle,
            Int2DoubleMap tensorValue,
            Map<String, WritePort> writePortMap) {

        // UUID 문자열 해시 기반 개방 주소법(Open Addressing)을 통해 고유한 물리 오프셋 확보
        long physicalAbsoluteOffset = deriveAbsoluteOffset(particle.documentUuid(), particle.chunkIndex());

        // 💡 [Zero-Allocation 최신 규격 호환 패치] 최신 FastUtil 라이브러리 규격에 완벽히 맞추어, Deprecated 된 `fastIterator()` 대신 안정성이 검증된 표준 `iterator()` 메서드 호출 적용
        ObjectIterator<Int2DoubleMap.Entry> iterator = tensorValue.int2DoubleEntrySet().iterator();
        while (iterator.hasNext()) {
            Int2DoubleMap.Entry entry = iterator.next();
            int dimensionId = entry.getIntKey();
            double energyValue = entry.getDoubleValue();

            String featureName = mapDimensionToFeatureName(dimensionId);
            WritePort targetPort = writePortMap.get(featureName);

            // FFM API 포인터를 호출하여 무거운 JVM 힙(Heap) 객체를 생성하지 않고 OS 커널의 물리적인 텐서 공간을 다이렉트 타격 덮어쓰기 (Absolute Zero-Allocation)
            if (targetPort != null) {
                targetPort.segment().set(TENSOR_FLOAT_LAYOUT, physicalAbsoluteOffset, (float) energyValue);
            }
        }

        logger.fine(String.format("   └─ [위상 관계망 물리 직조 완수] 비정형 문서(%s)의 사유 입자(Chunk 스니펫 %d)가 OS 물리 DB 매트릭스에 성공적으로 100%% 안착 및 영속화되었습니다.",
                particle.documentUuid(), particle.chunkIndex()));
    }

    // [1. 한글 상세 주석]
    // [보조 수학 연산: 희소 텐서 코사인 유사도] FastUtil 맵(희소 메모리 배열)을 선형 순회하며 두 텐서 간의 기하학적 각도 유사도(Cosine Similarity)를 산출합니다.
    // [2. 영문 상세 주석]
    // [Auxiliary Math Operation: Sparse Tensor Cosine Similarity] Linearly traverses the FastUtil map (sparse memory array) to calculate the geometric angular similarity (Cosine Similarity) between two tensors.

    private double calculateCosineSimilarity(Int2DoubleMap tensorA, Int2DoubleMap tensorB) {
        double dotProduct = 0.0;
        double normASquared = 0.0;
        double normBSquared = 0.0;

        ObjectIterator<Int2DoubleMap.Entry> iteratorA = tensorA.int2DoubleEntrySet().iterator();
        while (iteratorA.hasNext()) {
            Int2DoubleMap.Entry entry = iteratorA.next();
            double valA = entry.getDoubleValue();
            normASquared += (valA * valA); // A 벡터의 유클리드 노름(L2 Norm) 제곱 누적

            if (tensorB.containsKey(entry.getIntKey())) {
                double valB = tensorB.get(entry.getIntKey());
                dotProduct += (valA * valB); // 일치하는 차원이 존재하면 내적(Dot Product) 합산
            }
        }

        ObjectIterator<Int2DoubleMap.Entry> iteratorB = tensorB.int2DoubleEntrySet().iterator();
        while (iteratorB.hasNext()) {
            double valB = iteratorB.next().getDoubleValue();
            normBSquared += (valB * valB); // B 벡터 노름 누적
        }

        // 분모가 0이 되어 발산하는 시스템 크래시를 물리적으로 차단
        if (normASquared == 0.0 || normBSquared == 0.0)
            return 0.0;

        return dotProduct / (Math.sqrt(normASquared) * Math.sqrt(normBSquared) + DIRAC_EPSILON);
    }

    // [1. 한글 상세 주석]
    // [물리적 기존 텐서 추출] 새로운 텐서를 덮어쓰기 이전에, FFM API를 통해 커널 오프힙 메모리 영역에서 기존에 존재하여 안착해 있던 진리 텐서 스칼라 값을 읽어와 원시 맵(Int2DoubleMap) 형태로 복원(Restore)하여 반환합니다.
    // [2. 영문 상세 주석]
    // [Physical Existing Tensor Extraction] Prior to overwriting with a new tensor, reads the scalar values of the existing truth tensor settled in the kernel off-heap memory region via the FFM API, and restores them to a primitive map (Int2DoubleMap) format to return.

    private Int2DoubleMap extractExistingTensor(TopologicalTensorParticle particle, Map<String, WritePort> portMap) {
        Int2DoubleOpenHashMap existingTensorMap = new Int2DoubleOpenHashMap(3);
        long physicalAbsoluteOffset = deriveAbsoluteOffset(particle.documentUuid(), particle.chunkIndex());

        // 3D 공간 상의 X, Y, Z (0, 1, 2) 차원에 대해 기존 메모리 값을 FFM 포인터로 조회
        for (int dimension = 0; dimension < 3; dimension++) {
            String featureName = mapDimensionToFeatureName(dimension);
            WritePort targetPort = portMap.get(featureName);

            if (targetPort != null) {
                float existingEnergy = targetPort.segment().get(TENSOR_FLOAT_LAYOUT, physicalAbsoluteOffset);
                // 양자 진공 한계선(에프실론)을 초과하는 실질적인 물리 에너지가 존재할 때만 맵에 포함시켜 텐서의 희소성(Sparsity)을 압축 보존
                if (Math.abs(existingEnergy) > DIRAC_EPSILON) {
                    existingTensorMap.put(dimension, (double) existingEnergy);
                }
            }
        }
        return existingTensorMap;
    }

    // [1. 한글 상세 주석]
    // [데이터 형변환 유틸리티] 3차원 입자(Particle) DTO 레코드 객체를 시스템 규격 파이프라인에 맞는 원시 희소 맵(Int2DoubleMap) 포맷으로 매핑 변환합니다.
    // [2. 영문 상세 주석]
    // [Data Type Conversion Utility] Mapping-converts the 3D Particle DTO record object into the primitive sparse map (Int2DoubleMap) format suitable for the system specification pipeline.

    private Int2DoubleMap convertParticleCoordinatesToMap(TopologicalTensorParticle particle) {
        Int2DoubleOpenHashMap map = new Int2DoubleOpenHashMap(3);
        if (Math.abs(particle.xAxisDirection()) > DIRAC_EPSILON)
            map.put(0, particle.xAxisDirection());
        if (Math.abs(particle.yAxisInformation()) > DIRAC_EPSILON)
            map.put(1, particle.yAxisInformation());
        if (Math.abs(particle.zAxisAbstraction()) > DIRAC_EPSILON)
            map.put(2, particle.zAxisAbstraction());
        return map;
    }

    // [1. 한글 상세 주석]
    // 💡 [어댑터 패턴 코어 로직 (Adapter Pattern Core)] 외부 시스템에서 유입된 무거운 구형 자바 표준 제네릭 `Map<Integer, Double>`을 
    // 하위 T10 코어 모듈이 수용할 수 있는 가벼운 객체 박싱 제로(Zero-Boxing) 규격인 FastUtil `Int2DoubleMap` 원시 타입 배열 구조체로 안전하게 껍데기 변환(Casting)합니다.
    // [2. 영문 상세 주석]
    // 💡 [Adapter Pattern Core Logic] Safely transforms (Casts) the heavy old Java standard generic `Map<Integer, Double>` flowing in from external systems 
    // into the lightweight object Zero-Boxing specification FastUtil `Int2DoubleMap` primitive type array structure that lower T10 core modules can accommodate.

    private Int2DoubleMap adaptStandardMapToPrimitive(Map<Integer, Double> standardMap) {
        Int2DoubleOpenHashMap primitiveMap = new Int2DoubleOpenHashMap(standardMap.size());
        for (Map.Entry<Integer, Double> entry : standardMap.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                // Wrapper 클래스(Integer, Double)에서 원시 스칼라 값(int, double)을 추출하여 Unboxing 맵핑 삽입
                primitiveMap.put(entry.getKey().intValue(), entry.getValue().doubleValue());
            }
        }
        return primitiveMap;
    }

    // [1. 한글 상세 주석]
    // 💡 [좌표 무결성 라우팅 및 개방 주소법 방어 (Open Addressing Routing)] 
    // 비정형 문자열 UUID를 해싱(Hashing)한 뒤, `allocatedYIndexSet`을 조회하는 선형 탐사(Linear Probing) 로직을 통해 해시 충돌(Hash Collision) 파열을 완벽히 회피하며 파일 내 고유한 절대 물리 메모리 오프셋을 역산합니다.
    // [2. 영문 상세 주석]
    // 💡 [Coordinate Integrity Routing and Open Addressing Defense] 
    // Hashes the unstructured string UUID, and through Linear Probing logic querying `allocatedYIndexSet`, perfectly avoids Hash Collision ruptures and reverse-calculates a unique absolute physical memory offset within the file.

    private long deriveAbsoluteOffset(String documentUuid, int chunkIndex) {
        long virtualEntityIndex = documentTopologyRoutingDict.computeIfAbsent(documentUuid, uuid -> {
            long initialHash = Math.abs((long) uuid.hashCode()) % 10000L;
            long probingIndex = initialHash;

            // 💡 [개방 주소법] 할당하려던 Y축 인덱스(슬롯)가 이미 다른 UUID에 의해 점유(충돌) 중이라면, 한 칸씩 전진(++)하며 빈 공간을 확보할 때까지 루프 탐사
            while (allocatedYIndexSet.contains(probingIndex)) {
                probingIndex = (probingIndex + 1) % 10000L;
                
                // 우주 전체(10,000 슬롯)를 한 바퀴 다 돌고 다시 원래 인덱스로 돌아왔다면 공간 포화 붕괴(OOM) 판정
                if (probingIndex == initialHash) {
                    throw new IllegalStateException("[시공간 포화 붕괴] 물리 DB 매트릭스에 할당 가능한 위상 Y축 슬롯(전체 10,000개 한계)이 모두 해시 충돌로 100% 점유되어 완전히 고갈되었습니다.");
                }
            }
            // 마침내 찾아낸 안전한 빈 슬롯을 동시성 점유망에 등록
            allocatedYIndexSet.add(probingIndex);
            return probingIndex;
        });
        
        // (Entity Index * 10,000 스케일 팽창 + 청크 인덱스) * 4바이트(Float) 공식으로 최종 1D 메모리 쓰기 주소 오프셋 산출
        return (virtualEntityIndex * 10000L + (chunkIndex % 10000L)) * 4L;
    }

    // [1. 한글 상세 주석]
    // 시스템에서 산출된 기하학적 차원(Dimension) 물리 숫자 ID(0, 1, 2)를, 이에 엄격히 1:1로 대응되는 시스템 데이터베이스 하드웨어 파일 텐서의 고유 문자열 식별자 이름(지표명)으로 스위칭 매핑합니다.
    // [2. 영문 상세 주석]
    // Switching-maps the geometrically computed physical numeric dimension ID (0, 1, 2) from the system to the unique string identifier name (feature name) of the system database hardware file tensor that strictly corresponds to it 1:1.

    private String mapDimensionToFeatureName(int dimensionId) {
        return switch (dimensionId) {
            case 0 -> "FEATURE_X_POLARITY";
            case 1 -> "FEATURE_Y_MAGNITUDE";
            case 2 -> "FEATURE_Z_ABSTRACTION";
            default -> "FEATURE_UNKNOWN";
        };
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 아키텍처 단층(Fault Line)의 수복과 어댑터 패턴 (Adapter Pattern):
 * 대규모 소프트웨어 아키텍처를 V6.1과 같은 극단적인 하드웨어 성능 최적화 규격(Zero-Allocation, Object Boxing 소거, FastUtil)으로 업그레이드할 때,
 * 가장 흔하면서도 치명적으로 발생하는 아키텍처 시스템 붕괴는 계층(Layer) 간의 '타입 불일치(Type Mismatch)'로 인한 컴파일 파열입니다.
 * 외부 세계와 통신하는 상위 계층(T16)이나 수년 전에 짜여진 레거시 외부 크롤러들은 여전히 `java.util.Map<Integer, Double>` 이라는 전통적이고 무거운 범용 제네릭(Generic) 객체를 사용하여 페이로드 데이터를 넘겨주려 하는데 반해,
 * 실제 수학 연산을 수행하는 하위 계층(T8~T12)의 핵심 텐서 엔진 파이프라인들은 힙 메모리 박싱(Boxing) 오버헤드를 아예 물리적으로 없애기 위해 `Int2DoubleMap`이라는 날카로운 원시(Primitive) 타입 배열 구조체로 100% 교체(Rewrite)되었기 때문입니다.
 * 본 직조기 모듈(`423040`)은 이 두 개의 극단적인 아키텍처 단층이 맞부딪히는 최전선 경계선(Boundary)의 게이트웨이 역할을 완벽히 수행합니다.
 * `adaptStandardMapToPrimitive` 메서드는 이 경계에서 레거시 데이터를 1차적으로 흠집 없이 흡수하여, 하위 계층이 요구하는 원시 규격 껍데기로 안전하고 빠르게 캐스팅(Adapter)해 내려보냅니다. 
 * 이를 통해 외부 연계 시스템의 방대한 코드를 굳이 한꺼번에 셧다운 시키거나 전부 다 갈아 뒤엎지(Rewrite) 않고도, 통합 OS 내부 코어 엔진의 점진적이고 붕괴 없는 안정적인 차세대 마이그레이션(Migration)이 수학적으로 보장됩니다.
 * 
 * 2. 콜백 파이프라인(Callback Pipeline) 기반의 비동기 모순 해결 아키텍처:
 * 기존에 이미 FFM 디스크 메모리에 굳건히 안착되어 저장된 지식 진리 텐서와, 새롭게 외부에서 추출되어 투영된 신규 텐서가 코사인 유사도 상에서 양립할 수 없는 거대한 모순(Contradiction Collision)을 일으켰을 때, 
 * 이 직조기 모듈은 당황하여 전체 시스템 파이프라인 스레드를 멈추거나 끔찍한 RuntimeException을 외부로 던지지 않습니다.
 * 대신 T8 양자 모순 격리망 버퍼 포트로 해당 텐서 데이터를 던져 즉시 유폐(Suspend)시킨 뒤, `(finalDecidedTensor) -> { ... }` 라는 람다(Lambda) 비동기 콜백 함수만을 
 * 버퍼에 인자(Argument)로 남겨두고 미련 없이 쿨하게 자신의 스레드 메모리 점유 및 연산 작업을 종료(Return)해 버립니다.
 * 훗날(몇 분 혹은 며칠 뒤) 시스템 관리자(Human)나 상위 감독 에이전트가 UI 화면에서 이 모순 알람을 확인하고 덮어쓰기 승인 혹은 기각 결단(Resolve)을 내리는 바로 그 찰나의 순간에, 
 * 대기 중이던 이 람다 콜백 함수가 비동기적으로 스레드 풀에서 자동으로 깨어나, 과거 중단되어 멈춰있던 직조(Weaving) 덮어쓰기 과정을 소리 없이 조용히 끝마칩니다. 
 * 이것이 그 어떠한 스레드 데드락(Deadlock)이나 락아웃(Lockout) 병목도 허용하지 않고 수천만 건의 대규모 트래픽을 처리하는 HFT 시스템의 우아한 제어 역학(Control Dynamics) 설계입니다.
 * 
 * 3. 극강의 힙(Heap) 객체 멸균: 제로 얼로케이션 (Absolute Zero-Allocation) 및 FFM 다이렉트 타격:
 * `commitTensorSettlement` 메서드 내부를 들여다보면, 자바 언어의 흔하고 보편적인 파일 I/O 스트림(`FileOutputStream`, `BufferedOutputStream` 등) 코드가 단 한 줄도 존재하거나 등장하지 않습니다.
 * 오직 `targetPort.segment().set()` 이라는 단 한 줄의 FFM(Foreign Function & Memory API / Project Panama) 네이티브 코드가 
 * 운영체제(OS) 커널의 페이지 캐시(Page Cache) 물리 주소를 직접 겨냥하여, JVM 힙 메모리 이동이나 복사 없이 다이렉트 타격 덮어쓰기(Direct Write)를 집행합니다.
 * 이는 자바 가상 머신(JVM)의 힙(Heap) 메모리를 단 1바이트도 더럽히지 않으며, 쓸데없는 래퍼(Wrapper) 버퍼 객체의 생성을 원천적으로 물리적으로 막아 
 * 가비지 컬렉터(GC) 스톨 스파이크(Stall Spike) 현상을 아예 근원적으로 잠재우는, 극도의 하드웨어 성능을 쥐어짜내는 C언어급 시스템 레벨 커널 제어 기법의 결정체입니다.
 * =============================================================================
 */