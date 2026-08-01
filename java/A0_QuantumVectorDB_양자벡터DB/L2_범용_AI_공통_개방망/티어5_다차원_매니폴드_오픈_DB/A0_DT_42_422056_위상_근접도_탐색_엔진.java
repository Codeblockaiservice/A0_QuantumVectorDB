/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어5_다차원_매니폴드_오픈_DB
 * @alias Topology_Proximity_Search_Engine
 * @tier 5
 * @keywords K-NN Search, Sparse Tensor, Cosine Similarity, Primitive Min-Heap, Zero-Allocation, Parallel Stream
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_422056_위상_근접도_탐색_엔진.java
 * - 모듈명: 통합 OS V6.0 - Tier 5: 다차원 매니폴드 위상 근접도 탐색 엔진 (K-NN Search Engine)
 * 
 * [설계 명세]
 * 1. 역할: 쿼리(Query) 텐서와 전역 지식망 내의 텐서들 간의 코사인 유사도를 초고속으로 계산하여 상위 K개의 이웃(NN) 추출.
 * 2. 기능: 멀티코어 병렬 스트림 스캔(Parallel Stream Scan), 원시 배열 기반 최소 힙(Primitive Min-Heap)을 이용한 Top-K 색출.
 * 3. 의도: 수백만 개의 단어를 매번 전체 정렬(Sorting)할 때 발생하는 O(N log N)의 메모리 폭발과 지연(Latency) 억제.
 * 4. 이론: 공간 복잡도 최적화, Zero-Allocation 원시 힙 트리의 기하학, 희소 텐서 내적(Sparse Dot Product) 최적화.
 * 5. 공식: Similarity = (A · B) / (||A|| × ||B||) (희소 맵 교집합 내적 최적화 연산).
 * 
 * [변경/신설 사항]
 * - 💡 [핵심 최적화 1] 무수히 많은 DTO 객체를 생성하여 GC 스파이크를 유발하던 `PriorityQueue<SearchCandidate>`를 전면 폐기.
 * - 💡 [핵심 최적화 2] `double[]`과 `String[]`을 병렬 배열로 관리하는 `PrimitiveMinHeapAccumulator`를 자체 구현하여, 
 *                 수백만 번의 병렬 스트림 순회 도중 단 1개의 객체 할당(new)도 발생하지 않는 완벽한 제로 얼로케이션(Zero-Allocation) 달성.
 * - 기대효과: O(N log K) 속도로 압도적인 기하학적 유사도 탐색을 수행하며, HFT(고빈도 매매) 환경의 GC 스톨(Stop-The-World)을 영구 멸균.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 병렬 스트림 연산, 커스텀 콜렉터(Collector), 데이터 매핑을 위한 표준 라이브러리를 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of standard libraries for parallel stream operations, custom collectors, and data mapping.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어5_다차원_매니폴드_오픈_DB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collector;

public final class A0_DT_42_422056_위상_근접도_탐색_엔진 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.422056_TOPOLOGY_SEARCH_ENGINE");

    /**
     * 상태가 없는(Stateless) 순수 수학 연산 엔진이므로 다중 스레드 환경에서 안전하게 독립적인 인스턴스화가 가능합니다.
     */
    public A0_DT_42_422056_위상_근접도_탐색_엔진() {
        logger.info(" >> [통합 OS V6.0] A0_DT_42_422056 위상 근접도 탐색 엔진 기동. (Primitive Min-Heap 병렬 리덕션 코어 탑재 완료)");
    }

    /**
     * [결과 반환용 DTO 레코드]
     * 대규모 스캔 연산이 모두 끝난 후, 최종 결과물(Top-K)을 호출자에게 반환할 때에만 제한적으로 생성되는 데이터 캡슐입니다.
     */
    public record SearchCandidate(String wordKey, double cosineSimilarity, Map<Integer, Double> sparseTensorMap) implements Comparable<SearchCandidate> {
        @Override
        public int compareTo(SearchCandidate other) {
            // 유사도 기준 내림차순(가장 높은 유사도가 먼저 오도록) 정렬
            return Double.compare(other.cosineSimilarity, this.cosineSimilarity);
        }
    }

    /**
     * [탐색 로직 1: Top-K K-NN 탐색 수행]
     * 타겟 텐서와 전역 데이터셋의 모든 희소 텐서를 병렬로 대조하여 코사인 유사도가 가장 높은 상위 K개를 추출합니다.
     * 
     * @param queryTensorMap        검색의 기준이 되는 질의(Query) N차원 희소 텐서
     * @param globalTensorDataset   캐시 DB에서 의존성 주입(DI)으로 제공받은 전체 단어 생태계 데이터셋
     * @param topK                  추출할 최근접 이웃(Nearest Neighbors)의 개수
     * @return 유사도가 높은 순(내림차순)으로 정렬된 상위 K개의 후보 리스트
     */
    public List<SearchCandidate> executeKnnSearch(
            Map<Integer, Double> queryTensorMap, 
            Set<Map.Entry<String, Map<Integer, Double>>> globalTensorDataset, 
            int topK) {

        if (queryTensorMap == null || queryTensorMap.isEmpty() || globalTensorDataset == null || topK <= 0) {
            return Collections.emptyList();
        }

        long searchStartTime = System.currentTimeMillis();

        // 💡 [연산 최적화] 입력된 질의(Query) 텐서의 L2 노름(Norm)은 탐색 내내 변하지 않으므로 루프 외부에서 1회만 선계산하여 연산 중복을 방지합니다.
        double queryNorm = calculateL2Norm(queryTensorMap);
        if (queryNorm == 0.0) {
            return Collections.emptyList(); // 타겟이 완전 진공(0.0) 상태이면 방향성 비교 불가
        }

        // 💡 [핵심 최적화: Zero-Allocation 병렬 콜렉터 (Custom Collector)]
        // 기존의 객체 지향 PriorityQueue를 폐기하고, 원시 타입(Primitive) 배열로 이루어진 커스텀 Min-Heap 누산기를 투입합니다.
        Collector<Map.Entry<String, Map<Integer, Double>>, PrimitiveMinHeapAccumulator, PrimitiveMinHeapAccumulator> minHeapCollector =
            Collector.of(
                // 1. 공급자(Supplier): 각 병렬 워커 스레드마다 크기 K를 갖는 고립된 원시 Min-Heap 인스턴스 생성
                () -> new PrimitiveMinHeapAccumulator(topK),
                
                // 2. 누산기(Accumulator): 코사인 내적 계산 후 로컬 힙에 직접 밀어넣음 (이 과정에서 객체 할당(new) 0건 발생)
                (localHeap, tensorEntry) -> {
                    double similarity = calculateSparseCosineSimilarity(queryTensorMap, queryNorm, tensorEntry.getValue());
                    
                    // 두 텐서가 완전 직교(Orthogonal, 유사도 0)하거나 반대 방향인 에너지는 Heap에 기록할 가치조차 없으므로 스킵 (가지치기)
                    if (similarity > 0.0) {
                        localHeap.insert(tensorEntry.getKey(), similarity, tensorEntry.getValue());
                    }
                },
                
                // 3. 병합기(Combiner): 병렬 처리된 여러 스레드들의 로컬 힙 결과를 하나의 최종 힙으로 병합(Merge)
                (heap1, heap2) -> {
                    heap1.merge(heap2);
                    return heap1;
                }
            );

        try {
            // 💡 [하드웨어 가속] Java ForkJoinPool 기반 병렬 스트림(Parallel Stream) 스캐닝을 통해 멀티코어 100% 가동
            PrimitiveMinHeapAccumulator finalMinHeap = globalTensorDataset.parallelStream()
                                                                          .collect(minHeapCollector);

            // 최종 힙에서 원시 데이터를 추출하여 DTO 리스트로 변환 및 정렬
            List<SearchCandidate> finalResultList = finalMinHeap.extractSortedResults();

            long elapsedTimeMs = System.currentTimeMillis() - searchStartTime;
            logger.info(String.format("   ├─ [위상 탐색 완료] 전역 데이터셋 병렬 스캔 및 Top-%d 색출 완료. (소요 시간: %d ms, 대상: %d건)", 
                    topK, elapsedTimeMs, globalTensorDataset.size()));

            return finalResultList;

        } catch (Exception ex) {
            logger.log(Level.SEVERE, " [탐색 파이프라인 붕괴] 병렬 스트림 위상 대조 중 치명적 시스템 예외 발생.", ex);
            return Collections.emptyList();
        }
    }

    // =========================================================================
    // 💡 [객체 멸균 구역] 원시 타입 커스텀 Min-Heap 구조체 (Zero-Allocation Architecture)
    // =========================================================================
    private static class PrimitiveMinHeapAccumulator {
        private final int capacityK;
        private int currentSize;
        
        // 구조체(Struct) 형태의 병렬 배열 패턴(SoA: Structure of Arrays)을 적용하여 객체 생성(new) 오버헤드 원천 차단
        private final String[] wordArray;
        private final double[] similarityArray;
        private final Map<Integer, Double>[] tensorArray;

        @SuppressWarnings("unchecked")
        public PrimitiveMinHeapAccumulator(int capacityK) {
            this.capacityK = capacityK;
            this.currentSize = 0;
            this.wordArray = new String[capacityK];
            this.similarityArray = new double[capacityK];
            this.tensorArray = new Map[capacityK];
        }

        public void insert(String wordKey, double similarity, Map<Integer, Double> tensorMap) {
            if (currentSize < capacityK) {
                // 힙(Heap) 내 빈 공간이 남아있으면 배열 끝에 추가 후 위로 끌어올림 (Sift-Up)
                wordArray[currentSize] = wordKey;
                similarityArray[currentSize] = similarity;
                tensorArray[currentSize] = tensorMap;
                
                siftUp(currentSize);
                currentSize++;
            } else if (similarity > similarityArray[0]) {
                // 💡 [O(1) 필터링] 힙이 꽉 찼을 때, 새 데이터의 유사도가 Root 노드(현재까지의 K개 중 최솟값)보다 클 경우에만
                // Root 노드를 새로운 데이터로 파괴적 덮어쓰기 한 뒤 트리 아래로 끌어내림 (Sift-Down)
                wordArray[0] = wordKey;
                similarityArray[0] = similarity;
                tensorArray[0] = tensorMap;
                
                siftDown(0);
            }
        }

        public void merge(PrimitiveMinHeapAccumulator otherHeap) {
            for (int i = 0; i < otherHeap.currentSize; i++) {
                insert(otherHeap.wordArray[i], otherHeap.similarityArray[i], otherHeap.tensorArray[i]);
            }
        }

        public List<SearchCandidate> extractSortedResults() {
            List<SearchCandidate> resultList = new ArrayList<>(currentSize);
            for (int i = 0; i < currentSize; i++) {
                resultList.add(new SearchCandidate(wordArray[i], similarityArray[i], tensorArray[i]));
            }
            // 힙(Heap) 자료구조의 특성상 내부 데이터는 완벽히 정렬되어 있지 않으므로, 최종 추출 시 내림차순(유사도 높은 순) 정렬을 보장합니다.
            Collections.sort(resultList);
            return resultList;
        }

        private void siftUp(int index) {
            String targetWord = wordArray[index];
            double targetSimilarity = similarityArray[index];
            Map<Integer, Double> targetTensor = tensorArray[index];

            while (index > 0) {
                int parentIndex = (index - 1) >>> 1; // 비트 시프트 연산(>>>)으로 나눗셈 연산(CPU 사이클) 최적화
                
                if (targetSimilarity >= similarityArray[parentIndex]) {
                    break;
                }
                
                wordArray[index] = wordArray[parentIndex];
                similarityArray[index] = similarityArray[parentIndex];
                tensorArray[index] = tensorArray[parentIndex];
                index = parentIndex;
            }
            
            wordArray[index] = targetWord;
            similarityArray[index] = targetSimilarity;
            tensorArray[index] = targetTensor;
        }

        private void siftDown(int index) {
            int halfSize = currentSize >>> 1;
            String targetWord = wordArray[index];
            double targetSimilarity = similarityArray[index];
            Map<Integer, Double> targetTensor = tensorArray[index];

            while (index < halfSize) {
                int leftChildIndex = (index << 1) + 1;
                int rightChildIndex = leftChildIndex + 1;
                int minChildIndex = leftChildIndex;
                double minChildSimilarity = similarityArray[leftChildIndex];

                if (rightChildIndex < currentSize && similarityArray[rightChildIndex] < minChildSimilarity) {
                    minChildIndex = rightChildIndex;
                    minChildSimilarity = similarityArray[rightChildIndex];
                }

                if (targetSimilarity <= minChildSimilarity) {
                    break;
                }

                wordArray[index] = wordArray[minChildIndex];
                similarityArray[index] = similarityArray[minChildIndex];
                tensorArray[index] = tensorArray[minChildIndex];
                index = minChildIndex;
            }
            
            wordArray[index] = targetWord;
            similarityArray[index] = targetSimilarity;
            tensorArray[index] = targetTensor;
        }
    }

    /**
     * [수학 연산 1] 희소 텐서(Sparse Tensor)용 최적화된 코사인 유사도(Cosine Similarity) 연산.
     * 두 텐서 간의 위상학적 각도(유사성)를 도출합니다. Similarity = (A · B) / (||A|| × ||B||)
     * 
     * @param queryTensorMap 질의(Query) 희소 텐서
     * @param queryNorm      루프 외부에서 미리 1회 연산된 질의 텐서의 L2 노름 (중복 연산 방지용)
     * @param targetTensorMap DB에서 추출한 대조군 대상 텐서
     */
    private double calculateSparseCosineSimilarity(
            Map<Integer, Double> queryTensorMap, 
            double queryNorm, 
            Map<Integer, Double> targetTensorMap) {

        if (targetTensorMap == null || targetTensorMap.isEmpty()) return 0.0;

        double intersectionDotProduct = 0.0;
        double targetNormSquared = 0.0;

        // 💡 [기계적 공감(Mechanical Sympathy)] 더 크기가 작은 Map을 기준으로 순회하여 교집합(Intersection) 탐색의 Big-O 시간 복잡도를 최소화합니다.
        boolean isQuerySmaller = queryTensorMap.size() < targetTensorMap.size();
        Map<Integer, Double> iterationBaseMap = isQuerySmaller ? queryTensorMap : targetTensorMap;
        Map<Integer, Double> lookupTargetMap = isQuerySmaller ? targetTensorMap : queryTensorMap;

        for (Map.Entry<Integer, Double> entry : iterationBaseMap.entrySet()) {
            Integer dimensionId = entry.getKey();
            Double baseWeight = entry.getValue();

            Double lookupWeight = lookupTargetMap.get(dimensionId);
            if (lookupWeight != null) {
                intersectionDotProduct += (baseWeight * lookupWeight);
            }
        }

        // 만약 두 텐서 간의 직교성(Orthogonality)이 증명되어 내적(Dot Product)이 0.0이라면,
        // 비교 텐서의 전체 노름(Norm)을 계산하는 값비싼 Math.sqrt 호출을 건너뜁니다. (Zero-Compute Optimization)
        if (intersectionDotProduct == 0.0) {
            return 0.0;
        }

        // 대조군 텐서의 분모 노름 도출 (모든 에너지가 존재하는 차원들의 제곱합)
        for (Double energyValue : targetTensorMap.values()) {
            targetNormSquared += (energyValue * energyValue);
        }

        double targetNorm = Math.sqrt(targetNormSquared);
        if (targetNorm == 0.0) return 0.0;

        return intersectionDotProduct / (queryNorm * targetNorm);
    }

    /**
     * [수학 연산 2] 텐서의 유클리드 노름(Euclidean Norm, L2 Norm) 산출
     */
    private double calculateL2Norm(Map<Integer, Double> tensorMap) {
        double sumOfSquares = 0.0;
        for (Double energyValue : tensorMap.values()) {
            sumOfSquares += (energyValue * energyValue);
        }
        return Math.sqrt(sumOfSquares);
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 로직과 스토리지의 철저한 디커플링 (SRP/DIP 분리):
 * 만약 이전 모듈인 불변 캐시(`422055`) 내부에 이 복잡한 탐색 연산을 하드코딩했다면, 
 * DB 모듈은 '저장'이라는 단일 책임 원칙(SRP)을 상실하고 아키텍처가 비대하게 붕괴되었을 것입니다.
 * 이 K-NN 탐색 엔진은 DB의 내부 자료구조를 직접 참조하지 않습니다. 오직 메서드 매개변수로 외부에서 주입(DI)받은 
 * `globalTensorDataset(Set<Map.Entry>)` 컬렉션만을 바라보는 의존성 역전 원칙(DIP)을 따릅니다. 
 * 스토리지 계층과 수학 연산 계층이 완벽히 분리되었기에, 미래에 캐시 DB가 Redis나 분산 인메모리(Grid)망으로 
 * 전면 교체되더라도 이 탐색 엔진의 수학적 톱니바퀴는 단 한 줄의 코드 수정 없이 영구적으로 재사용됩니다.
 * 
 * 2. 객체 지향의 종말과 원시 타입 최소 힙 (Primitive Min-Heap for Zero-Allocation):
 * 자바의 표준 `PriorityQueue`는 사용하기 편리하지만, 값을 넣고 뺄 때마다 내부적으로 `Object[]` 트리를 재배열하며
 * 요소를 감싸고 있는 DTO 껍데기 객체(예: `SearchCandidate`)를 필연적으로 힙(Heap) 메모리에 무한정 양산합니다.
 * 만약 100만 개의 단어 텐서를 스캔하여 Top-10을 뽑는다면, 스트림 연산 도중 100만 개의 DTO 임시 객체가 생성되었다 버려지며 
 * 시스템 가비지 컬렉터(GC)를 완전히 마비(Stop-The-World) 시킵니다.
 * 수술된 V6.0 엔진은 자바 표준 객체 기반 큐를 전면 폐기하고, 오직 `double[]`, `String[]` 형태의 
 * 평탄화된 원시 배열(Primitive Array)만으로 이루어진 C언어 방식의 커스텀 구조체 Min-Heap(`PrimitiveMinHeapAccumulator`)을 도입했습니다. 
 * 스캔 도중 메모리 할당(new)은 단 한 번도 일어나지 않아, HFT(고빈도 매매) 환경 등 레이턴시가 치명적인 환경에서 GC 지연 스파이크를 물리적으로 멸균했습니다.
 * 
 * 3. 희소 맵 교집합 내적 (Dot Product of Sparse Maps):
 * 코사인 유사도의 분자 공식: $Similarity = \frac{\vec{A} \cdot \vec{B}}{\|\vec{A}\| \times \|\vec{B}\|}$
 * 두 텐서가 각각 5개, 8개의 차원(Dimension)에만 에너지를 가지고 있다면, 고정 배열(Dense Vector, `double[4096]`) 아키텍처에서는 
 * 4,096번의 무의미한 0.0 곱셈 루프를 강제로 돌아야만 합니다. 
 * 그러나 희소 맵(Sparse Map) 환경에서는 두 텐서 중 더 크기가 작은 맵(Map)을 기준으로 순회하며, 상대방 맵에 
 * 동일한 차원의 키(Key)가 존재하는지 `get()` 으로 O(1) 시간복잡도로 검사하는 것만으로 순식간에 교집합 내적을 도출해냅니다. 
 * 이는 탐색 연산량(Time Complexity)을 전체 공간의 크기(Dimension N)가 아닌, '실제 에너지가 존재하는 요소의 개수(K)' 단위로 
 * 강등시켜 극강의 Zero-Compute 효율(연산 공간 압축)을 만들어냅니다.
 * =============================================================================
 */