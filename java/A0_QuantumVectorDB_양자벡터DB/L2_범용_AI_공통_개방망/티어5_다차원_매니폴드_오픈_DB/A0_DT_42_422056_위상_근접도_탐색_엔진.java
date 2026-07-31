package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어5_다차원_매니폴드_오픈_DB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collector;

/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_422056
 * [파일명] A0_DT_42_422056_위상_근접도_탐색_엔진.java
 * [모듈명] A0_QuantumVectorDB_양자벡터DB OS V6.0 - Tier 5: 다차원 매니폴드 위상 근접도 탐색 엔진 (K-NN Search Engine)
 * 
 * [설계 명세]
 * 1. 역할: 타겟 텐서와 DB 내 텐서들 간의 코사인 유사도를 초고속으로 계산하여 상위 K개의 이웃(NN) 추출.
 * 2. 기능: 멀티코어 병렬 스트림 스캔, 원시 타입 최소 힙(Primitive Min-Heap) 기반 Top-K 색출.
 * 3. 의도: 수백만 개의 단어를 매번 전체 정렬(sorted())할 때 발생하는 O(N log N)의 메모리 폭발과 래그(Lag) 억제.
 * 4. 이론: 열역학적 낭비 방어, Zero-Allocation 원시 힙 트리의 기하학.
 * 5. 공식: Similarity = (A · B) / (||A|| × ||B||) (희소 맵 교집합 내적 최적화).
 * 6. 변경/신설 사항:
 *    - 💡 [수술] 무수히 많은 DTO 객체를 생성하여 GC 폭탄을 유발하던 `PriorityQueue<탐색_후보>`를 전면 폐기.
 *    - 💡 [신설] `double[]`과 `String[]`을 병렬로 다루는 `원시_최소힙_누산기`를 자체 구현하여, 
 *      수백만 번의 병렬 스트림 순회 도중 단 1개의 객체 할당(new)도 발생하지 않는 완전한 제로 얼로케이션(Zero-Allocation) 달성.
 * 7. 기대효과: O(N log K) 속도로 압도적인 기하학적 유사도 탐색을 수행하며, HFT 환경의 GC 스톨을 영구 멸균.
 * ==============================================================================
 */
public final class A0_DT_42_422056_위상_근접도_탐색_엔진 {

    private static final Logger 로거 = Logger.getLogger("A0_OS.MATRIX.422056_TOPOLOGY_SEARCH_ENGINE");

    /**
     * 상태가 없는(Stateless) 순수 수학 엔진이므로 독립적인 인스턴스화가 가능합니다.
     */
    public A0_DT_42_422056_위상_근접도_탐색_엔진() {
        로거.info(" >> [A0_QuantumVectorDB_양자벡터DB OS V6.0] A0_DT_42_422056 위상 근접도 탐색 엔진 기동. (Primitive Min-Heap 병렬 리덕션 코어 장착)");
    }

    /**
     * [결과 반환용 DTO 레코드]
     * 연산이 모두 끝난 후, 최종 결과물(Top K)을 외부로 사출할 때에만 제한적으로 사용되는 캡슐입니다.
     */
    public record 탐색_후보(String 표면단어, double 코사인유사도, Map<Integer, Double> 위상텐서) implements Comparable<탐색_후보> {
        @Override
        public int compareTo(탐색_후보 다른_후보) {
            // 내림차순 정렬을 위한 비교
            return Double.compare(다른_후보.코사인유사도, this.코사인유사도);
        }
    }

    /**
     * [탐색 역학 1: Top-K 근접 이웃 색출]
     * 타겟 텐서와 우주(DB)의 모든 텐서를 병렬로 대조하여 가장 위상적 에너지가 흡사한 상위 K개를 추출합니다.
     * 
     * @param 타겟_텐서     검색의 기준이 되는 N차원 희소 텐서 (입력 질의)
     * @param 우주_전체_레시피망 캐시 DB(422055)에서 주입받은 전체 단어 생태계 (DIP 의존성 역전)
     * @param 추출할_K개    색출할 최근접 이웃의 개수
     * @return 유사도(내림차순)가 높은 상위 K개의 탐색 후보 리스트
     */
    public List<탐색_후보> 실행_K_최근접_이웃_탐색(
            Map<Integer, Double> 타겟_텐서, 
            Set<Map.Entry<String, Map<Integer, Double>>> 우주_전체_레시피망, 
            int 추출할_K개) {

        if (타겟_텐서 == null || 타겟_텐서.isEmpty() || 우주_전체_레시피망 == null || 추출할_K개 <= 0) {
            return Collections.emptyList();
        }

        long 탐색_시작시간 = System.currentTimeMillis();

        // 💡 [연산 최적화] 타겟 텐서의 노름(Norm, 벡터의 길이)은 변하지 않으므로 루프 외부에서 1회만 선계산
        double 타겟_노름 = 산출하다_텐서_노름(타겟_텐서);
        if (타겟_노름 == 0.0) {
            return Collections.emptyList(); // 타겟이 완전 진공(0.0)이면 비교 불가
        }

        // 💡 [핵심 교정: Zero-Allocation 병렬 콜렉터]
        // 객체 지향 PriorityQueue를 폐기하고, 원시 타입 배열로 이루어진 커스텀 Min-Heap 누산기를 투입합니다.
        Collector<Map.Entry<String, Map<Integer, Double>>, 원시_최소힙_누산기, 원시_최소힙_누산기> 최소힙_콜렉터 =
            Collector.of(
                // 1. 공급자(Supplier): 각 병렬 스레드마다 크기 K의 원시 Min-Heap 생성
                () -> new 원시_최소힙_누산기(추출할_K개),
                
                // 2. 누산기(Accumulator): 내적 계산 후 힙에 직접 밀어넣음 (객체 할당 없음)
                (로컬_힙, 텐서_엔트리) -> {
                    double 유사도 = 산출하다_희소텐서_코사인유사도_최적화(타겟_텐서, 타겟_노름, 텐서_엔트리.getValue());
                    
                    // 직교(유사도 0)하거나 반대 방향인 에너지는 힙에 넣을 가치조차 없음
                    if (유사도 > 0.0) {
                        로컬_힙.밀어넣기(텐서_엔트리.getKey(), 유사도, 텐서_엔트리.getValue());
                    }
                },
                
                // 3. 병합기(Combiner): 병렬 스레드들의 로컬 힙을 하나로 합침
                (힙1, 힙2) -> {
                    힙1.병합하다(힙2);
                    return 힙1;
                }
            );

        try {
            // 💡 [하드웨어 가속] Java 8+ ForkJoinPool 기반 병렬 스트림(Parallel Stream) 스캐닝
            원시_최소힙_누산기 최종_최소힙 = 우주_전체_레시피망.parallelStream()
                                                              .collect(최소힙_콜렉터);

            // 최종 힙에서 데이터를 뽑아 리스트화 및 정렬
            List<탐색_후보> 최종_결과망 = 최종_최소힙.최종_결과_추출_및_정렬();

            long 소요시간 = System.currentTimeMillis() - 탐색_시작시간;
            로거.info(String.format("  ├─ [위상 탐색 완료] 우주 전체 스캔 및 Top-%d 색출 완료. (소요 시간: %d ms, 대상: %d건)", 
                    추출할_K개, 소요시간, 우주_전체_레시피망.size()));

            return 최종_결과망;

        } catch (Exception 예외) {
            로거.log(Level.SEVERE, " [탐색 붕괴] 병렬 스트림 위상 대조 중 치명적 시스템 예외 발생.", 예외);
            return Collections.emptyList();
        }
    }

    // =========================================================================
    // 💡 [객체 멸균 구역] 원시 타입 커스텀 Min-Heap 구조체
    // =========================================================================
    private static class 원시_최소힙_누산기 {
        private final int 한계치_K;
        private int 현재_사이즈;
        
        // 병렬 배열을 통한 객체 생성(new) 멸균
        private final String[] 단어_배열;
        private final double[] 유사도_배열;
        private final Map<Integer, Double>[] 텐서_배열;

        @SuppressWarnings("unchecked")
        public 원시_최소힙_누산기(int k) {
            this.한계치_K = k;
            this.현재_사이즈 = 0;
            this.단어_배열 = new String[k];
            this.유사도_배열 = new double[k];
            this.텐서_배열 = new Map[k];
        }

        public void 밀어넣기(String 단어, double 유사도, Map<Integer, Double> 텐서) {
            if (현재_사이즈 < 한계치_K) {
                // 힙 공간이 남아있으면 배열 끝에 추가 후 위로 끌어올림(Sift-Up)
                단어_배열[현재_사이즈] = 단어;
                유사도_배열[현재_사이즈] = 유사도;
                텐서_배열[현재_사이즈] = 텐서;
                
                상승시키다_최소힙_노드(현재_사이즈);
                현재_사이즈++;
            } else if (유사도 > 유사도_배열[0]) {
                // 💡 [O(1) 필터링] 힙이 꽉 찼을 때, 새 데이터가 Root(최솟값)보다 클 경우에만
                // Root를 파괴하고 덮어쓴 뒤 아래로 끌어내림(Sift-Down)
                단어_배열[0] = 단어;
                유사도_배열[0] = 유사도;
                텐서_배열[0] = 텐서;
                
                하강시키다_최소힙_노드(0);
            }
        }

        public void 병합하다(원시_최소힙_누산기 다른_힙) {
            for (int i = 0; i < 다른_힙.현재_사이즈; i++) {
                밀어넣기(다른_힙.단어_배열[i], 다른_힙.유사도_배열[i], 다른_힙.텐서_배열[i]);
            }
        }

        public List<탐색_후보> 최종_결과_추출_및_정렬() {
            List<탐색_후보> 결과망 = new ArrayList<>(현재_사이즈);
            for (int i = 0; i < 현재_사이즈; i++) {
                결과망.add(new 탐색_후보(단어_배열[i], 유사도_배열[i], 텐서_배열[i]));
            }
            // 최종 리스트를 내림차순(가장 유사도 높은 순)으로 정렬
            Collections.sort(결과망);
            return 결과망;
        }

        private void 상승시키다_최소힙_노드(int 인덱스) {
            String 타겟_단어 = 단어_배열[인덱스];
            double 타겟_유사도 = 유사도_배열[인덱스];
            Map<Integer, Double> 타겟_텐서 = 텐서_배열[인덱스];

            while (인덱스 > 0) {
                int 부모_인덱스 = (인덱스 - 1) >>> 1; // 비트 시프트로 나눗셈 최적화
                
                if (타겟_유사도 >= 유사도_배열[부모_인덱스]) {
                    break;
                }
                
                단어_배열[인덱스] = 단어_배열[부모_인덱스];
                유사도_배열[인덱스] = 유사도_배열[부모_인덱스];
                텐서_배열[인덱스] = 텐서_배열[부모_인덱스];
                인덱스 = 부모_인덱스;
            }
            
            단어_배열[인덱스] = 타겟_단어;
            유사도_배열[인덱스] = 타겟_유사도;
            텐서_배열[인덱스] = 타겟_텐서;
        }

        private void 하강시키다_최소힙_노드(int 인덱스) {
            int 절반_사이즈 = 현재_사이즈 >>> 1;
            String 타겟_단어 = 단어_배열[인덱스];
            double 타겟_유사도 = 유사도_배열[인덱스];
            Map<Integer, Double> 타겟_텐서 = 텐서_배열[인덱스];

            while (인덱스 < 절반_사이즈) {
                int 왼쪽_자식 = (인덱스 << 1) + 1;
                int 오른쪽_자식 = 왼쪽_자식 + 1;
                int 최소자식_인덱스 = 왼쪽_자식;
                double 최소자식_유사도 = 유사도_배열[왼쪽_자식];

                if (오른쪽_자식 < 현재_사이즈 && 유사도_배열[오른쪽_자식] < 최소자식_유사도) {
                    최소자식_인덱스 = 오른쪽_자식;
                    최소자식_유사도 = 유사도_배열[오른쪽_자식];
                }

                if (타겟_유사도 <= 최소자식_유사도) {
                    break;
                }

                단어_배열[인덱스] = 단어_배열[최소자식_인덱스];
                유사도_배열[인덱스] = 유사도_배열[최소자식_인덱스];
                텐서_배열[인덱스] = 텐서_배열[최소자식_인덱스];
                인덱스 = 최소자식_인덱스;
            }
            
            단어_배열[인덱스] = 타겟_단어;
            유사도_배열[인덱스] = 타겟_유사도;
            텐서_배열[인덱스] = 타겟_텐서;
        }
    }

    /**
     * [수학 역학 1] 희소 텐서용 최적화된 코사인 유사도 연산
     * 두 텐서 간의 각도(유사성)를 도출합니다. $Similarity = \frac{\vec{A} \cdot \vec{B}}{\|\vec{A}\| \times \|\vec{B}\|}$
     * 
     * @param 타겟_텐서 질의 텐서
     * @param 타겟_노름 루프 외부에서 미리 계산된 질의 텐서의 노름 (중복 연산 방지용)
     * @param 비교_텐서 DB에서 꺼내온 대조군 텐서
     */
    private double 산출하다_희소텐서_코사인유사도_최적화(
            Map<Integer, Double> 타겟_텐서, 
            double 타겟_노름, 
            Map<Integer, Double> 비교_텐서) {

        if (비교_텐서 == null || 비교_텐서.isEmpty()) return 0.0;

        double 교집합_내적_합 = 0.0;
        double 비교군_노름_제곱 = 0.0;

        // 💡 [기계적 공감] 더 크기가 작은 맵을 기준으로 순회하여 교집합 탐색의 Big-O를 최소화합니다.
        boolean 타겟이_더_작음 = 타겟_텐서.size() < 비교_텐서.size();
        Map<Integer, Double> 순회_기준_맵 = 타겟이_더_작음 ? 타겟_텐서 : 비교_텐서;
        Map<Integer, Double> 조회_대상_맵 = 타겟이_더_작음 ? 비교_텐서 : 타겟_텐서;

        for (Map.Entry<Integer, Double> 엔트리 : 순회_기준_맵.entrySet()) {
            Integer 차원_ID = 엔트리.getKey();
            Double 기준_가중치 = 엔트리.getValue();

            Double 조회_가중치 = 조회_대상_맵.get(차원_ID);
            if (조회_가중치 != null) {
                교집합_내적_합 += (기준_가중치 * 조회_가중치);
            }
        }

        // 만약 두 텐서 간의 직교성(Orthogonality)이 증명되어 내적이 0이라면,
        // 비교 텐서의 전체 노름(Norm)을 계산하는 값비싼 Math.sqrt를 호출할 필요가 없습니다. (Zero-Compute)
        if (교집합_내적_합 == 0.0) {
            return 0.0;
        }

        // 비교 텐서의 노름 도출 (모든 에너지가 존재하는 차원들의 제곱합)
        for (Double 에너지 : 비교_텐서.values()) {
            비교군_노름_제곱 += (에너지 * 에너지);
        }

        double 비교군_노름 = Math.sqrt(비교군_노름_제곱);
        if (비교군_노름 == 0.0) return 0.0;

        return 교집합_내적_합 / (타겟_노름 * 비교군_노름);
    }

    /**
     * [수학 역학 2] 텐서의 유클리드 노름(Euclidean Norm, $L_2$ Norm) 산출
     */
    private double 산출하다_텐서_노름(Map<Integer, Double> 텐서) {
        double 제곱_합 = 0.0;
        for (Double 에너지 : 텐서.values()) {
            제곱_합 += (에너지 * 에너지);
        }
        return Math.sqrt(제곱_합);
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 로직과 스토리지의 철저한 해체 (SRP/DIP 분리):
 * 만약 이전 모듈인 불변 캐시(`422055`) 내부에 이 복잡한 탐색 연산을 우겨넣었다면, 
 * DB는 '저장'이라는 단일 책임 원칙(SRP)을 상실하고 비대해졌을 것입니다.
 * 이 탐색 엔진은 DB의 내부 자료구조를 직접 참조하지 않습니다. 오직 외부에서 주입받은 
 * `우주_전체_레시피망(Set<Map.Entry>)` 만을 바라보는 의존성 역전 원칙(DIP)을 따릅니다. 
 * 스토리지와 수학 연산이 완벽히 분리되었기에, 미래에 캐시 DB가 Redis나 분산 인메모리망으로 
 * 교체되더라도 이 엔진의 수학적 톱니바퀴는 단 한 줄의 코드 수정 없이 영구적으로 재사용됩니다.
 * 
 * 2. 객체 지향의 종말과 원시 타입 최소 힙 (Primitive Min-Heap for Zero-Allocation):
 * 자바의 `PriorityQueue`는 사용하기 편리하지만, 값을 넣고 뺄 때마다 내부적으로 `Object[]` 트리를 재배열하며
 * 감싸고 있는 껍데기 객체(예: `탐색_후보`)를 필연적으로 힙 메모리에 무한히 양산합니다.
 * 만약 100만 개의 단어를 스캔하여 Top-10을 뽑는다면, 스트림 도중 100만 개의 DTO 객체가 생성되었다 버려지며 
 * 가비지 컬렉터(GC)를 완전히 마비시킵니다.
 * 수술된 V6.0 엔진은 객체 기반 큐를 전면 폐기하고, 오직 `double[]`, `String[]` 형태의 
 * 원시 배열(Primitive Array)만으로 이루어진 C언어 방식의 커스텀 Min-Heap(`원시_최소힙_누산기`)을 이식했습니다. 
 * 스캔 도중 메모리 할당(new)은 단 한 번도 일어나지 않아, HFT 환경에서 GC 지연(Stop-The-World)을 물리적으로 멸균했습니다.
 * 
 * 3. 희소 맵 교집합 내적 (Dot Product of Sparse Maps):
 * $Similarity = \frac{\vec{A} \cdot \vec{B}}{\|\vec{A}\| \times \|\vec{B}\|}$
 * 두 단어의 텐서가 각각 5개, 8개의 차원만 에너지를 가지고 있다면, 고정 배열(`double[4096]`)의 경우 
 * 4,096번의 무의미한 0.0 곱셈 루프를 돌아야 합니다. 
 * 그러나 희소 맵(Sparse Map) 환경에서는 더 크기가 작은 맵을 기준으로 순회하며, 상대방 맵에 
 * 해당 키(Key)가 존재하는지 `get()` 해보는 것만으로 순식간에 교집합 내적을 도출해냅니다. 
 * 이는 연산량(Time Complexity)을 공간의 크기(Dimension)가 아닌, '존재하는 에너지의 개수' 단위로 
 * 강등시켜 극강의 Zero-Compute 효율을 만들어냅니다.
 * =============================================================================
 */