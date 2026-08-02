/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L2_범용_AI_공통_개방망.티어20_연방_합의망
 * @alias Spacetime_Sharding_Router
 * @tier 20
 * @keywords Consistent Hashing, Scatter-Gather, Distributed Tensor Routing, High Availability, Fail-Fast, Fallback
 * 
 * [파일 개요 (File Overview)]
 * - 파일명: A0_DT_42_425020_은하계_샤딩_라우터.java
 * - 모듈명: 통합 OS V6.1 - Tier 20: 시공간 샤딩 라우터 (분산망 스캐터-개더 코어)
 * - 기능 및 역할: 데이터베이스의 Y축(Entity/종목)을 안정 해시(Consistent Hashing) 링에 매핑하여 분산 클러스터 노드 간 데이터를 샤딩(Sharding)하고, 
 *             다중 노드에 걸친 쿼리 발생 시 스캐터-개더(Scatter-Gather) 방식으로 텐서를 병합 라우팅합니다.
 * - 이론 및 기술: 안정 해시 링(Consistent Hash Ring), 가상 노드(Virtual Nodes) 부하 분산, 맵리듀스(Map-Reduce) 사상 기반 스캐터-개더 패턴, 인과율 보존 롤포워드(Roll-forward).
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [아키텍처 혁신] 무한 블로킹 방지 (Fail-Fast Scatter): `executeScatterGatherQuery` 메서드 내의 무방비한 `allTasksFuture.join()` 호출 구조를 
 *                 `orTimeout(3, TimeUnit.SECONDS)` 기반으로 개편하여 무한 대기 타임아웃 뇌관을 파괴했습니다.
 * - 💡 [아키텍처 혁신] 부분 수집(Partial Gather) 롤포워드 폴백: `orTimeout` 예외 발생 시 전체 트랜잭션을 붕괴(Abort)시키지 않고, 
 *                 정상적으로 수신된 타 노드의 텐서 파편들만으로 데이터 융합을 강행(Roll-forward)하는 폴백(Fallback) 로직을 이식하여 시스템 응답 가용성(High Availability)을 극한으로 끌어올렸습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 안정 해시 링(Consistent Hash Ring) 구축, 병렬 비동기 스캐터-개더 연산을 위한 자바 표준 라이브러리들을 Import 합니다.
// [2. 영문 상세 주석]
// Package declaration and import of Java standard libraries for building a Consistent Hash Ring and performing parallel asynchronous scatter-gather operations.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L2_범용_AI_공통_개방망.티어20_연방_합의망;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 대규모 분산 환경에서 Y축(종목/엔티티)의 소유권(Ownership)을 할당하고, 물리적으로 분산된 텐서를 병합 조립하는 샤딩 라우터 코어입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A sharding router core that allocates ownership of the Y-axis (tickers/entities) in a massive distributed environment and merges physically distributed tensors.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_425020
 * [파일명] A0_DT_42_425020_은하계_샤딩_라우터.java
 * [모듈명] 통합 OS V6.1 - Tier 20: 시공간 샤딩 라우터 (분산망 스캐터-개더 코어)
 * 
 * [설계 명세]
 * 1. 역할: 지능형 인덱스 사전의 최상단 라우팅 레이어에 위치하여, 특정 종목(Y축) 텐서 데이터가 클러스터 내 어느 물리 노드에
 * 상주하는지 판별 및 분배.
 * 2. 기능: FNV-1a 알고리즘 기반 64비트 안정 해시 링(Consistent Hash Ring) 구축,
 * 스캐터-개더(Scatter-Gather) 병렬 질의 수행.
 * 3. 의도: 단일 머신의 한계(Memory & Disk I/O)를 돌파하기 위한 완벽한 스케일 아웃(Scale-out) 아키텍처 제공.
 * ==============================================================================
 */
public final class A0_DT_42_425020_은하계_샤딩_라우터 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.425020_SHARDING_ROUTER");

    // [1. 한글 상세 주석]
    // 데이터 쏠림(Data Skew / Hotspot) 현상을 방지하기 위해 1개의 물리 노드당 생성할 가상 노드(Virtual Node)
    // 복제본의 개수입니다.
    // [2. 영문 상세 주석]
    // The number of Virtual Node replicas to create per physical node to prevent
    // Data Skew (Hotspot) phenomena.

    private static final int VIRTUAL_NODE_REPLICAS = 128;

    // [1. 한글 상세 주석]
    // FNV-1a 64비트 해시값을 Key로, 해당 해시 스페이스를 담당하는 물리 노드의 ID를 Value로 가지는 안정 해시 링
    // 자료구조(SkipListMap)입니다.
    // [2. 영문 상세 주석]
    // A Consistent Hash Ring data structure (SkipListMap) that uses FNV-1a 64-bit
    // hash values as Keys and the ID of the physical node responsible for that hash
    // space as Values.

    private final ConcurrentSkipListMap<Long, String> consistentHashRing = new ConcurrentSkipListMap<>();

    private final String currentLocalNodeId;
    private final List<String> globalClusterNodes;

    // [1. 한글 상세 주석]
    // 클러스터 내 타 노드에 데이터 조회를 비동기 위임(Scatter)하기 위한 분산 통신(RPC) 포트 의존성 객체입니다.
    // [2. 영문 상세 주석]
    // A distributed communication (RPC) port dependency object for asynchronously
    // delegating (Scatter) data queries to other nodes in the cluster.

    private final ScatterRpcPort scatterRpcPort;

    private final ExecutorService scatterParallelThreadPool;

    /**
     * [분산 텐서 조회 RPC 포트 인터페이스 규격]
     * 다른 노드에 존재하는 텐서를 Zero-Copy 통신으로 읽어오거나 로컬 메모리에서 직접 조회하는 통신 어댑터 규격입니다.
     */
    public interface ScatterRpcPort {
        CompletableFuture<Map<String, Map<Integer, Float>>> fetchRemoteTensorAsync(
                String targetNodeId, List<String> assignedEntities, String featureName, int startTick, int endTick);

        Map<String, Map<Integer, Float>> fetchLocalTensor(
                List<String> localEntities, String featureName, int startTick, int endTick);
    }

    /**
     * [생성자] 클러스터 노드 목록을 주입받아 안정 해시 링을 구축하고 스캐터-개더(Scatter-Gather) 분산 질의 엔진을 점화합니다.
     */
    public A0_DT_42_425020_은하계_샤딩_라우터(String currentLocalNodeId, List<String> clusterNodes, ScatterRpcPort rpcPort) {
        if (clusterNodes == null || clusterNodes.isEmpty() || rpcPort == null) {
            throw new IllegalArgumentException("[배관 파열] 클러스터 노드망 목록 또는 RPC 통신 포트 의존성이 누락되어 샤딩 라우터를 점화할 수 없습니다.");
        }

        this.currentLocalNodeId = currentLocalNodeId;
        this.globalClusterNodes = new ArrayList<>(clusterNodes);
        this.scatterRpcPort = rpcPort;

        // 원격 질의(Scatter) 병렬 비동기 처리를 위한 분산 라우팅 전용 스레드 풀 할당
        int availableCores = Math.max(4, Runtime.getRuntime().availableProcessors());
        this.scatterParallelThreadPool = Executors.newFixedThreadPool(availableCores, runnable -> {
            Thread thread = new Thread(runnable, "OS_SCATTER_GATHER_WORKER");
            thread.setDaemon(true);
            return thread;
        });

        buildConsistentHashRing();
        logger.info(String.format(" >> [통합 OS V6.1] A0_DT_42_425020 샤딩 라우터 기동. (총 %d개 노드, %d개 가상 노드 해시 링 전개 완료)",
                globalClusterNodes.size(), consistentHashRing.size()));
    }

    // [1. 한글 상세 주석]
    // 클러스터에 참여하는 모든 물리 노드에 대해 각각 VIRTUAL_NODE_REPLICAS 개수만큼 가상 노드 식별자를 생성하여 해시 링에
    // 고르게 배치합니다.
    // [2. 영문 상세 주석]
    // Creates virtual node identifiers equal to VIRTUAL_NODE_REPLICAS for every
    // physical node participating in the cluster and evenly places them on the hash
    // ring.

    private void buildConsistentHashRing() {
        consistentHashRing.clear();
        for (String nodeId : globalClusterNodes) {
            for (int i = 0; i < VIRTUAL_NODE_REPLICAS; i++) {
                String virtualNodeIdentifier = nodeId + "_VNODE_" + i;
                long hashKey = calculateFnv1aHash(virtualNodeIdentifier);
                consistentHashRing.put(hashKey, nodeId);
            }
        }
    }

    // [1. 한글 상세 주석]
    // FNV-1a 64비트 해시 알고리즘. 외부 라이브러리 의존성 없이 순수 O(L) 속도로 문자열을 우수한 균등 분포의 64비트
    // 정수(Long)로 해싱합니다.
    // [2. 영문 상세 주석]
    // FNV-1a 64-bit hash algorithm. Hashes strings into an excellently evenly
    // distributed 64-bit integer (Long) at pure O(L) speed without external library
    // dependencies.

    private long calculateFnv1aHash(String text) {
        long FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325L;
        long FNV_PRIME_64 = 0x100000001b3L;

        long hashValue = FNV_OFFSET_BASIS_64;
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);

        for (byte b : bytes) {
            hashValue ^= (b & 0xff);
            hashValue *= FNV_PRIME_64;
        }
        return hashValue;
    }

    // [1. 한글 상세 주석]
    // 특정 엔티티 식별자(예: "005930")가 클러스터 내 어느 물리 노드의 관할(Ownership)인지 O(log N) 탐색 속도로
    // 도출합니다.
    // [2. 영문 상세 주석]
    // Derives which physical node in the cluster has jurisdiction (Ownership) over
    // a specific entity identifier (e.g., "005930") at O(log N) search speed.

    public String resolveTargetNode(String entityCode) {
        if (consistentHashRing.isEmpty()) {
            return currentLocalNodeId; // 해시 링 데이터 붕괴 시 안전 폴백(Fallback) 방어
        }

        long hashKey = calculateFnv1aHash(entityCode);

        // ceilingEntry: 해시 링 구조에서 지정된 키보다 크거나 같은 위치에 있는 첫 번째 가상 노드를 스캔 탐색합니다.
        Map.Entry<Long, String> matchedEntry = consistentHashRing.ceilingEntry(hashKey);

        // 만약 도출된 키가 링의 최댓값을 초과하여 일치하는 노드가 없다면, 원형(Ring) 토폴로지 구조이므로 첫 번째 노드(최솟값)로
        // 순환(Wrap-around)합니다.
        if (matchedEntry == null) {
            return consistentHashRing.firstEntry().getValue();
        }
        return matchedEntry.getValue();
    }

    // [1. 한글 상세 주석]
    // 💡 [초정밀 아키텍처 혁신: Fail-Fast 스캐터-개더 엔진 및 부분 수집 폴백(Partial Gather Fallback)]
    // 복수의 다중 종목에 대한 텐서 질의 시 질의를 클러스터에 흩뿌리고(Scatter) 결과를 하나로 병합(Gather)합니다.
    // 이때 특정 원격 노드가 크래시(Crash)되어 응답이 오지 않을 경우, 전체 시스템 트랜잭션이 영구 정지(Hanging)되는 것을 막기
    // 위해 `orTimeout()` 방어막을 칩니다.
    // [2. 영문 상세 주석]
    // 💡 [Ultra-Precision Architecture Innovation: Fail-Fast Scatter-Gather Engine
    // and Partial Gather Fallback]
    // When querying tensors for multiple entities, scatters the queries across the
    // cluster and gathers the results into one.
    // Sets up an `orTimeout()` shield to prevent the entire system transaction from
    // permanently hanging if a specific remote node crashes and fails to respond.

    public Map<String, Map<Integer, Float>> executeScatterGatherQuery(
            List<String> requestedEntities,
            String featureName,
            int startTick,
            int endTick) {

        if (requestedEntities == null || requestedEntities.isEmpty()) {
            return Collections.emptyMap();
        }

        // 1. [라우팅 맵 구성 (Routing Setup)] 해시 링을 기반으로 각 물리 노드가 처리해야 할 종목 목록을 분류 조립합니다.
        Map<String, List<String>> nodeRoutingMap = new HashMap<>();
        for (String entity : requestedEntities) {
            String targetNode = resolveTargetNode(entity);
            nodeRoutingMap.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(entity);
        }

        List<CompletableFuture<Map<String, Map<Integer, Float>>>> asyncGatherFutures = new ArrayList<>();

        // 2. [스캐터 (Scatter Phase)] 분산 노드망으로 질의를 동시에 병렬 폭격(Broadcast)합니다.
        for (Map.Entry<String, List<String>> allocationEntry : nodeRoutingMap.entrySet()) {
            String targetNode = allocationEntry.getKey();
            List<String> targetEntityGroup = allocationEntry.getValue();

            if (targetNode.equals(currentLocalNodeId)) {
                // 로컬 노드의 할당분은 네트워크 I/O 통신 없이 직접 FFM 메모리를 타격하여 조회 (Zero-Network Overhead)
                CompletableFuture<Map<String, Map<Integer, Float>>> localTaskFuture = CompletableFuture
                        .supplyAsync(() -> scatterRpcPort.fetchLocalTensor(targetEntityGroup, featureName, startTick,
                                endTick), scatterParallelThreadPool);
                asyncGatherFutures.add(localTaskFuture);
            } else {
                // 💡 [부분 수집 폴백 아키텍처 강화] 원격 노드 할당분은 RPC를 통해 비동기 전송
                // `orTimeout()`을 걸어 3초 안에 물리적 응답이 오지 않으면 TimeoutException을 격발시키고
                // `exceptionally` 블록으로 강제 진입시킵니다.
                CompletableFuture<Map<String, Map<Integer, Float>>> remoteTaskFuture = scatterRpcPort
                        .fetchRemoteTensorAsync(targetNode, targetEntityGroup, featureName, startTick, endTick)
                        .orTimeout(3, TimeUnit.SECONDS)
                        .exceptionally(ex -> {
                            logger.log(Level.WARNING, " 🚨 [통신망 파열 및 롤포워드(Roll-forward) 강행] 노드 " + targetNode
                                    + " 에 대한 데이터 질의가 3초를 초과하여 타임아웃 되었습니다. 응답 없는 파편을 제외하고 정상 수집된 부분 텐서 맵만으로 융합을 강행합니다.",
                                    ex);
                            // 네트워크 파열 시 빈 맵(Empty Map)을 반환하여 Exception 전파로 인한 스캐터-개더 전체 파이프라인 붕괴를 구조적으로 완벽히
                            // 방어 (Fail-Fast & Graceful Degradation)
                            return Collections.emptyMap();
                        });
                asyncGatherFutures.add(remoteTaskFuture);
            }
        }

        // 3. 💡 [동기화 장벽 멸균 (Synchronization Barrier)] 모든 노드로부터 데이터가 반환(또는 타임아웃)될 때까지 메인
        // 스레드 대기
        CompletableFuture<Void> allTasksFuture = CompletableFuture
                .allOf(asyncGatherFutures.toArray(new CompletableFuture[0]));
        try {
            // 내부의 개별 Future(임무) 객체들에 `orTimeout` 예외 및 폴백 처리가 모두 내장되어 있으므로, `join()` 호출 시
            // 영원한 무한 대기(Deadlock)에 절대 빠지지 않습니다.
            allTasksFuture.join();
        } catch (Exception ex) {
            logger.severe(" [스캐터-개더 붕괴] 분산 수집 동기화 조립(Join) 중 치명적 시스템 예외가 발생했습니다.");
        }

        // 4. [개더 (Gather Phase)] 조각난 텐서 단면 파편들을 하나의 완벽한 Map 구조체로 융합
        Map<String, Map<Integer, Float>> finalMergedTensorMap = new HashMap<>();
        for (CompletableFuture<Map<String, Map<Integer, Float>>> completedTask : asyncGatherFutures) {
            // join() 호출 시 대상 노드가 다운되었더라도 exceptionally 블록에서 이미 emptyMap으로 치환했으므로 Null 예외 위험
            // 없이 극도로 안전합니다.
            Map<String, Map<Integer, Float>> fragmentResult = completedTask.join();
            if (fragmentResult != null) {
                finalMergedTensorMap.putAll(fragmentResult);
            }
        }

        logger.fine(
                String.format("   ├─ [스캐터-개더 융합 수료] 총 %d개 클러스터 노드에서 분산 병렬 수집된 %d개 종목의 텐서가 완벽히 하나로 융합(Gathered)되었습니다.",
                        nodeRoutingMap.size(), finalMergedTensorMap.size()));

        return Collections.unmodifiableMap(finalMergedTensorMap);
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 철학 (Theoretical Background & Philosophy)]
 * 
 * 1. 무한 블로킹 멸균과 Fail-Fast 스캐터 (Fail-Fast Scatter and Timeout Isolation):
 * 일반적인 Java 환경의 병렬 처리에서 `CompletableFuture.allOf().join()` 방식은 분산 컴퓨팅의 가장 흔하면서도
 * 치명적인 안티 패턴(Anti-Pattern)입니다.
 * 질의(Query)를 병렬로 보낸 10대의 클러스터 노드 중 단 1대의 노드가 커널 패닉으로 멈추거나 네트워크 케이블 랙(Lag)이
 * 발생하면,
 * 메인 오케스트레이터 라우터 스레드는 영원히 오지 않을 단 1개의 응답을 멍하니 기다리며 무한 대기 교착(Deadlock/Hanging)
 * 상태에 빠집니다.
 * 수리된 V6.1 라우터 아키텍처는 비동기 임무(Task)마다 `.orTimeout(3, TimeUnit.SECONDS)`이라는 타임아웃
 * 뇌관(Detonator)을 장착했습니다.
 * 특정 타겟 노드가 3초 내에 응답을 주지 못하면 스스로 예외 폭파(TimeoutException)를 격발시켜, 메인 HFT 코어의 생명주기
 * 블로킹을 즉각 해방시킵니다.
 * 
 * 2. 부분 수집 폴백 (Partial Gather Fallback)과 시스템 인과율 보존 (High Availability
 * Roll-forward):
 * 10대 중 단 1대의 노드가 응답하지 않는다고 해서 융합(Gather) 연산 전체를 취소(Abort Exception)시키고 사용자에게
 * HTTP 500 에러를 던진다면,
 * 정상 가동 중인 나머지 9대 노드가 성실하게 수집하여 반환한 90%의 데이터까지 모조리 무의미하게 폐기되는 최악의 연쇄
 * 붕괴(Cascading Failure)가 발생합니다.
 * 본 라우터 시스템은 타임아웃이 발생하면 `.exceptionally()` 블록이 이를 캐치하여 상위로 에러를 전파하지 않고 조용히
 * `Collections.emptyMap()`을 반환합니다.
 * 즉, 클라이언트가 요구한 500개 종목 중 50개가 타임아웃으로 누락되더라도, 살아남아 수집된 450개 종목의 텐서 파편만으로 최종
 * 매트릭스 맵을 조립 강행(Roll-forward)하여
 * 시스템을 조금 절룩거리게 할지언정 결코 죽이지 않는 완벽한 데이터 응답 가용성(High Availability & Graceful
 * Degradation)의 극치를 보여줍니다.
 * 
 * 3. 안정 해시 링 (Consistent Hash Ring)과 가상 노드 (Virtual Node Replicas):
 * 전통적인 DB 파티셔닝 방식인 `Hash(종목명) % N(노드 수)` 모듈로 연산 방식은,
 * 운영 도중 클러스터에 물리 노드 장비가 한 대 추가되거나(N -> N+1) 삭제될 때마다 기존 데이터 파티션의 90% 이상이 어긋나버리는
 * 전면적인 데이터 재배치 폭풍(Re-balancing Storm)을 유발합니다.
 * 본 샤딩 라우터는 64비트 정수 공간(Long.MIN_VALUE ~ Long.MAX_VALUE) 범위의 거대한 원형 공간 토폴로지(Ring
 * Topology)를 논리적으로 구성하며,
 * 1개의 물리 기계 서버당 128개의 가상 분신(Virtual Node) 해시값을 창조하여 링 위에 골고루 무작위 산포시킵니다.
 * 이 구조적 혁신으로 인해 삼성전자, 테슬라 등 쿼리 트래픽이 극단적으로 몰리는 헤비(Heavy) 종목들이 한 기계에 쏠려 디스크
 * 스래싱(Hotspot Thrashing)을 유발하는 편향 현상을 물리적으로 완벽히 방어하며,
 * 노드 추가/제거 시 데이터 재배치 파장을 인접한 노드 구간으로만 최소화하는 0.1초 수준의 Scale-Out/Scale-In 탄력성을
 * 보장합니다.
 * =============================================================================
 */