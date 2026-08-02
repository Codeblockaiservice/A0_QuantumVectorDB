/*
 * ==============================================================================
 * [Meta-Tags]
 * @module L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인
 * @alias Semantic_Document_Shredder_Axe
 * @tier 16
 * @keywords Strategy Pattern, Dynamic Chunking, Port and Adapter, Format Decoupling, RAG Pipeline
 * 
 * [파일 개요 (File Overview)]
 * - 파일명 (File Name): A0_DT_42_423020_시맨틱_문헌_해체_도끼.java
 * - 기능 (Function): PROCESSING 상태로 유입된 비정형 문헌(TXT, CSV, PDF 등)의 포맷을 해체하고 순수 텍스트를 추출.
 * - 역할 (Role): 물리적 한계를 지닌 원시 문서를 AI 모델이 소화할 수 있는 '시맨틱 청크(Semantic Chunk)' 단위로 분할하는 무인 파쇄기.
 * - 이론 (Theory): 포트 앤 어댑터 아키텍처(Port and Adapter Architecture), RAG 기반 다형성 청킹 전략(Polymorphic Chunking Strategy), 매직 넘버 식별.
 * - 기대효과 (Effect): 특정 서드파티 라이브러리 및 정적인 분할 방식에 종속되지 않고, 문헌의 메타데이터 특성에 맞춘 완벽한 동적 파편화를 이룩함.
 * 
 * [신규/변경/삭제 사항]
 * - 💡 [명칭 교정] 비유적 한글 변수/메서드명을 글로벌 소프트웨어 공학 표준 영문으로 전면 순화(Refactoring).
 * - 💡 [초정밀 통제] 다형성 청킹 전략(Strategy Pattern) 결속: 
 *                 과거 1000자로 고정(Hardcoding)되어 있던 슬라이딩 윈도우 한계치를 제거했습니다.
 *                 문서의 모달리티(메타데이터)를 런타임에 분석하여 BPE 토큰 단위, 문단 단위, 고정 길이 단위 등 
 *                 동적 청킹 룰셋(Strategy)을 외부로부터 주입(DI)받아 실행하는 파이프라인 구조로 승격시켰습니다.
 * ==============================================================================
 */

// [1. 한글 상세 주석]
// 패키지 선언 및 의존성 모듈 Import. 비동기 에러 사출 로거 및 파일 I/O 제어를 위한 코어 라이브러리를 포함합니다.
// [2. 영문 상세 주석]
// Package declaration and import of dependency modules. Includes core libraries for asynchronous anomaly logging and file I/O control.
// [3. 자바 코드]
package A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어16_무인_자율화_파이프라인;

import A0_QuantumVectorDB_양자벡터DB.L1_기저_아카이브_및_원시_데이터망.티어3_무결성_검수_및_감시망.A0_DT_42_422033_LMAX_이상_보고서_로거;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

// [1. 한글 상세 주석]
// 컴플라이언스 선언 및 클래스 헤더. 비정형 원시 문헌의 물리적 포맷을 해체하고 논리적 의미 단위로 분할하는 시맨틱 파이프라인 모듈입니다.
// [2. 영문 상세 주석]
// Compliance declaration and class header. A semantic pipeline module that dismantles the physical formats of raw unstructured documents and splits them into logical semantic units.
// [3. 자바 코드]
/**
 * ==============================================================================
 * [컴플라이언스 준수 선언]
 * Tier 5+ Standard / SPLM Specification / OS Charter Compliance Verified
 * 
 * [12자리 코드번호] A0_DT_42_423020
 * [파일명] A0_DT_42_423020_시맨틱_문헌_해체_도끼.java
 * [모듈명] 통합 OS V6.0 - Tier 16: 시맨틱 문헌 해체 도끼 (무인 비정형 문서 파쇄기)
 * 
 * [설계 명세]
 * 1. 역할: PROCESSING 폴더로 유입된 비정형 문헌의 포맷(TXT, CSV, PDF 등)을 디코딩하고 순수 평문 텍스트를 추출.
 * 2. 기능: 의미가 단절되지 않도록 문단/문장 기반의 슬라이딩 윈도우(Sliding Window) 청킹(Chunking) 등 동적 수행.
 * 3. 의도: 방대한 문서를 LLM의 컨텍스트 윈도우 한계에 맞게 최적화된 논리 단위(Chunk)로 쪼개어 다음 계층의 임베딩 엔진에 공급.
 * 4. 이론: RAG(Retrieval-Augmented Generation) 기반 시맨틱 청킹, 문맥 오버랩(Context Overlap), 포트 앤 어댑터(Port and Adapter), 전략 패턴(Strategy Pattern).
 * 5. 기술: 의존성 역전 원칙(DIP)을 적용한 전략 제공자 인터페이스, NIO FileChannel 기반 매직 넘버(Magic Number) 식별.
 * 6. 💡 [V6.0 핵심 아키텍처] 다형성 청킹 전략(Strategy Pattern) 결속:
 * 문헌이 학술 논문인지 트위터(단문)인지 도메인을 고려하지 않고 맹목적으로 고정 문자열 길이로 자르던 한계를 소거했습니다.
 * `ChunkingStrategyProvider` 인터페이스 포트를 통해 런타임에 동적인 청킹 룰셋(Strategy)을 주입받아 모듈의 유연성을 극대화합니다.
 * ==============================================================================
 */
public final class A0_DT_42_423020_시맨틱_문헌_해체_도끼 {

    private static final Logger logger = Logger.getLogger("A0_OS.MATRIX.423020_SEMANTIC_SHREDDER");

    // [의존성 포트] 다른 티어(메타데이터, 임베딩, 외부 라이브러리)와의 모듈 간 결합도를 0으로 수렴시키는 어댑터 인터페이스 포트 선언
    private final RegistryEnrollmentPort registryEnrollmentPort;
    private final EmbeddingTransferPort embeddingTransferPort;

    // [1. 한글 상세 주석]
    // 💡 [아키텍처 방어망] 통합 OS 코어가 Apache PDFBox 같은 특정 서드파티 라이브러리의 클래스 참조에 오염되지 않도록 격리하는 ACL(Anti-Corruption Layer) 포트입니다.
    // [2. 영문 상세 주석]
    // 💡 [Architecture Defense Net] An ACL (Anti-Corruption Layer) port that isolates the Integrated OS core from being contaminated by class references to specific third-party libraries like Apache PDFBox.
    private final UnstructuredTextExtractorPort textExtractorPort;

    // 💡 [전략 패턴 결속망] 런타임에 문서의 특성에 맞는 최적의 청킹 전략을 동적으로 판단하여 주입하는 팩토리(Factory) 포트
    private final ChunkingStrategyProvider chunkingStrategyProvider;

    // 파이프라인 인과율을 추적하고 에러를 사출할 LMAX 비동기 로거
    private final A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger;

    /**
     * [이관 포트 인터페이스 1: Tier 1 호적부 메타데이터 연결]
     * 해체된 문서의 메타데이터를 시스템의 고유 엔티티(Entity)로 공식 등재합니다.
     */
    @FunctionalInterface
    public interface RegistryEnrollmentPort {
        void requestNewEntityEnrollment(DocumentMetadata metadata);
    }

    /**
     * [이관 포트 인터페이스 2: Tier 16 위상 임베딩 사영소 연결]
     * 잘게 쪼개진 순수 텍스트 청크 파편들을 고차원 벡터 텐서로 변환하기 위해 다음 임베딩 엔진 파이프라인으로 던집니다.
     */
    @FunctionalInterface
    public interface EmbeddingTransferPort {
        void transferShreddedChunks(DocumentMetadata metadata, List<SemanticChunkPayload> chunkPayloadList);
    }

    /**
     * [이관 포트 인터페이스 3: 외부 서드파티 바이너리 추출 어댑터 연결]
     */
    @FunctionalInterface
    public interface UnstructuredTextExtractorPort {
        String extractBinaryDocumentText(Path physicalFilePath) throws IOException;
    }

    // [1. 한글 상세 주석]
    // 💡 [전략 패턴 인터페이스] 하드코딩된 내부 청킹 로직을 외부로 분리하여 유연한 다형성(Polymorphism) 구조로 추상화합니다.
    // [2. 영문 상세 주석]
    // 💡 [Strategy Pattern Interfaces] Abstracts the hardcoded internal chunking logic outwards into a flexible polymorphic structure.

    /**
     * [다형성 청킹 전략 인터페이스]
     * 텍스트를 파쇄하는 구체적인 알고리즘(예: BPE 토큰 기반, 고정 윈도우 길이 기반, 문단/의미 기반)의 계약 규격.
     */
    @FunctionalInterface
    public interface PolymorphicChunkingStrategy {
        List<SemanticChunkPayload> executeChunking(String documentUuid, String rawText);
    }

    /**
     * [청킹 전략 제공자 (Factory/Provider)]
     * 문헌의 메타데이터(포맷, 도메인, 크기 등)를 분석하여 해당 문서에 가장 적합한 청킹 전략(Strategy) 객체를 런타임에 결정하여 하사합니다.
     */
    @FunctionalInterface
    public interface ChunkingStrategyProvider {
        PolymorphicChunkingStrategy determineOptimalStrategy(DocumentMetadata metadata);
    }

    /**
     * [데이터 DTO 1: 문서 메타데이터 레코드]
     */
    public record DocumentMetadata(
            String documentUuid,
            String originalFileName,
            String extractedFormat,
            long creationEpochSeconds) {
    }

    /**
     * [데이터 DTO 2: 시맨틱 청크 파편 레코드]
     */
    public record SemanticChunkPayload(
            String documentUuid,
            int chunkIndex, // 문서 내에서의 순서 (조립 및 컨텍스트 추론 시 활용)
            String rawTextFragment) {
    }

    // [1. 한글 상세 주석]
    // [생성자] 문서 해체 모듈을 기동하고 전/후방 파이프라인과 전략 제공자 포트를 조립 결속시킵니다.
    // [2. 영문 상세 주석]
    // [Constructor] Starts the document shredding module and binds the front/rear pipelines and the strategy provider port.

    public A0_DT_42_423020_시맨틱_문헌_해체_도끼(
            RegistryEnrollmentPort registryEnrollmentPort,
            EmbeddingTransferPort embeddingTransferPort,
            UnstructuredTextExtractorPort textExtractorPort,
            ChunkingStrategyProvider chunkingStrategyProvider, // 💡 동적 청킹 룰셋 주입 포트
            A0_DT_42_422033_LMAX_이상_보고서_로거 anomalyLogger) {

        if (registryEnrollmentPort == null || embeddingTransferPort == null || textExtractorPort == null || chunkingStrategyProvider == null) {
            throw new IllegalArgumentException("[배관 누락] 전후방 데이터 포트 또는 전략 제공자 인터페이스가 단절되어 모듈을 기동할 수 없습니다.");
        }

        this.registryEnrollmentPort = registryEnrollmentPort;
        this.embeddingTransferPort = embeddingTransferPort;
        this.textExtractorPort = textExtractorPort;
        this.chunkingStrategyProvider = chunkingStrategyProvider;
        this.anomalyLogger = anomalyLogger;

        logger.info(" >> [통합 OS V6.0] A0_DT_42_423020 시맨틱 문헌 해체 파이프라인 기동. (다형성 청킹 전략 패턴 결속 완료)");
    }

    /**
     * [해체 역학 1: 메인 파이프라인 수신단]
     * 사상의 지평선 감시망(`423010`)이 흡수 완료된 안전한 파일의 경로를 넘겨주면, 이를 즉각 해체 파쇄합니다.
     * 
     * @param processingFilePath PROCESSING 폴더에 안전하게 파일 락(Lock)이 풀려 이관된 원본 파일 절대 경로
     */
    public void executeDocumentShreddingAndTransfer(Path processingFilePath) {
        String fileName = processingFilePath.getFileName().toString();
        logger.info("   ├─ [문서 파쇄 격발] 물리적 문헌의 포맷 껍데기를 파괴합니다: " + fileName);

        try {
            // 1. [포맷 파괴 및 원시 텍스트 추출]
            // 확장자가 아닌 매직 넘버(Magic Number)를 통한 포맷 식별 및 순수 문자열 디코딩 복원
            DocumentExtractionResult extractionResult = extractRawTextAndIdentifyFormat(processingFilePath);

            if (extractionResult.rawText().isEmpty()) {
                if (anomalyLogger != null) {
                    anomalyLogger.logAnomalyEvent("DOCUMENT", "UNKNOWN", "ALL", "EMPTY_DOCUMENT",
                            "데이터 내용이 없는 진공 상태의 문서이거나 텍스트를 추출할 수 없는 지원 불가 포맷입니다: " + fileName);
                }
                logger.warning(" [파쇄 경고] 텍스트를 추출할 수 없는 포맷이거나 파일이 비어 있습니다: " + fileName);
                return;
            }

            // 2. [엔티티 인덱싱] 문헌의 고유 식별자(UUID) 부여 및 메타데이터 캡슐화
            String documentUuid = "DOC_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
            long currentEpoch = Instant.now().getEpochSecond();

            DocumentMetadata metadata = new DocumentMetadata(
                    documentUuid,
                    fileName,
                    extractionResult.formatSignature(),
                    currentEpoch);

            // 3. [Tier 1 호적부 등재 통보] 시스템 내의 새로운 지식 엔티티(Entity)로 공식 선언
            registryEnrollmentPort.requestNewEntityEnrollment(metadata);

            // 4. 💡 [동적 전략 결속 및 시맨틱 청킹 실행]
            // 메타데이터(예: PDF 여부, 파일명, 모달리티)를 바탕으로 최적의 청킹 전략 객체를 런타임에 주입받아 분할을 실행합니다.
            PolymorphicChunkingStrategy optimalStrategy = chunkingStrategyProvider.determineOptimalStrategy(metadata);
            List<SemanticChunkPayload> chunkPayloadList = optimalStrategy.executeChunking(metadata.documentUuid(), extractionResult.rawText());

            logger.fine(String.format("   ├─ [파쇄 완료] 문헌(%s)이 %s 전략에 의해 %d개의 의미론적 텍스트 청크 파편으로 분할되었습니다.",
                    documentUuid, optimalStrategy.getClass().getSimpleName(), chunkPayloadList.size()));

            // 5. [위상 임베딩 사영소로 이관] 잘게 쪼개진 텍스트 청크들을 임베딩 텐서 벡터로 사영(Projection)시키기 위해 다음 계층으로 던짐
            embeddingTransferPort.transferShreddedChunks(metadata, chunkPayloadList);

        } catch (Exception ex) {
            // 예외의 침묵 방지 및 LMAX 비동기 큐를 통한 에러 내역 사출
            if (anomalyLogger != null) {
                anomalyLogger.logAnomalyEvent("DOCUMENT", "UNKNOWN", "ALL", "SHREDDER_ERROR",
                        "문헌 해체 및 텍스트 추출 중 치명적 예외 발생: " + fileName + ", " + ex.getMessage());
            }
            logger.log(Level.SEVERE, " [파이프라인 붕괴] 문헌 해체 파이프라인 처리 중 치명적 예외 발생: " + fileName, ex);
        }
    }

    private record DocumentExtractionResult(String formatSignature, String rawText) {
    }

    /**
     * [해체 역학 2: 포맷 디커플링 및 원시 텍스트 복원]
     * 파일명 끝에 달린 확장자를 맹신하지 않고, 파일 헤더의 매직 넘버(Magic Number) 바이트를 직접 읽어 진짜 포맷을 판별합니다.
     */
    private DocumentExtractionResult extractRawTextAndIdentifyFormat(Path filePath) throws IOException {

        // 💡 [매직 넘버 딥 스캔]
        // NIO 다이렉트 버퍼로 파일 최상단의 앞 4바이트만 초고속 판독하여 파일의 진정한 시그니처 DNA를 파악합니다.
        byte[] magicHeaderBytes = new byte[4];
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.wrap(magicHeaderBytes);
            channel.read(buffer);
        }

        String hexSignature = String.format("%02X%02X%02X%02X", magicHeaderBytes[0], magicHeaderBytes[1], magicHeaderBytes[2], magicHeaderBytes[3]);

        // PDF 포맷 매직 넘버 식별 (25 50 44 46 = %PDF)
        if (hexSignature.startsWith("25504446")) {
            logger.info("      └─ PDF 바이너리 포맷 감지. 외부 어댑터 포트를 통한 PDF 텍스트 추출을 위임합니다.");

            // 외부 서드파티 어댑터 포트 호출 (의존성 역전 원칙 적용)
            String extractedBinaryText = textExtractorPort.extractBinaryDocumentText(filePath);

            return new DocumentExtractionResult("PDF", extractedBinaryText != null ? extractedBinaryText : "");
        }

        // 특정 바이너리 시그니처가 없으면 기본적으로 UTF-8 평문 텍스트(TXT, CSV, MD)로 간주하고 전체 문자열을 읽어들입니다.
        String fullPlainText = Files.readString(filePath, StandardCharsets.UTF_8);

        String fallbackFormat = filePath.getFileName().toString().endsWith(".csv") ? "CSV" : "TXT";
        return new DocumentExtractionResult(fallbackFormat, fullPlainText);
    }

    // =========================================================================
    // 💡 [전략 구현체] 기존 하드코딩 로직을 캡슐화하여 팩토리(Factory) 제공용으로 승격시킨 기본 청킹 전략
    // =========================================================================

    // [1. 한글 상세 주석]
    // 💡 [안전망 보존] 하드코딩을 제거하되, 기존의 기능이 목업(Mock) 처리되는 것을 방지하기 위해 
    // 기존의 슬라이딩 윈도우 로직을 온전한 하나의 `PolymorphicChunkingStrategy` 인터페이스 구현체로 승격시켜 클래스 내부에 보존합니다.
    // [2. 영문 상세 주석]
    // 💡 [Preserving Safety Net] Removes hardcoding, but to prevent the functionality from becoming a mock, 
    // the existing sliding window logic is elevated to a full `PolymorphicChunkingStrategy` implementation and preserved intact within the class.

    /**
     * [기본 룰셋: 슬라이딩 윈도우 기반 시맨틱 청킹]
     * 전략 제공자(Provider)가 문서별 특수 룰셋을 하달하지 않을 때 범용(Fallback)으로 쓰이는 가장 안정적인 청킹 전략 클래스입니다.
     */
    public static class DefaultSlidingWindowChunkingStrategy implements PolymorphicChunkingStrategy {

        private final int maxCharCount;
        private final int overlapCharCount;

        public DefaultSlidingWindowChunkingStrategy(int maxCharCount, int overlapCharCount) {
            this.maxCharCount = maxCharCount;
            this.overlapCharCount = overlapCharCount;
        }

        @Override
        public List<SemanticChunkPayload> executeChunking(String documentUuid, String rawText) {
            if (rawText == null || rawText.isBlank()) {
                return Collections.emptyList();
            }

            List<SemanticChunkPayload> chunkPayloadList = new ArrayList<>();
            int chunkSequenceIndex = 0;

            // 1차 해체: 문단(Paragraph)을 기준으로 분해 (정규식을 통해 연속된 개행(\n)을 문단 단위로 간주)
            String[] paragraphFragments = rawText.split("\\n\\s*\\n+");

            StringBuilder currentChunkBuffer = new StringBuilder(maxCharCount + 200);

            for (String paragraph : paragraphFragments) {
                String refinedParagraph = paragraph.trim();
                if (refinedParagraph.isEmpty())
                    continue;

                // 단일 문단 자체가 설정된 한계치를 초과할 경우의 특수 제어
                if (refinedParagraph.length() > maxCharCount) {
                    if (currentChunkBuffer.length() > 0) {
                        chunkPayloadList.add(new SemanticChunkPayload(documentUuid, chunkSequenceIndex++, currentChunkBuffer.toString().trim()));
                        currentChunkBuffer.setLength(0);
                    }
                    chunkSequenceIndex = executeForcedSlidingSplitAndEmit(documentUuid, refinedParagraph, chunkPayloadList, chunkSequenceIndex);
                    continue;
                }

                // 현재 버퍼의 크기가 초과될 경우, 버퍼를 사출(Emit)하고 이전 청크의 꼬리를 물고 넘어감(Overlap)
                if (currentChunkBuffer.length() + refinedParagraph.length() > maxCharCount) {
                    chunkPayloadList.add(new SemanticChunkPayload(documentUuid, chunkSequenceIndex++, currentChunkBuffer.toString().trim()));
                    String tailOverlapContext = extractTailOverlapContext(currentChunkBuffer.toString());

                    currentChunkBuffer.setLength(0);
                    currentChunkBuffer.append(tailOverlapContext).append("\n\n").append(refinedParagraph).append("\n\n");
                } else {
                    currentChunkBuffer.append(refinedParagraph).append("\n\n");
                }
            }

            if (currentChunkBuffer.length() > 0) {
                chunkPayloadList.add(new SemanticChunkPayload(documentUuid, chunkSequenceIndex, currentChunkBuffer.toString().trim()));
            }

            return Collections.unmodifiableList(chunkPayloadList);
        }

        private String extractTailOverlapContext(String pastChunkText) {
            if (pastChunkText.length() <= overlapCharCount)
                return pastChunkText;
            int splitStartPoint = pastChunkText.length() - overlapCharCount;

            // 문맥 단절을 최소화하기 위해 공백 단위로 스마트하게 꼬리를 잘라냅니다.
            int smartSplitPoint = pastChunkText.indexOf(' ', splitStartPoint);
            if (smartSplitPoint == -1)
                smartSplitPoint = splitStartPoint;

            return "..." + pastChunkText.substring(smartSplitPoint).trim();
        }

        private int executeForcedSlidingSplitAndEmit(
                String documentUuid,
                String giantParagraph,
                List<SemanticChunkPayload> chunkList,
                int currentSequence) {

            int totalStringLength = giantParagraph.length();
            int scanPointer = 0;

            while (scanPointer < totalStringLength) {
                int endPointer = Math.min(scanPointer + maxCharCount, totalStringLength);

                if (endPointer < totalStringLength) {
                    int spacePosition = giantParagraph.lastIndexOf(' ', endPointer);
                    if (spacePosition > scanPointer)
                        endPointer = spacePosition;
                }

                String splitTextFragment = giantParagraph.substring(scanPointer, endPointer).trim();
                if (!splitTextFragment.isEmpty()) {
                    chunkList.add(new SemanticChunkPayload(documentUuid, currentSequence++, splitTextFragment));
                }

                scanPointer = endPointer - overlapCharCount;
                if (scanPointer < 0)
                    scanPointer = 0;
                if (scanPointer == endPointer)
                    break;
            }
            return currentSequence;
        }
    }
}

/*
 * =============================================================================
 * 🧠 [이론적 배경 및 시스템 아키텍처 (Theoretical Background & System Architecture)]
 * 
 * 1. 💡 다형성 청킹 전략(Strategy Pattern)의 위력:
 * 문서를 해체할 때 단순히 `1000자`라는 맹목적인 상수에 의존하는 것은 모든 환자에게 동일한 감기약을 처방하는 것과 같습니다.
 * 트위터나 뉴스 헤드라인(단문)은 빠르고 짧은 문맥 단위로 끊어야 모델의 정밀도가 올라가며,
 * 학술 논문이나 소설(장문)은 더 큰 윈도우 크기와 무거운 오버랩을 주거나 BPE(Byte Pair Encoding) 토큰 기반으로 쪼개야
 * LLM의 어텐션(Attention)이 분산되지 않고 유지됩니다.
 * 개선된 V6.0 파이프라인은 `ChunkingStrategyProvider`라는 전략 팩토리(Strategy Factory) 인터페이스를 통해
 * 런타임에 동적인 룰셋 객체를 외부로부터 주입(DI) 받습니다.
 * 이로 인해 문서 해체 모듈은 문헌의 메타데이터(파일 형식, 도메인 모달리티 등)를 분석하여 스스로 가장 적합한 
 * 톱날 알고리즘(Strategy)으로 교체 장착한 뒤, 완벽한 의미론적 텍스트 파편화를 성취합니다.
 * 
 * 2. 문서 해체와 시맨틱 청킹 (Semantic Chunking)의 기하학:
 * 최신 AI 모델(LLM)조차도 테라바이트급의 방대한 문서를 한 번에 입력창에 구겨 넣지 못합니다 (컨텍스트 윈도우 한계).
 * RAG(검색 증강 생성) 시스템에서 개발자들이 가장 흔히 저지르는 실수는 텍스트를 무지성으로 `substring(0, 1000)` 처럼 잘라버리는 행위입니다.
 * 이 경우 문장의 허리가 물리적으로 끊어지며 AI의 어텐션(Attention) 메커니즘이 앞뒤 문맥을 연결하지 못해 환각(Hallucination)에 빠집니다.
 * 이 모듈 내부에 캡슐화된 기본 전략(`DefaultSlidingWindowChunkingStrategy`)은 문단(Paragraph)의 의미를 존중하며 문자열을 조립하되,
 * 한계치에 다다르면 직전 청크의 꼬리(Overlap)를 일부 뜯어와 다음 청크의 머리에 붙입니다.
 * 이는 신경망의 시냅스(Synapse)가 끊어지지 않도록 데이터의 논리적 연결고리(Topological Link)를 
 * 영원히 수호하는 궁극의 시맨틱 보존형 RAG 알고리즘입니다.
 * 
 * 3. 포트 앤 어댑터 아키텍처 (Port and Adapter Architecture)의 헥사고날(Hexagonal) 설계 철학:
 * 만약 이 모듈 최상단에 `import org.apache.pdfbox.pdmodel.PDDocument`를 직접 선언했다면,
 * 통합 OS의 순수한 L1 코어 파이프라인은 외부 서드파티 라이브러리의 버전 업데이트나 런타임 취약점(CVE) 버그에 
 * 직접적으로 의존성이 묶이고 오염(Corruption)되었을 것입니다.
 * 이 모듈은 오직 `UnstructuredTextExtractorPort` 라는 인터페이스형 함수 계약(Contract)만을 정의하여 내부 코어를 철저히 방어(ACL)합니다.
 * PDF 파싱과 같은 복잡한 외부 종속 로직은 상위 L5 관제탑이 외부 어댑터 껍데기(Adapter) 클래스로 감싸서 런타임에 주입(DI)하게 됩니다.
 * 내일 당장 PDF 파서 엔진 라이브러리가 완전히 다른 것으로 교체되더라도, 이 코어 모듈의 소스 코드는 단 한 줄도 수정될 필요가 없는 극강의 유지보수성을 획득했습니다.
 * =============================================================================
 */