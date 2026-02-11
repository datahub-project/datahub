package com.linkedin.metadata.search.rescore;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import lombok.Builder;
import lombok.Value;

/**
 * Result of rescoring a single document with full explanation.
 *
 * <p>This is attached to SearchEntity.extraFields["_rescoreExplain"] for debugging.
 */
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RescoreResult {
  /** Document URN/ID */
  String documentId;

  /** Original BM25 score from Stage 1 (ES) */
  double bm25Score;

  /** Final score after Stage 2 rescoring */
  double finalScore;

  /** The formula that was evaluated */
  String formula;

  /** All signal values with their contributions (ordered) */
  Map<String, SignalValue> signals;
}
