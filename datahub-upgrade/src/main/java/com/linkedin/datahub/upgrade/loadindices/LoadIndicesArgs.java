package com.linkedin.datahub.upgrade.loadindices;

import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import io.ebean.annotation.TxIsolation;
import java.util.Collection;
import lombok.Data;

@Data
public class LoadIndicesArgs {
  public int batchSize;
  public int limit;
  public String urnLike;
  public Long lePitEpochMs;
  public Long gePitEpochMs;
  public Collection<String> aspectNames;
  public String lastUrn;
  // Baseline is the bootstrap default: an initial population, whose events may carry an entity's
  // complete stored state and a physical target index. Catch-up marks a replay over a window of
  // changes; its events carry neither.
  public String v3BackfillMode = BackfillHeaders.MODE_BASELINE;
  // Escape hatch (default off): when true, an aspect that fails MCL conversion is counted and
  // skipped instead of failing the whole run. Off keeps the fail-loud default so a corrupt aspect
  // is not silently dropped from the rebuilt index.
  public boolean skipConversionErrors = false;

  /**
   * Index the V3 strategy writes into instead of the search group's alias. Set by the fill when it
   * rebuilds into a next physical index that the alias does not point at yet.
   */
  public String v3TargetIndex;

  /**
   * Wall-clock deadline. The run stops after the batch that crosses it, once that batch has been
   * written and reported, so a resume continues from a consistent cursor.
   */
  public Long deadlineEpochMs;

  /**
   * Isolation for the aspect scan; null takes the store's default. The operator command trades
   * dirty reads for scan speed, a fill running beside live writes reads committed rows only.
   */
  public TxIsolation txIsolation;

  public LoadIndicesArgs clone() {
    LoadIndicesArgs cloned = new LoadIndicesArgs();
    cloned.batchSize = this.batchSize;
    cloned.limit = this.limit;
    cloned.urnLike = this.urnLike;
    cloned.lePitEpochMs = this.lePitEpochMs;
    cloned.gePitEpochMs = this.gePitEpochMs;
    cloned.aspectNames = this.aspectNames;
    cloned.lastUrn = this.lastUrn;
    cloned.v3BackfillMode = this.v3BackfillMode;
    cloned.skipConversionErrors = this.skipConversionErrors;
    cloned.v3TargetIndex = this.v3TargetIndex;
    cloned.deadlineEpochMs = this.deadlineEpochMs;
    cloned.txIsolation = this.txIsolation;
    return cloned;
  }
}
