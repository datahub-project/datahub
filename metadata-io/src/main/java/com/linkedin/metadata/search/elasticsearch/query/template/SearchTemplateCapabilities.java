package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalBridge;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Sections this process can run. OSS advertises keyword retrieval and engine rescore. A stored
 * section outside that set fails the search after parent merge.
 */
public final class SearchTemplateCapabilities {
  public static final Set<String> PRESETS = Set.of("none", "usage", "usageAndMatchTier");
  public static final Set<String> SIZERS = Set.of("fixed", "tokenCurve");
  public static final Set<String> FUSIONS =
      Set.of("interleave", "weightedBlend", "fillFreeSlots", "keywordPreferred");
  public static final Set<String> FACETS = Set.of("firstMember", "patchFromMembers", "candidates");
  public static final Set<String> MODES = Set.of("keyword", "vector");

  private SearchTemplateCapabilities() {}

  public static void requireSupported(
      @Nonnull MergedSearchTemplate template, boolean documentRerank) {
    requireSupported(template, documentRerank, false);
  }

  /**
   * Scroll drops {@code knn}, so a knn section does not fail a scroll. On a search, document rerank
   * is advertised only when the request includes the Document entity.
   */
  public static void requireSupported(
      @Nonnull MergedSearchTemplate template, boolean documentRerank, boolean scroll) {
    SearchTemplateRanking ranking = template.getRanking();
    if (ranking != null) {
      String preset = ranking.hasPreset() ? ranking.getPreset() : null;
      if (preset != null && !preset.isBlank() && !"none".equals(preset)) {
        throw unsupported("ranking preset " + preset);
      }
      String sizer = ranking.hasWindowSizer() ? ranking.getWindowSizer() : null;
      if (sizer != null && !sizer.isBlank() && !"fixed".equals(sizer)) {
        throw unsupported("ranking windowSizer " + sizer);
      }
    }
    if (template.getKnn() != null && !scroll && !documentRerank) {
      throw unsupported("knn");
    }
    if (template.getLadder() != null
        && template.getLadder().hasJoin()
        && template.getLadder().getJoin() != null
        && "merge".equalsIgnoreCase(template.getLadder().getJoin())) {
      throw unsupported("merge join");
    }
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    if (retrieval == null) {
      return;
    }
    String mode = retrieval.hasMode() ? retrieval.getMode() : null;
    if (mode != null && !mode.isBlank() && !"keyword".equals(mode)) {
      throw unsupported("retrieval mode " + mode);
    }
    SearchTemplateRetrievalBridge bridge = retrieval.hasBridge() ? retrieval.getBridge() : null;
    if (bridge != null && !SearchTemplateLoader.emptyRetrieval(retrievalWithBridge(bridge))) {
      throw unsupported("retrieval bridge");
    }
  }

  /**
   * Bridge presence is the capability. Other retrieval fields may be unset, so emptiness is checked
   * on a retrieval that contains only the bridge.
   */
  @Nonnull
  private static SearchTemplateRetrieval retrievalWithBridge(
      @Nonnull SearchTemplateRetrievalBridge bridge) {
    SearchTemplateRetrieval retrieval = new SearchTemplateRetrieval();
    retrieval.setBridge(bridge);
    return retrieval;
  }

  @Nonnull
  private static SearchTemplateException unsupported(@Nonnull String section) {
    return new SearchTemplateException("Search template section is not supported: " + section);
  }
}
