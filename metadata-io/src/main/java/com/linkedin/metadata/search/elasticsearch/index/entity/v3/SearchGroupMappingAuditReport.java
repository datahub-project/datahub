package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Aggregate V3 registry audit across all search groups. */
public class SearchGroupMappingAuditReport {
  private final List<SearchGroupMappingAudit> searchGroupAudits;

  private SearchGroupMappingAuditReport(
      @Nonnull final Collection<SearchGroupMappingAudit> searchGroupAudits) {
    this.searchGroupAudits =
        Collections.unmodifiableList(
            searchGroupAudits.stream()
                .sorted((left, right) -> left.getSearchGroup().compareTo(right.getSearchGroup()))
                .collect(Collectors.toList()));
  }

  @Nonnull
  public static SearchGroupMappingAuditReport create(
      @Nonnull final Collection<SearchGroupMappingAudit> searchGroupAudits) {
    return new SearchGroupMappingAuditReport(searchGroupAudits);
  }

  @Nonnull
  public List<SearchGroupMappingAudit> getSearchGroupAudits() {
    return searchGroupAudits;
  }

  public int getSearchGroupCount() {
    return searchGroupAudits.size();
  }

  public int getTotalEntityCount() {
    return searchGroupAudits.stream().mapToInt(SearchGroupMappingAudit::getEntityCount).sum();
  }

  public boolean isEvaluated() {
    return !searchGroupAudits.isEmpty();
  }

  @Nonnull
  public List<String> getFailingSearchGroups() {
    return searchGroupAudits.stream()
        .filter(audit -> !audit.isPassing())
        .map(SearchGroupMappingAudit::getSearchGroup)
        .collect(Collectors.toList());
  }

  public boolean isPassing() {
    return isEvaluated() && getFailingSearchGroups().isEmpty();
  }

  @Nonnull
  public Map<String, Object> toMap() {
    final Map<String, Object> result = new TreeMap<>();
    result.put("searchGroupCount", getSearchGroupCount());
    result.put("totalEntityCount", getTotalEntityCount());
    result.put("evaluated", isEvaluated());
    result.put("passing", isPassing());
    result.put("failingSearchGroups", getFailingSearchGroups());
    result.put(
        "searchGroups",
        searchGroupAudits.stream()
            .map(SearchGroupMappingAudit::toMap)
            .collect(Collectors.toList()));
    return Collections.unmodifiableMap(result);
  }
}
