package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.data.template.FloatMap;
import com.linkedin.data.template.StringArray;
import com.linkedin.search.template.SearchTemplateProfiles;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Write-time shape of {@code profiles} and the root-first merge of the parent chain into one
 * selection. A null selection resolves each entity to its registry default.
 */
public final class SearchTemplateProfilesPolicy {
  private SearchTemplateProfilesPolicy() {}

  /** Rejects a selection that is invalid in every build. */
  public static void check(@Nonnull SearchTemplateProfiles profiles) {
    boolean useDefault = profiles.hasUseDefault() && profiles.isUseDefault() != null;
    boolean replace = profiles.hasReplace() && profiles.getReplace() != null;
    Set<String> add = ids("add", profiles.hasAdd() ? profiles.getAdd() : null);
    Set<String> remove = ids("remove", profiles.hasRemove() ? profiles.getRemove() : null);
    if (useDefault) {
      if (!profiles.isUseDefault()) {
        throw new SearchTemplateException("Search template profiles.useDefault must be true");
      }
      if (replace
          || !add.isEmpty()
          || !remove.isEmpty()
          || (profiles.hasOverride() && profiles.getOverride() != null)) {
        throw new SearchTemplateException(
            "Search template profiles.useDefault cannot be combined with other operations");
      }
      return;
    }
    if (replace) {
      if (profiles.getReplace().isEmpty()) {
        throw new SearchTemplateException("Search template profiles.replace is empty");
      }
      ids("replace", profiles.getReplace());
    }
    for (String id : add) {
      if (remove.contains(id)) {
        throw new SearchTemplateException(
            "Search template profiles names " + id + " in both add and remove");
      }
    }
    if (profiles.hasOverride() && profiles.getOverride() != null) {
      for (Map.Entry<String, Float> entry : profiles.getOverride().entrySet()) {
        if (entry.getKey() == null || entry.getKey().isBlank()) {
          throw new SearchTemplateException("Search template profiles.override has an empty id");
        }
        Float weight = entry.getValue();
        if (weight == null || !Float.isFinite(weight) || weight <= 0f) {
          throw new SearchTemplateException(
              "Search template profiles.override weight must be positive: " + entry.getKey());
        }
      }
    }
  }

  /**
   * Applies {@code child} to the inherited selection. {@code useDefault} drops it, and a section
   * with no operation keeps it. {@code replace} sets the list, then {@code add} and {@code remove}
   * patch that list, the inherited list, or, when none was set, the entity default. {@code
   * override} weights merge with the child's winning.
   */
  @Nullable
  public static SearchTemplateProfiles merge(
      @Nullable SearchTemplateProfiles parent, @Nonnull SearchTemplateProfiles child) {
    if (child.hasUseDefault() && Boolean.TRUE.equals(child.isUseDefault())) {
      return null;
    }
    if (SearchTemplateLoader.emptyProfiles(child)) {
      return parent;
    }
    Set<String> replace = null;
    Set<String> add = new LinkedHashSet<>();
    Set<String> remove = new LinkedHashSet<>();
    Map<String, Float> override = new LinkedHashMap<>();
    if (parent != null) {
      if (parent.hasReplace() && parent.getReplace() != null) {
        replace = new LinkedHashSet<>(parent.getReplace());
      }
      if (parent.hasAdd() && parent.getAdd() != null) {
        add.addAll(parent.getAdd());
      }
      if (parent.hasRemove() && parent.getRemove() != null) {
        remove.addAll(parent.getRemove());
      }
      if (parent.hasOverride() && parent.getOverride() != null) {
        override.putAll(parent.getOverride());
      }
    }
    if (child.hasReplace() && child.getReplace() != null) {
      replace = new LinkedHashSet<>(child.getReplace());
      add.clear();
      remove.clear();
    }
    List<String> childAdd = child.hasAdd() && child.getAdd() != null ? child.getAdd() : List.of();
    List<String> childRemove =
        child.hasRemove() && child.getRemove() != null ? child.getRemove() : List.of();
    if (replace != null) {
      replace.removeAll(childRemove);
      replace.addAll(childAdd);
    } else {
      add.removeAll(childRemove);
      add.addAll(childAdd);
      remove.removeAll(childAdd);
      remove.addAll(childRemove);
    }
    if (child.hasOverride() && child.getOverride() != null) {
      override.putAll(child.getOverride());
    }
    SearchTemplateProfiles merged = new SearchTemplateProfiles();
    if (replace != null) {
      merged.setReplace(new StringArray(replace));
    }
    if (!add.isEmpty()) {
      merged.setAdd(new StringArray(add));
    }
    if (!remove.isEmpty()) {
      merged.setRemove(new StringArray(remove));
    }
    if (!override.isEmpty()) {
      merged.setOverride(new FloatMap(override));
    }
    return merged;
  }

  @Nonnull
  private static Set<String> ids(@Nonnull String operation, @Nullable List<String> values) {
    Set<String> ids = new LinkedHashSet<>();
    if (values == null) {
      return ids;
    }
    for (String id : values) {
      if (id == null || id.isBlank()) {
        throw new SearchTemplateException(
            "Search template profiles." + operation + " has an empty id");
      }
      if (!ids.add(id)) {
        throw new SearchTemplateException(
            "Search template profiles." + operation + " names " + id + " more than once");
      }
    }
    return ids;
  }
}
