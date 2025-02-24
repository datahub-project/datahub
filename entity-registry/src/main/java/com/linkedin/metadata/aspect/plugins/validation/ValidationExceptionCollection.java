package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Used to store a collection of exceptions, keyed by the URN/AspectName pair */
public class ValidationExceptionCollection
    extends HashMap<Pair<Urn, String>, Set<AspectValidationException>> {

  private final Set<Integer> failedHashCodes;
  private final Set<Integer> filteredHashCodes;

  public ValidationExceptionCollection() {
    super();
    this.failedHashCodes = new HashSet<>();
    this.filteredHashCodes = new HashSet<>();
  }

  public boolean hasFatalExceptions() {
    return !failedHashCodes.isEmpty();
  }

  public static ValidationExceptionCollection newCollection() {
    return new ValidationExceptionCollection();
  }

  public void addException(AspectValidationException exception) {
    super.computeIfAbsent(exception.getAspectGroup(), key -> new HashSet<>()).add(exception);
    if (!AspectValidationException.SubType.FILTER.equals(exception.getSubType())) {
      failedHashCodes.add(exception.getItem().hashCode());
    } else {
      filteredHashCodes.add(exception.getItem().hashCode());
    }
  }

  public void addException(BatchItem item, String message) {
    addException(item, message, null);
  }

  public void addException(BatchItem item, String message, Exception ex) {
    addException(AspectValidationException.forItem(item, message, ex));
  }

  public Stream<AspectValidationException> streamAllExceptions() {
    return values().stream().flatMap(Collection::stream);
  }

  public <T extends BatchItem> Collection<T> successful(Collection<T> items) {
    return streamSuccessful(items.stream()).collect(Collectors.toList());
  }

  public <T extends BatchItem> Stream<T> streamSuccessful(Stream<T> items) {
    return items.filter(
        i -> !failedHashCodes.contains(i.hashCode()) && !filteredHashCodes.contains(i.hashCode()));
  }

  public <T extends BatchItem> Collection<T> exceptions(Collection<T> items) {
    return streamExceptions(items.stream()).collect(Collectors.toList());
  }

  public <T extends BatchItem> Stream<T> streamExceptions(Stream<T> items) {
    return items.filter(i -> failedHashCodes.contains(i.hashCode()));
  }

  @Override
  public String toString() {
    return String.format(
        "ValidationExceptionCollection{%s}",
        entrySet().stream()
            // sort by entity/aspect
            .sorted(Comparator.comparing(p -> p.getKey().toString()))
            .map(
                e ->
                    String.format(
                        "EntityAspect:%s Exceptions: %s", e.getKey().toString(), e.getValue()))
            .collect(Collectors.joining("; ")));
  }
}
