package com.linkedin.metadata.aspect.patch.builder.subtypesupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;

/**
 * Used for supporting intermediate subtypes when constructing a patch for an aspect that includes
 * complex objects.
 *
 * @param <T> The parent patch builder type
 */
public interface IntermediatePatchBuilder<T extends AbstractMultiFieldPatchBuilder<T>> {

  /** Convenience method to return parent patch builder in functional callstack */
  T getParent();

  /**
   * Exposes subpath values to parent patch builder in Op, Path, Value triples. Should usually only
   * be called by the parent patch builder class when constructing the path values.
   */
  List<ImmutableTriple<String, String, JsonNode>> getSubPaths();
}
