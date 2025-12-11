/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package utils;

import javax.annotation.Nonnull;

/** Utility functions for Search */
public class SearchUtil {

  private SearchUtil() {
    // utility class
  }

  /**
   * Returns the string with the forward slash escaped More details on reserved characters in
   * Elasticsearch can be found at,
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
   *
   * @param input
   * @return
   */
  @Nonnull
  public static String escapeForwardSlash(@Nonnull String input) {
    if (input.contains("/")) {
      input = input.replace("/", "\\/");
    }
    return input;
  }
}
