/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.schematron.utils;

/** Constants used throughout the schema conversion process. */
public final class Constants {
  private Constants() {}

  public static final String ADD_TAG_OPERATION = "ADD_TAG";
  public static final String ADD_TERM_OPERATION = "ADD_TERM";

  public static final String TAG_URN_PREFIX = "urn:li:tag:";
  public static final String TERM_URN_PREFIX = "urn:li:glossaryTerm:";
}
