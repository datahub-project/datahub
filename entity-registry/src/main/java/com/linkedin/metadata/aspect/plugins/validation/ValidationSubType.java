/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.plugins.validation;

public enum ValidationSubType {
  // A validation exception is thrown
  VALIDATION,
  // A failed precondition is thrown if the header constraints are not met
  PRECONDITION,
  // Exclude from processing further
  FILTER,
  // An authorization validation
  AUTHORIZATION
}
