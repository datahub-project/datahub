/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.search.features;

import com.linkedin.metadata.search.SearchEntity;
import java.util.List;

/** Interface for extractors that extract Features for each entity returned by search */
public interface FeatureExtractor {
  /** Return the extracted features for each entity returned by search */
  List<Features> extractFeatures(List<SearchEntity> entities);
}
