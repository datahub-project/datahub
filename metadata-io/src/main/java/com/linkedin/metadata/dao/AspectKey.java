/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import lombok.NonNull;
import lombok.Value;

/** A value class that holds the components of a key for metadata retrieval. */
@Value
public class AspectKey<URN extends Urn, ASPECT extends RecordTemplate> {

  @NonNull Class<ASPECT> aspectClass;

  @NonNull URN urn;

  @NonNull Long version;
}
