/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.resource;

import com.linkedin.common.urn.Urn;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ResourceReference {
  /** The urn of an entity */
  Urn urn;

  /** The type of the SubResource */
  SubResourceType subResourceType;

  /** The subresource being targeted */
  String subResource;
}
