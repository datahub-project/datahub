/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder
public class AuthorizedActors {
  String privilege;
  List<Urn> users;
  List<Urn> groups;
  List<Urn> roles;
  boolean allUsers;
  boolean allGroups;
}
