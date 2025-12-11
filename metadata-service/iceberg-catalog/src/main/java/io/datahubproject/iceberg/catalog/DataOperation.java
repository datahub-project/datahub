/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.authorization.PoliciesConfig.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.List;

public enum DataOperation {
  READ_ONLY(
      DATA_READ_ONLY_PRIVILEGE,
      DATA_MANAGE_VIEWS_PRIVILEGE,
      DATA_READ_WRITE_PRIVILEGE,
      DATA_MANAGE_TABLES_PRIVILEGE),

  READ_WRITE(DATA_READ_WRITE_PRIVILEGE, DATA_MANAGE_TABLES_PRIVILEGE),
  MANAGE_VIEWS(DATA_MANAGE_VIEWS_PRIVILEGE, DATA_MANAGE_TABLES_PRIVILEGE),
  MANAGE_TABLES(DATA_MANAGE_TABLES_PRIVILEGE),
  MANAGE_NAMESPACES(DATA_MANAGE_NAMESPACES_PRIVILEGE),

  LIST(DATA_LIST_ENTITIES_PRIVILEGE);

  public final List<PoliciesConfig.Privilege> ascendingPrivileges;
  public final List<PoliciesConfig.Privilege> descendingPrivileges;

  DataOperation(PoliciesConfig.Privilege... ascendingPrivileges) {
    this.ascendingPrivileges = ImmutableList.copyOf(ascendingPrivileges);
    this.descendingPrivileges = Lists.reverse(this.ascendingPrivileges);
  }
}
