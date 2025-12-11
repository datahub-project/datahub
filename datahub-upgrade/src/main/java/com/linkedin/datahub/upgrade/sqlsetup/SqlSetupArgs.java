/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.sqlsetup;

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.Value;

@Value
@AllArgsConstructor
@ToString(exclude = {"cdcPassword", "createUserPassword"})
public class SqlSetupArgs {
  boolean createTables;
  boolean createDatabase; // PostgreSQL only
  boolean createUser;
  boolean iamAuthEnabled;
  DatabaseType dbType; // mysql or postgres
  boolean cdcEnabled;
  String cdcUser;
  String cdcPassword;
  String createUserUsername;
  String createUserPassword; // If null, IAM authentication is used
  String host;
  int port;
  String databaseName;
}
