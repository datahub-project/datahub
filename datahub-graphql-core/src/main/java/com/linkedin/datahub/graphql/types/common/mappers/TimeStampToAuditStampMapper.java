/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.TimeStamp;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuditStamp;
import javax.annotation.Nullable;

public class TimeStampToAuditStampMapper {

  public static final TimeStampToAuditStampMapper INSTANCE = new TimeStampToAuditStampMapper();

  public static AuditStamp map(
      @Nullable final QueryContext context, @Nullable final TimeStamp input) {
    if (input == null) {
      return null;
    }
    final AuditStamp result = new AuditStamp();
    result.setTime(input.getTime());
    if (input.hasActor()) {
      result.setActor(input.getActor().toString());
    }
    return result;
  }
}
