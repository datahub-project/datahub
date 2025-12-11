/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.spark.model;

import datahub.event.MetadataChangeProposalWrapper;
import java.util.Date;
import java.util.List;
import lombok.Data;

@Data
public abstract class LineageEvent {
  private final String master;
  private final String appName;
  private final String appId;
  private final long time;

  public abstract List<MetadataChangeProposalWrapper> asMetadataEvents();

  public String timeStr() {
    return new Date(getTime()).toInstant().toString();
  }
}
