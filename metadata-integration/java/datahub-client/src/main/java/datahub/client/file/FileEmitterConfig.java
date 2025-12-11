/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.client.file;

import datahub.event.EventFormatter;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FileEmitterConfig {
  @Builder.Default @lombok.NonNull private final String fileName = null;

  @Builder.Default
  private final EventFormatter eventFormatter =
      new EventFormatter(EventFormatter.Format.PEGASUS_JSON);
}
