/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.client.s3;

import datahub.event.EventFormatter;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3EmitterConfig {
  @Builder.Default @lombok.NonNull String bucketName = null;
  @Builder.Default String pathPrefix = null;
  @Builder.Default String fileName = null;

  @Builder.Default
  EventFormatter eventFormatter = new EventFormatter(EventFormatter.Format.PEGASUS_JSON);

  @Builder.Default String region = null;
  @Builder.Default String endpoint = null;
  @Builder.Default String accessKey = null;
  @Builder.Default String secretKey = null;
  @Builder.Default String sessionToken = null;
  @Builder.Default String profileFile = null;
  @Builder.Default String profileName = null;
}
