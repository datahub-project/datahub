/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.telemetry;

import com.mixpanel.mixpanelapi.MixpanelAPI;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class MixpanelApiFactory {
  private static final String EVENTS_ENDPOINT = "https://track.datahubproject.io/mp/track";
  private static final String PEOPLE_ENDPOINT = "https://track.datahubproject.io/mp/engage";

  @Bean(name = "mixpanelApi")
  @ConditionalOnProperty("telemetry.enabledServer")
  @Scope("singleton")
  protected MixpanelAPI getInstance() throws Exception {
    return new MixpanelAPI(EVENTS_ENDPOINT, PEOPLE_ENDPOINT);
  }
}
