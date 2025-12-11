/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.form;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.service.FormService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class FormServiceFactory {
  @Bean(name = "formService")
  @Scope("singleton")
  @Nonnull
  protected FormService getInstance(final SystemEntityClient entityClient) throws Exception {
    return new FormService(entityClient);
  }
}
