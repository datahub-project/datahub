/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.businessattribute;

import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.BusinessAttributeService;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
public class BusinessAttributeServiceFactory {
  @Bean(name = "businessAttributeService")
  @Scope("singleton")
  @Nonnull
  protected BusinessAttributeService businessAttributeService(
      final EntityService<?> entityService) {
    return new BusinessAttributeService(entityService);
  }
}
