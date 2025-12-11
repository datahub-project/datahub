/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.service;

import static org.mockito.Mockito.mock;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RestrictedServiceTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:foo,bar,baz1)");
  private static final String ENCRYPED_DATASET_URN = "12d3as456tgs";
  private static final Urn TEST_RESTRICTED_URN =
      UrnUtils.getUrn(String.format("urn:li:restricted:%s", ENCRYPED_DATASET_URN));

  @Test
  private void testEncryptRestrictedUrn() throws Exception {
    SecretService mockSecretService = mock(SecretService.class);
    Mockito.when(mockSecretService.encrypt(TEST_DATASET_URN.toString()))
        .thenReturn(ENCRYPED_DATASET_URN);
    final RestrictedService service = new RestrictedService(mockSecretService);

    Assert.assertEquals(service.encryptRestrictedUrn(TEST_DATASET_URN), TEST_RESTRICTED_URN);
  }

  @Test
  private void testDecryptRestrictedUrn() throws Exception {
    SecretService mockSecretService = mock(SecretService.class);
    Mockito.when(mockSecretService.decrypt(ENCRYPED_DATASET_URN))
        .thenReturn(TEST_DATASET_URN.toString());
    final RestrictedService service = new RestrictedService(mockSecretService);

    Assert.assertEquals(service.decryptRestrictedUrn(TEST_RESTRICTED_URN), TEST_DATASET_URN);
  }
}
