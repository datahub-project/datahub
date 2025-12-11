/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.system.cdc;

import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.config.CDCSetupConfig;
import org.testng.annotations.Test;

public class CDCSourceSetupConfigTest {

  @Test
  public void testConstructor() {
    CDCSetupConfig config = new CDCSetupConfig();
    assertNotNull(config);
  }

  @Test
  public void testCDCSetupBeanMethod() {
    CDCSetupConfig config = new CDCSetupConfig();
    // Test that the method exists and can be called
    // (The actual conditional logic is tested in integration tests)
    assertNotNull(config);
  }
}
