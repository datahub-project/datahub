/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.test.testing.urn;

import com.linkedin.data.template.Custom;

public class PizzaUrnCoercer extends BaseUrnCoercer<PizzaUrn> {
  private static final boolean REGISTER_COERCER =
      Custom.registerCoercer(new PizzaUrnCoercer(), PizzaUrn.class);
}
