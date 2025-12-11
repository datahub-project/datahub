/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.test.testing.urn;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;

public final class SingleAspectEntityUrn extends Urn {

  private static final String ENTITY_TYPE = "entitySingleAspectEntity";

  public SingleAspectEntityUrn(long id) throws URISyntaxException {
    super(ENTITY_TYPE, Long.toString(id));
  }

  public static SingleAspectEntityUrn createFromString(String rawUrn) throws URISyntaxException {
    return new SingleAspectEntityUrn(Urn.createFromString(rawUrn).getIdAsInt());
  }
}
