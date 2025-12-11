/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.util;

import com.linkedin.common.urn.VersionedUrn;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

public class VersionedUrnCoercer implements DirectCoercer<VersionedUrn> {
  static {
    Custom.registerCoercer(new VersionedUrnCoercer(), VersionedUrn.class);
  }

  @Override
  public Object coerceInput(VersionedUrn object) throws ClassCastException {
    return object.toString();
  }

  @Override
  public VersionedUrn coerceOutput(Object object) throws TemplateOutputCastException {
    String pairStr = (String) object;
    String[] split = pairStr.split(" , ");
    return VersionedUrn.of(split[0].substring(1), split[1].substring(0, split[1].length() - 1));
  }
}
