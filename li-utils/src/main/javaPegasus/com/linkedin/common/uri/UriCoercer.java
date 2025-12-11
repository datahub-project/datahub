/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.common.uri;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

public class UriCoercer implements DirectCoercer<Uri> {
  private static final boolean REGISTER_COERCER =
      Custom.registerCoercer(new UriCoercer(), Uri.class);

  @Override
  public Object coerceInput(Uri object) throws ClassCastException {
    return object.toString();
  }

  @Override
  public Uri coerceOutput(Object object) throws TemplateOutputCastException {
    return new Uri((String) object);
  }
}
