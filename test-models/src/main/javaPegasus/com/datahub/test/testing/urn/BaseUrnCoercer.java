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
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public abstract class BaseUrnCoercer<T extends Urn> implements DirectCoercer<T> {
  public BaseUrnCoercer() {}

  public Object coerceInput(T object) throws ClassCastException {
    return object.toString();
  }

  @SuppressWarnings("unchecked")
  public T coerceOutput(Object object) throws TemplateOutputCastException {
    try {
      return (T) Urn.createFromString((String) object);
    } catch (URISyntaxException e) {
      throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
    }
  }
}
