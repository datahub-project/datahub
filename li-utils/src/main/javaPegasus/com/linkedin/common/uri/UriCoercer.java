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
