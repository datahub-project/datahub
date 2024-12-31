package com.linkedin.common.url;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

public class UrlCoercer implements DirectCoercer<Url> {
  private static final boolean REGISTER_COERCER =
      Custom.registerCoercer(new UrlCoercer(), Url.class);

  @Override
  public Object coerceInput(Url object) throws ClassCastException {
    return object.toString();
  }

  @Override
  public Url coerceOutput(Object object) throws TemplateOutputCastException {
    return new Url((String) object);
  }
}
