package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;


public class UrnCoercer implements DirectCoercer<Urn> {

  private static final boolean REGISTER_COERCER = Custom.registerCoercer(new UrnCoercer(), Urn.class);

  public UrnCoercer() {
  }

  public Object coerceInput(Urn object) throws ClassCastException {
    return object.toString();
  }

  public Urn coerceOutput(Object object) throws TemplateOutputCastException {
    try {
      return Urn.createFromString((String) object);
    } catch (URISyntaxException e) {
      throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
    }
  }
}
