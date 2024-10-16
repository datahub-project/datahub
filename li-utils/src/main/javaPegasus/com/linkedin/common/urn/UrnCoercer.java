//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.linkedin.common.urn;

import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public class UrnCoercer implements DirectCoercer<Urn> {
  public UrnCoercer() {}

  public Object coerceInput(Urn object) throws ClassCastException {
    return object.toString();
  }

  public Urn coerceOutput(Object object) throws TemplateOutputCastException {
    if (object.getClass() != String.class) {
      throw new TemplateOutputCastException("Urn not backed by String");
    } else {
      try {
        return Urn.createFromString((String) object);
      } catch (URISyntaxException use) {
        throw new TemplateOutputCastException("Invalid URN syntax: " + use.getMessage(), use);
      }
    }
  }
}
