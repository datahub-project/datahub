package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class CorpuserUrn extends Urn {

  public static final String ENTITY_TYPE = "corpuser";

  private static final Pattern URN_PATTERN = Pattern.compile("^" + URN_PREFIX + ENTITY_TYPE + ":([\\-\\w]+)$");

  private final String usernameEntity;

  public CorpuserUrn(String username) {
    super(ENTITY_TYPE, username);
    this.usernameEntity = username;
  }

  public String getUsernameEntity() {
    return usernameEntity;
  }

  public static CorpuserUrn createFromString(String rawUrn) throws URISyntaxException {
    String username = new Urn(rawUrn).getContent();
    return new CorpuserUrn(username);
  }

  public static CorpuserUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Can't cast URN to CorpuserUrn, not same ENTITY");
    }

    Matcher matcher = URN_PATTERN.matcher(urn.toString());
    if (matcher.find()) {
      return new CorpuserUrn(matcher.group(1));
    } else {
      throw new URISyntaxException(urn.toString(), "CorpuserUrn syntax error");
    }
  }

  public static CorpuserUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(new DirectCoercer<CorpuserUrn>() {
      public Object coerceInput(CorpuserUrn object) throws ClassCastException {
        return object.toString();
      }

      public CorpuserUrn coerceOutput(Object object) throws TemplateOutputCastException {
        try {
          return CorpuserUrn.createFromString((String) object);
        } catch (URISyntaxException e) {
          throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
        }
      }
    }, CorpuserUrn.class);
  }

}
