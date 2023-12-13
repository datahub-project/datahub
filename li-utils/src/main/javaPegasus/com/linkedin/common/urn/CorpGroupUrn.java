package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class CorpGroupUrn extends Urn {

  public static final String ENTITY_TYPE = "corpGroup";

  private final String _groupName;

  public CorpGroupUrn(String groupName) {
    super(ENTITY_TYPE, TupleKey.createWithOneKeyPart(groupName));
    this._groupName = groupName;
  }

  private CorpGroupUrn(TupleKey entityKey, String groupName) {
    super("li", "corpGroup", entityKey);
    this._groupName = groupName;
  }

  public String getGroupNameEntity() {
    return _groupName;
  }

  public static CorpGroupUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  private static CorpGroupUrn decodeUrn(String groupName) throws Exception {
    return new CorpGroupUrn(TupleKey.create(new Object[] {groupName}), groupName);
  }

  public static CorpGroupUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Urn entity type should be 'corpGroup'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 1) {
        throw new URISyntaxException(urn.toString(), "Invalid number of keys.");
      } else {
        try {
          return decodeUrn((String) key.getAs(0, String.class));
        } catch (Exception var3) {
          throw new URISyntaxException(
              urn.toString(), "Invalid URN Parameter: '" + var3.getMessage());
        }
      }
    }
  }

  public static CorpGroupUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<CorpGroupUrn>() {
          public Object coerceInput(CorpGroupUrn object) throws ClassCastException {
            return object.toString();
          }

          public CorpGroupUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return CorpGroupUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        CorpGroupUrn.class);
  }
}
