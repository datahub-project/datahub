package com.linkedin.common.urn;

import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;
import java.net.URISyntaxException;

public final class TestEntityUrn extends Urn {

  public static final String ENTITY_TYPE = "testEntity";

  private final String _keyPart1;
  private final String _keyPart2;
  private final String _keyPart3;

  public TestEntityUrn(String keyPart1, String keyPart2, String keyPart3) {
    super(ENTITY_TYPE, TupleKey.create(keyPart1, keyPart2, keyPart3));
    this._keyPart1 = keyPart1;
    this._keyPart2 = keyPart2;
    this._keyPart3 = keyPart3;
  }

  public static TestEntityUrn createFromString(String rawUrn) throws URISyntaxException {
    return createFromUrn(Urn.createFromString(rawUrn));
  }

  public static TestEntityUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!"li".equals(urn.getNamespace())) {
      throw new URISyntaxException(urn.toString(), "Urn namespace type should be 'li'.");
    } else if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(
          urn.toString(),
          "Urn entity type should be '" + ENTITY_TYPE + " got " + urn.getEntityType() + "'.");
    } else {
      TupleKey key = urn.getEntityKey();
      if (key.size() != 3) {
        throw new URISyntaxException(
            urn.toString(), "Invalid number of keys: found " + key.size() + " expected 3.");
      } else {
        try {
          return new TestEntityUrn(
              key.getAs(0, String.class), key.getAs(1, String.class), key.getAs(2, String.class));
        } catch (Exception e) {
          throw new URISyntaxException(urn.toString(), "Invalid URN Parameter: '" + e.getMessage());
        }
      }
    }
  }

  public static TestEntityUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }

  static {
    Custom.registerCoercer(
        new DirectCoercer<TestEntityUrn>() {
          public Object coerceInput(TestEntityUrn object) throws ClassCastException {
            return object.toString();
          }

          public TestEntityUrn coerceOutput(Object object) throws TemplateOutputCastException {
            try {
              return TestEntityUrn.createFromString((String) object);
            } catch (URISyntaxException e) {
              throw new TemplateOutputCastException("Invalid URN syntax: " + e.getMessage(), e);
            }
          }
        },
        TestEntityUrn.class);
  }
}
