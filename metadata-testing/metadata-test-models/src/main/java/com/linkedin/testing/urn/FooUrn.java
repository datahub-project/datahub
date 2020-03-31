package com.linkedin.testing.urn;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;


public final class FooUrn extends Urn {

  public static final String ENTITY_TYPE = "entityFoo";

  public FooUrn(int id) throws URISyntaxException {
    super(ENTITY_TYPE, Integer.toString(id));
  }

  public static FooUrn createFromString(String rawUrn) throws URISyntaxException {
    return new FooUrn(Urn.createFromString(rawUrn).getIdAsInt());
  }
}
