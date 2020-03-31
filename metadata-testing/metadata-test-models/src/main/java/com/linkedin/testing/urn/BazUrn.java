package com.linkedin.testing.urn;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;


public final class BazUrn extends Urn {

  public static final String ENTITY_TYPE = "entityBaz";

  public BazUrn(int id) throws URISyntaxException {
    super(ENTITY_TYPE, Integer.toString(id));
  }

  public static BazUrn createFromString(String rawUrn) throws URISyntaxException {
    return new BazUrn(Urn.createFromString(rawUrn).getIdAsInt());
  }
}
