package com.datahub.test.testing.urn;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;

public final class SingleAspectEntityUrn extends Urn {

  private static final String ENTITY_TYPE = "entitySingleAspectEntity";

  public SingleAspectEntityUrn(long id) throws URISyntaxException {
    super(ENTITY_TYPE, Long.toString(id));
  }

  public static SingleAspectEntityUrn createFromString(String rawUrn) throws URISyntaxException {
    return new SingleAspectEntityUrn(Urn.createFromString(rawUrn).getIdAsInt());
  }
}
