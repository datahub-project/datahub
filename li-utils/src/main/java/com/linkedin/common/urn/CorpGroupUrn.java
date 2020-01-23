package com.linkedin.common.urn;

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CorpGroupUrn extends Urn {

  public static final String ENTITY_TYPE = "corpGroup";

  private static final Pattern URN_PATTERN = Pattern.compile("^" + URN_PREFIX + ENTITY_TYPE + ":(\\w+)$");

  private final String groupNameEntity;

  public CorpGroupUrn(String groupName) {
    super(ENTITY_TYPE, groupName);
    this.groupNameEntity = groupName;
  }

  public String getGroupNameEntity() {
    return groupNameEntity;
  }

  public static CorpGroupUrn createFromString(String rawUrn) throws URISyntaxException {
    String groupName = new Urn(rawUrn).getContent();
    return new CorpGroupUrn(groupName);
  }

  public static CorpGroupUrn createFromUrn(Urn urn) throws URISyntaxException {
    if (!ENTITY_TYPE.equals(urn.getEntityType())) {
      throw new URISyntaxException(urn.toString(), "Can't cast URN to CorpGroupUrn, not same ENTITY");
    }

    Matcher matcher = URN_PATTERN.matcher(urn.toString());
    if (matcher.find()) {
      return new CorpGroupUrn(matcher.group(1));
    } else {
      throw new URISyntaxException(urn.toString(), "CorpGroupUrn syntax error");
    }
  }

  public static CorpGroupUrn deserialize(String rawUrn) throws URISyntaxException {
    return createFromString(rawUrn);
  }
}
