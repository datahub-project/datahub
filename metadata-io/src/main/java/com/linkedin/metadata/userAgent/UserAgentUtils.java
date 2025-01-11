package com.linkedin.metadata.userAgent;

import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;

public class UserAgentUtils {

  private UserAgentUtils() {}

  public static final UserAgentAnalyzer UAA =
      UserAgentAnalyzer.newBuilder()
          .hideMatcherLoadStats()
          .withField(UserAgent.AGENT_CLASS)
          .withCache(1000)
          .build();
}
