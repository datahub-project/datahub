package com.linkedin.metadata.builders.search;

import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.CorpUserAspect;


public class CorpUserInfoMockUtils {

  private CorpUserInfoMockUtils() {
    // Util class should not have public constructor
  }

  public static CorpUserInfo corpUserInfo() {
    return new CorpUserInfo().setTitle("fooBarEng")
        .setEmail("fooBar@linkedin.com")
        .setFullName("fooBar")
        .setActive(true);
  }

  public static CorpUserAspect corpUserAspect(CorpUserInfo corpUserInfo) {
    CorpUserAspect aspect = new CorpUserAspect();
    aspect.setCorpUserInfo(corpUserInfo);
    return aspect;
  }

  public static CorpUserAspect corpUserAspect(CorpUserEditableInfo corpUserEditableInfo) {
    CorpUserAspect aspect = new CorpUserAspect();
    aspect.setCorpUserEditableInfo(corpUserEditableInfo);
    return aspect;
  }
}