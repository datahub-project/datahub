package com.linkedin.metadata.config.usage.cigate;

import javax.annotation.Nonnull;

/** Selects the active {@link UsageRegistryCiGateProfile} for CI gate tests. */
public final class UsageRegistryCiGateProfiles {

  private static final String PROFILE_PROPERTY = "usage.registry.cigate.profile";

  private UsageRegistryCiGateProfiles() {}

  @Nonnull
  public static UsageRegistryCiGateProfile active() {
    String className = System.getProperty(PROFILE_PROPERTY);
    if (className == null || className.isBlank()) {
      return OssUsageRegistryCiGateProfile.INSTANCE;
    }
    try {
      Class<?> profileClass = Class.forName(className);
      Object instance = profileClass.getDeclaredConstructor().newInstance();
      if (!(instance instanceof UsageRegistryCiGateProfile profile)) {
        throw new IllegalStateException(
            PROFILE_PROPERTY + " must implement UsageRegistryCiGateProfile: " + className);
      }
      return profile;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to load " + PROFILE_PROPERTY + ": " + className, e);
    }
  }
}
