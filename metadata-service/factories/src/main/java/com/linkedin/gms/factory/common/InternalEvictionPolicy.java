package com.linkedin.gms.factory.common;

public enum InternalEvictionPolicy {
  NEVER("NEVER"), // Dangerous, can cause OOM exceptions due to never evicting entries
  TTL("TTL"), // Pretty dangerous still, will only evict entries time based
  MAX_SIZE("MAX_SIZE"), // Limits entries by size, but entries can stay forever, less dangerous than just time
  SIZE_AND_TTL("SIZE_AND_TTL"); // The recommended setting, limits growth by both time and size

  String policyName;

  InternalEvictionPolicy(String policyName) {
    this.policyName = policyName;
  }

  public boolean isPolicy(String policyName) {
    return this.policyName.equalsIgnoreCase(policyName);
  }
}
