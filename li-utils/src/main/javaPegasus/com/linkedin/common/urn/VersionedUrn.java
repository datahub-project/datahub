package com.linkedin.common.urn;

public class VersionedUrn {

  private final String _urn;
  private final String _versionStamp;

  public VersionedUrn(String urn, String versionStamp) {
    _urn = urn;
    _versionStamp = versionStamp;
  }

  public String getUrn() {
    return _urn;
  }

  public String getVersionStamp() {
    return _versionStamp;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof VersionedUrn) {
      VersionedUrn other = (VersionedUrn) obj;
      return equals(_urn, other._urn) && equals(_versionStamp, other._versionStamp);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h1 = _urn != null ? _urn.hashCode() : 0;
    int h2 = _versionStamp != null ? _versionStamp.hashCode() : 0;
    return 31 * h1 + h2;
  }

  @Override
  public String toString() {
    return "(" + _urn + " , " + _versionStamp + ")";
  }

  private static boolean equals(Object o1, Object o2) {
    if (o1 != null) {
      return o1.equals(o2);
    }
    return o2 == null;
  }

  /*convenient method*/
  public static VersionedUrn of(String urn, String versionStamp) {
    return new VersionedUrn(urn, versionStamp);
  }
}
