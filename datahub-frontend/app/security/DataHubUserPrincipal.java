package security;

import java.security.Principal;

public class DataHubUserPrincipal implements Principal {
  private final String name;

  public DataHubUserPrincipal(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DataHubUserPrincipal that = (DataHubUserPrincipal) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "DataHubUserPrincipal[" + name + "]";
  }
}
