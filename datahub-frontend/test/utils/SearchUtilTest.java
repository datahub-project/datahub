package utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SearchUtilTest {
  @Test
  public void testEscapeForwardSlash() {
    // escape "/"
    assertEquals("\\/foo\\/bar", SearchUtil.escapeForwardSlash("/foo/bar"));
    // "/" is escaped but "*" is not escaped and is treated as regex. Since currently we want to
    // retain the regex behaviour with "*"
    assertEquals("\\/foo\\/bar\\/*", SearchUtil.escapeForwardSlash("/foo/bar/*"));
    assertEquals("", "");
    assertEquals("foo", "foo");
  }
}
