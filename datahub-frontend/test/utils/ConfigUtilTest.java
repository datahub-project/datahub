package utils;

import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConfigUtilTest {

  private static final List<String> DEFAULT = List.of("TLSv1.2", "TLSv1.3");

  @Test
  void getStringList_missingKey_returnsDefault() {
    Config config = ConfigFactory.parseString("other = 1");
    assertEquals(DEFAULT, ConfigUtil.getStringList(config, "missing", DEFAULT));
  }

  @Test
  void getStringList_blankValue_returnsDefault() {
    Config config = ConfigFactory.parseString("key = \"\"");
    assertEquals(DEFAULT, ConfigUtil.getStringList(config, "key", DEFAULT));
  }

  @Test
  void getStringList_commaDelimited_returnsTrimmedList() {
    Config config = ConfigFactory.parseString("key = \"TLSv1.2, TLSv1.3\"");
    assertEquals(List.of("TLSv1.2", "TLSv1.3"), ConfigUtil.getStringList(config, "key", DEFAULT));
  }

  @Test
  void getStringList_singleValue_returnsSingleElementList() {
    Config config = ConfigFactory.parseString("key = \"TLSv1.3\"");
    assertEquals(List.of("TLSv1.3"), ConfigUtil.getStringList(config, "key", DEFAULT));
  }

  @Test
  void getStringList_emptySegmentsFiltered() {
    Config config = ConfigFactory.parseString("key = \"TLSv1.2,, TLSv1.3 , \"");
    assertEquals(List.of("TLSv1.2", "TLSv1.3"), ConfigUtil.getStringList(config, "key", DEFAULT));
  }
}
