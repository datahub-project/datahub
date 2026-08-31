package com.linkedin.datahub.graphql.types.mappers;

import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.generated.OriginType;
import org.testng.annotations.Test;

public class PdlEnumMapperTest {

  @Test
  public void testMapKnownValue() {
    assertEquals(
        PdlEnumMapper.map(OriginType.class, OriginType.UNKNOWN, OriginType.UNKNOWN),
        OriginType.UNKNOWN);
  }

  private enum FakePdlEnum {
    KNOWN_VALUE,
    UNKNOWN_WITH_CUSTOM_TO_STRING,
    $UNKNOWN;

    @Override
    public String toString() {
      return this == UNKNOWN_WITH_CUSTOM_TO_STRING ? "UNKNOWN" : name();
    }
  }

  @Test
  public void testMapDollarUnknownFallsBack() {
    OriginType result =
        PdlEnumMapper.map(OriginType.class, FakePdlEnum.$UNKNOWN, OriginType.UNKNOWN);
    assertEquals(result, OriginType.UNKNOWN);
  }

  @Test
  public void testMapUnrecognizedValueFallsBack() {
    OriginType result =
        PdlEnumMapper.map(OriginType.class, FakePdlEnum.KNOWN_VALUE, OriginType.UNKNOWN);
    assertEquals(result, OriginType.UNKNOWN);
  }

  @Test
  public void testMapUsesEnumNameNotToString() {
    OriginType result =
        PdlEnumMapper.map(
            OriginType.class, FakePdlEnum.UNKNOWN_WITH_CUSTOM_TO_STRING, OriginType.NATIVE);
    assertEquals(result, OriginType.NATIVE);
  }

  @Test
  public void testMapDefaultNullMatch() {
    OriginType result = PdlEnumMapper.mapDefaultNull(OriginType.class, OriginType.UNKNOWN);
    assertEquals(result, OriginType.UNKNOWN);
  }

  @Test
  public void testMapDefaultNullNoMatch() {
    OriginType result = PdlEnumMapper.mapDefaultNull(OriginType.class, FakePdlEnum.KNOWN_VALUE);
    assertNull(result);
  }
}
