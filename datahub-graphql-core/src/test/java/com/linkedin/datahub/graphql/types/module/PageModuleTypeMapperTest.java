package com.linkedin.datahub.graphql.types.module;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.module.DataHubPageModuleType;
import org.testng.annotations.Test;

public class PageModuleTypeMapperTest {

  @Test
  public void testMapValidModuleType() {
    // Test mapping a valid module type
    DataHubPageModuleType gmsType = DataHubPageModuleType.LINK;

    com.linkedin.datahub.graphql.generated.DataHubPageModuleType result =
        PageModuleTypeMapper.map(gmsType);

    assertNotNull(result);
    assertEquals(result, com.linkedin.datahub.graphql.generated.DataHubPageModuleType.LINK);
  }

  @Test
  public void testMapAllValidModuleTypes() {
    // Test all the valid module types that exist in both enums
    DataHubPageModuleType[] gmsTypes = {
      DataHubPageModuleType.LINK,
      DataHubPageModuleType.RICH_TEXT,
      DataHubPageModuleType.ASSET_COLLECTION,
      DataHubPageModuleType.HIERARCHY,
      DataHubPageModuleType.OWNED_ASSETS,
      DataHubPageModuleType.DOMAINS,
      DataHubPageModuleType.ASSETS,
      DataHubPageModuleType.CHILD_HIERARCHY,
      DataHubPageModuleType.DATA_PRODUCTS,
      DataHubPageModuleType.RELATED_TERMS,
      DataHubPageModuleType.UNKNOWN
    };

    for (DataHubPageModuleType gmsType : gmsTypes) {
      com.linkedin.datahub.graphql.generated.DataHubPageModuleType result =
          PageModuleTypeMapper.map(gmsType);

      assertNotNull(result);
      assertEquals(result.name(), gmsType.name());
    }
  }

  @Test
  public void testMapInvalidModuleTypeReturnsUnknown() {
    // Create a mock DataHubPageModuleType that returns a value not in the GraphQL enum
    DataHubPageModuleType mockType = mock(DataHubPageModuleType.class);
    when(mockType.toString()).thenReturn("INVALID_MODULE_TYPE_12345");

    // This should catch the IllegalArgumentException and return UNKNOWN
    com.linkedin.datahub.graphql.generated.DataHubPageModuleType result =
        PageModuleTypeMapper.map(mockType);

    assertNotNull(result);
    assertEquals(result, com.linkedin.datahub.graphql.generated.DataHubPageModuleType.UNKNOWN);
  }

  @Test
  public void testMapWithNullContextValidType() {
    // Test that the mapper works with null context (first parameter)
    DataHubPageModuleType gmsType = DataHubPageModuleType.RICH_TEXT;

    com.linkedin.datahub.graphql.generated.DataHubPageModuleType result =
        PageModuleTypeMapper.INSTANCE.apply(null, gmsType);

    assertNotNull(result);
    assertEquals(result, com.linkedin.datahub.graphql.generated.DataHubPageModuleType.RICH_TEXT);
  }

  @Test
  public void testMapWithNullContextInvalidType() {
    // Test that the mapper handles invalid type with null context
    DataHubPageModuleType mockType = mock(DataHubPageModuleType.class);
    when(mockType.toString()).thenReturn("NONEXISTENT_TYPE");

    com.linkedin.datahub.graphql.generated.DataHubPageModuleType result =
        PageModuleTypeMapper.INSTANCE.apply(null, mockType);

    assertNotNull(result);
    assertEquals(result, com.linkedin.datahub.graphql.generated.DataHubPageModuleType.UNKNOWN);
  }

  @Test
  public void testMapUnknownTypeExplicitly() {
    // Explicitly test that UNKNOWN maps to UNKNOWN
    DataHubPageModuleType gmsType = DataHubPageModuleType.UNKNOWN;

    com.linkedin.datahub.graphql.generated.DataHubPageModuleType result =
        PageModuleTypeMapper.map(gmsType);

    assertNotNull(result);
    assertEquals(result, com.linkedin.datahub.graphql.generated.DataHubPageModuleType.UNKNOWN);
  }

  @Test
  public void testStaticMapMethodUsesInstanceApply() {
    // Verify that the static map method uses the instance's apply method
    DataHubPageModuleType gmsType = DataHubPageModuleType.DOMAINS;

    com.linkedin.datahub.graphql.generated.DataHubPageModuleType result1 =
        PageModuleTypeMapper.map(gmsType);
    com.linkedin.datahub.graphql.generated.DataHubPageModuleType result2 =
        PageModuleTypeMapper.INSTANCE.apply(null, gmsType);

    assertEquals(result1, result2);
  }
}
