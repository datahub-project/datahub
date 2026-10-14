package com.linkedin.metadata.models;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertSame;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class DefaultEntitySpecTest {

  @Test
  public void testRelationshipFieldSpecsAreMemoized() {
    AspectSpec aspectSpec =
        new AspectSpec(
            new AspectAnnotation("testAspect", false, false, null, 1L),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null);
    DefaultEntitySpec entitySpec =
        new DefaultEntitySpec(
            List.of(aspectSpec),
            new EntityAnnotation("testEntity", "testAspect"),
            mock(RecordDataSchema.class));

    assertSame(entitySpec.getRelationshipFieldSpecs(), entitySpec.getRelationshipFieldSpecs());
  }
}
