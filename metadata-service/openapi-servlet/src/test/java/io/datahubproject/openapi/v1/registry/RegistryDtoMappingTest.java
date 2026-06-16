package io.datahubproject.openapi.v1.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import io.datahubproject.openapi.v1.models.registry.AspectAnnotationDto;
import io.datahubproject.openapi.v1.models.registry.AspectSpecDto;
import io.datahubproject.openapi.v1.models.registry.EntityAnnotationDto;
import io.datahubproject.openapi.v1.models.registry.EntitySpecDto;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class RegistryDtoMappingTest {

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testAspectAnnotationDtoMapsSchemaVersion() {
    AspectAnnotation annotation = new AspectAnnotation("domains", false, false, null, 2L);

    AspectAnnotationDto dto = AspectAnnotationDto.fromAspectAnnotation(annotation);

    assertEquals(dto.getName(), "domains");
    assertEquals(dto.getSchemaVersion(), 2L);
  }

  @Test
  public void testAspectAnnotationDtoMapsDefaultSchemaVersion() {
    AspectAnnotation annotation = new AspectAnnotation("status", false, false, null, 1L);

    AspectAnnotationDto dto = AspectAnnotationDto.fromAspectAnnotation(annotation);

    assertEquals(dto.getSchemaVersion(), 1L);
  }

  @Test
  public void testEntityAnnotationDtoMapsSearchGroup() {
    EntityAnnotation annotation = new EntityAnnotation("dataset", "datasetKey", "primary");

    EntityAnnotationDto dto = EntityAnnotationDto.fromEntityAnnotation(annotation);

    assertEquals(dto.getName(), "dataset");
    assertEquals(dto.getKeyAspect(), "datasetKey");
    assertEquals(dto.getSearchGroup(), "primary");
  }

  @Test
  public void testEntityAnnotationDtoMapsDefaultSearchGroup() {
    EntityAnnotation annotation = new EntityAnnotation("chart", "chartKey");

    EntityAnnotationDto dto = EntityAnnotationDto.fromEntityAnnotation(annotation);

    assertEquals(dto.getSearchGroup(), "default");
  }

  @Test
  public void testAspectSpecDtoFromRealRegistryIncludesSchemaVersion() {
    AspectSpec domains =
        TestOperationContexts.defaultEntityRegistry().getAspectSpecs().get("domains");
    assertNotNull(domains);

    AspectSpecDto dto = AspectSpecDto.fromAspectSpec(domains);

    assertNotNull(dto.getAspectAnnotation());
    assertEquals(dto.getAspectAnnotation().getName(), "domains");
    assertEquals(dto.getAspectAnnotation().getSchemaVersion(), 2L);
  }

  @Test
  public void testEntitySpecDtoFromRealRegistryIncludesSearchGroup() {
    EntitySpec dataset = TestOperationContexts.defaultEntityRegistry().getEntitySpec("dataset");
    assertNotNull(dataset);

    EntitySpecDto dto = EntitySpecDto.fromEntitySpec(dataset);

    assertNotNull(dto.getEntityAnnotation());
    assertEquals(dto.getEntityAnnotation().getSearchGroup(), "primary");
  }
}
