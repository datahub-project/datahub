package com.linkedin.metadata.service.docimport;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class ImportUseCaseTest {

  @Test
  public void testToSubType_contextDocument() {
    assertNull(ImportUseCase.CONTEXT_DOCUMENT.toSubType());
  }

  @Test
  public void testToSubType_skill() {
    assertEquals(ImportUseCase.SKILL.toSubType(), "SKILL");
  }

  @Test
  public void testFromString_validValues() {
    assertEquals(ImportUseCase.fromString("CONTEXT_DOCUMENT"), ImportUseCase.CONTEXT_DOCUMENT);
    assertEquals(ImportUseCase.fromString("SKILL"), ImportUseCase.SKILL);
  }

  @Test
  public void testFromString_caseInsensitive() {
    assertEquals(ImportUseCase.fromString("skill"), ImportUseCase.SKILL);
    assertEquals(ImportUseCase.fromString("context_document"), ImportUseCase.CONTEXT_DOCUMENT);
  }

  @Test
  public void testFromString_nullDefaultsToContextDocument() {
    assertEquals(ImportUseCase.fromString(null), ImportUseCase.CONTEXT_DOCUMENT);
  }

  @Test
  public void testFromString_unknownDefaultsToContextDocument() {
    assertEquals(ImportUseCase.fromString("UNKNOWN_VALUE"), ImportUseCase.CONTEXT_DOCUMENT);
    assertEquals(ImportUseCase.fromString(""), ImportUseCase.CONTEXT_DOCUMENT);
  }
}
