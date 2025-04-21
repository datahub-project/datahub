package com.linkedin.metadata.forms.validators;

import static com.linkedin.metadata.Constants.FORM_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.forms.validation.FormPromptValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Stream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FormPromptValidatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn TEST_FORM_URN = UrnUtils.getUrn("urn:li:form:form1");
  private static final Urn TEST_FORM_URN_2 = UrnUtils.getUrn("urn:li:form:form2");

  private SearchRetriever mockSearchRetriever;
  private CachingAspectRetriever mockAspectRetriever;
  private GraphRetriever mockGraphRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockSearchRetriever = Mockito.mock(SearchRetriever.class);
    mockGraphRetriever = Mockito.mock(GraphRetriever.class);
    mockAspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(mockSearchRetriever)
            .graphRetriever(mockGraphRetriever)
            .cachingAspectRetriever(mockAspectRetriever)
            .build();
  }

  @Test
  public void testValidUpsert() {
    FormPromptArray prompts = new FormPromptArray();
    prompts.add(new FormPrompt().setId("test1"));
    FormInfo formInfo =
        new FormInfo().setName("test form").setType(FormType.VERIFICATION).setPrompts(prompts);

    Mockito.when(
            mockSearchRetriever.scroll(
                Mockito.eq(Collections.singletonList(FORM_ENTITY_NAME)),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(10),
                Mockito.eq(new ArrayList<>()),
                Mockito.any(SearchFlags.class)))
        .thenReturn(new ScrollResult().setEntities(new SearchEntityArray()));

    // Test validation
    Stream<AspectValidationException> validationResult =
        FormPromptValidator.validateFormInfoUpserts(
            TestMCP.ofOneUpsertItem(TEST_FORM_URN, formInfo, TEST_REGISTRY), retrieverContext);

    // Assert no validation exceptions
    Assert.assertTrue(validationResult.findAny().isEmpty());
  }

  @Test
  public void testInvalidUpsertWithDuplicatePromptIdsInOneForm() {
    // two prompts with the same ID
    FormPromptArray prompts = new FormPromptArray();
    prompts.add(new FormPrompt().setId("test1"));
    prompts.add(new FormPrompt().setId("test1"));
    prompts.add(new FormPrompt().setId("test3"));
    FormInfo formInfo =
        new FormInfo().setName("test form").setType(FormType.VERIFICATION).setPrompts(prompts);

    // Mock search results with no matches
    ScrollResult mockResult = new ScrollResult();
    mockResult.setEntities(new SearchEntityArray());
    Mockito.when(
            mockSearchRetriever.scroll(
                Mockito.eq(Collections.singletonList(FORM_ENTITY_NAME)),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(10),
                Mockito.eq(new ArrayList<>()),
                Mockito.any(SearchFlags.class)))
        .thenReturn(mockResult);

    // Test validation
    Stream<AspectValidationException> validationResult =
        FormPromptValidator.validateFormInfoUpserts(
            TestMCP.ofOneUpsertItem(TEST_FORM_URN, formInfo, TEST_REGISTRY), retrieverContext);

    // Assert validation exception exists
    Assert.assertFalse(validationResult.findAny().isEmpty());
  }

  @Test
  public void testInvalidUpsertWithDuplicatePromptIdsInDifferentForm() {
    // two prompts with the same ID
    FormPromptArray prompts = new FormPromptArray();
    prompts.add(new FormPrompt().setId("test1"));
    prompts.add(new FormPrompt().setId("test2"));
    prompts.add(new FormPrompt().setId("test3"));
    FormInfo formInfo =
        new FormInfo().setName("test form").setType(FormType.VERIFICATION).setPrompts(prompts);

    // Mock search results with a match - meaning another form has
    SearchEntity existingForm = new SearchEntity();
    existingForm.setEntity(TEST_FORM_URN_2);
    ScrollResult mockResult = new ScrollResult();
    mockResult.setEntities(new SearchEntityArray(Collections.singletonList(existingForm)));
    Mockito.when(
            mockSearchRetriever.scroll(
                Mockito.eq(Collections.singletonList(FORM_ENTITY_NAME)),
                Mockito.any(Filter.class),
                Mockito.eq(null),
                Mockito.eq(10),
                Mockito.eq(new ArrayList<>()),
                Mockito.any(SearchFlags.class)))
        .thenReturn(mockResult);

    // Test validation
    Stream<AspectValidationException> validationResult =
        FormPromptValidator.validateFormInfoUpserts(
            TestMCP.ofOneUpsertItem(TEST_FORM_URN, formInfo, TEST_REGISTRY), retrieverContext);

    // Assert validation exception exists
    Assert.assertFalse(validationResult.findAny().isEmpty());
  }
}
