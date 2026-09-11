package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORM_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormPromptAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FormAssignmentAuthorizationValidatorTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
  private static final Urn FORM_A = UrnUtils.getUrn("urn:li:form:a");
  private static final Urn FORM_B = UrnUtils.getUrn("urn:li:form:b");

  private FormAssignmentAuthorizationValidator validator;
  private AuthorizationSession mockAuthSession;
  private AspectRetriever mockAspectRetriever;
  private RetrieverContext mockRetrieverContext;
  private MockedStatic<AuthUtil> authUtilMockedStatic;

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    validator = new FormAssignmentAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(FormAssignmentAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(FORMS_ASPECT_NAME)
                        .build(),
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(FORM_ENTITY_NAME)
                        .aspectName(DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME)
                        .build()))
            .build());
    mockAuthSession = Mockito.mock(AuthorizationSession.class);
    mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    mockRetrieverContext = Mockito.mock(RetrieverContext.class);
    Mockito.when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Collections.emptyMap());
  }

  @AfterMethod
  public void tearDown() {
    authUtilMockedStatic.close();
  }

  @Test
  public void testDenyAssignWithoutPrivilege() {
    stubManageForms(false);

    Stream<AspectValidationException> result =
        validate(
            TestMCP.ofOneUpsertItem(TEST_DATASET_URN, forms(FORM_A), new TestEntityRegistry()));

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAllowAssignWithPrivilege() {
    stubManageForms(true);

    Stream<AspectValidationException> result =
        validate(
            TestMCP.ofOneUpsertItem(TEST_DATASET_URN, forms(FORM_A), new TestEntityRegistry()));

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testDenyRemoveWithoutPrivilege() {
    stubManageForms(false);
    stubCurrentForms(forms(FORM_A, FORM_B));

    // Proposed drops FORM_B
    Stream<AspectValidationException> result =
        validate(
            TestMCP.ofOneUpsertItem(TEST_DATASET_URN, forms(FORM_A), new TestEntityRegistry()));

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAllowCompletionWithoutPrivilege() {
    stubManageForms(false);
    stubCurrentForms(forms(FORM_A));

    // Same form, moved from incomplete to completed with a completed prompt: membership unchanged.
    Forms completed = new Forms();
    completed.setIncompleteForms(new FormAssociationArray());
    FormAssociation association = new FormAssociation().setUrn(FORM_A);
    association.setCompletedPrompts(
        new FormPromptAssociationArray(new FormPromptAssociation().setId("prompt-1")));
    completed.setCompletedForms(new FormAssociationArray(association));

    Stream<AspectValidationException> result =
        validate(TestMCP.ofOneUpsertItem(TEST_DATASET_URN, completed, new TestEntityRegistry()));

    Assert.assertTrue(result.findAny().isEmpty());
    // The privilege is never consulted when membership is unchanged.
    authUtilMockedStatic.verify(
        () ->
            AuthUtil.isAuthorized(
                eq(mockAuthSession), eq(PoliciesConfig.MANAGE_DOCUMENTATION_FORMS_PRIVILEGE)),
        Mockito.never());
  }

  @Test
  public void testDenyDynamicAssignmentWithoutPrivilege() {
    stubManageForms(false);

    Stream<AspectValidationException> result =
        validate(
            TestMCP.ofOneUpsertItem(
                FORM_A,
                new DynamicFormAssignment().setFilter(new Filter()),
                new TestEntityRegistry()));

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAllowDynamicAssignmentWithPrivilege() {
    stubManageForms(true);

    Stream<AspectValidationException> result =
        validate(
            TestMCP.ofOneUpsertItem(
                FORM_A,
                new DynamicFormAssignment().setFilter(new Filter()),
                new TestEntityRegistry()));

    Assert.assertTrue(result.findAny().isEmpty());
  }

  private Stream<AspectValidationException> validate(java.util.Set<BatchItem> items) {
    return validator.validateProposedAspectsWithAuth(
        OperationFingerprint.EMPTY,
        items.stream().map(i -> (TestMCP) i).collect(Collectors.toList()),
        mockRetrieverContext,
        mockAuthSession);
  }

  private void stubManageForms(boolean allowed) {
    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorized(
                    eq(mockAuthSession), eq(PoliciesConfig.MANAGE_DOCUMENTATION_FORMS_PRIVILEGE)))
        .thenReturn(allowed);
  }

  private void stubCurrentForms(RecordTemplate current) {
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(
            Map.of(TEST_DATASET_URN, Map.of(FORMS_ASPECT_NAME, new Aspect(current.data()))));
  }

  private static Forms forms(Urn... incomplete) {
    Forms forms = new Forms();
    FormAssociationArray incompleteForms = new FormAssociationArray();
    for (Urn urn : incomplete) {
      incompleteForms.add(new FormAssociation().setUrn(urn));
    }
    forms.setIncompleteForms(incompleteForms);
    forms.setCompletedForms(new FormAssociationArray());
    return forms;
  }
}
