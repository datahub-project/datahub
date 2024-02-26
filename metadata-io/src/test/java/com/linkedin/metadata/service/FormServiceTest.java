package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormAssociationArray;
import com.linkedin.common.FormVerificationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormType;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

@Slf4j
public class FormServiceTest {

  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,Test,PROD)");
  private static final Urn TEST_FORM_URN = UrnUtils.getUrn("urn:li:form:test");
  public static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:jdoe");
  public static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:test");

  @Test
  private void testIsFormAssignedToUsersWithOwners() throws Exception {
    EntityClient mockClient = mockEntityClient(null, null);
    FormService formService = new FormService(mockClient, mockSystemAuthentication());

    FormActorAssignment formActors = new FormActorAssignment();
    formActors.setOwners(true);
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_FORM_URN.getEntityType()),
                Mockito.eq(TEST_FORM_URN),
                Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // ensure this user is an explicit owner
    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(TEST_ACTOR_URN);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_ENTITY_URN.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertTrue(
        formService.isFormAssignedToUser(
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            new ArrayList<>(),
            mockSystemAuthentication(TEST_ACTOR_URN.toString())));
  }

  @Test
  private void testIsFormAssignedToUserExplicitly() throws Exception {
    EntityClient mockClient = mockEntityClient(null, null);
    FormService formService = new FormService(mockClient, mockSystemAuthentication());

    FormActorAssignment formActors = new FormActorAssignment();
    formActors.setOwners(false);
    // explicitly set this actor as assigned
    formActors.setUsers(new UrnArray(ImmutableList.of(TEST_ACTOR_URN)));
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_FORM_URN.getEntityType()),
                Mockito.eq(TEST_FORM_URN),
                Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // this user is not an owner
    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(null);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_ENTITY_URN.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertTrue(
        formService.isFormAssignedToUser(
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            new ArrayList<>(),
            mockSystemAuthentication(TEST_ACTOR_URN.toString())));
  }

  @Test
  private void testIsFormAssignedToUserGroupExplicitly() throws Exception {
    EntityClient mockClient = mockEntityClient(null, null);
    FormService formService = new FormService(mockClient, mockSystemAuthentication());

    FormActorAssignment formActors = new FormActorAssignment();
    formActors.setOwners(false);
    // explicitly set this actor as assigned
    formActors.setGroups(new UrnArray(ImmutableList.of(TEST_GROUP_URN)));
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_FORM_URN.getEntityType()),
                Mockito.eq(TEST_FORM_URN),
                Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // no owners on this entity
    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(null);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_ENTITY_URN.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertTrue(
        formService.isFormAssignedToUser(
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            ImmutableList.of(TEST_GROUP_URN),
            mockSystemAuthentication(TEST_ACTOR_URN.toString())));
  }

  @Test
  private void testIsFormIsNotAssignedToUser() throws Exception {
    EntityClient mockClient = mockEntityClient(null, null);
    FormService formService = new FormService(mockClient, mockSystemAuthentication());

    FormActorAssignment formActors = new FormActorAssignment();
    // owners is set to false, no actors or groups assigned means this test will return false
    formActors.setOwners(false);
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_FORM_URN.getEntityType()),
                Mockito.eq(TEST_FORM_URN),
                Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(null);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_ENTITY_URN.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertFalse(
        formService.isFormAssignedToUser(
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            ImmutableList.of(TEST_GROUP_URN),
            mockSystemAuthentication(TEST_ACTOR_URN.toString())));
  }

  @Test
  private void testIsFormAssignedToGroupOwner() throws Exception {
    EntityClient mockClient = mockEntityClient(null, null);
    FormService formService = new FormService(mockClient, mockSystemAuthentication());

    FormActorAssignment formActors = new FormActorAssignment();
    formActors.setOwners(true);
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, formActors);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_FORM_URN.getEntityType()),
                Mockito.eq(TEST_FORM_URN),
                Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // group that the user is in is assigned
    Map<String, EnvelopedAspect> ownershipAspectMap = createOwnershipAspectMap(TEST_GROUP_URN);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_ENTITY_URN.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN),
                Mockito.eq(ImmutableSet.of(OWNERSHIP_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(ownershipAspectMap)));

    Assert.assertTrue(
        formService.isFormAssignedToUser(
            TEST_FORM_URN,
            TEST_ENTITY_URN,
            TEST_ACTOR_URN,
            ImmutableList.of(TEST_GROUP_URN),
            mockSystemAuthentication(TEST_ACTOR_URN.toString())));
  }

  @Test
  private void testVerifyFormForEntity() throws Exception {
    EntityClient mockClient = mockEntityClient(null, null);
    FormService formService = new FormService(mockClient, mockSystemAuthentication());

    // form type VERIFICATION
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, new FormActorAssignment());
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_FORM_URN.getEntityType()),
                Mockito.eq(TEST_FORM_URN),
                Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    final FormAssociation formAssociation = new FormAssociation();
    formAssociation.setUrn(TEST_FORM_URN);
    final List<FormAssociation> completedForms = ImmutableList.of(formAssociation);
    Map<String, EnvelopedAspect> formsAspectMap = createFormsAspectMap(completedForms);
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_ENTITY_URN.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN),
                Mockito.eq(ImmutableSet.of(FORMS_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formsAspectMap)));

    Assert.assertTrue(
        formService.verifyFormForEntity(
            TEST_FORM_URN, TEST_ENTITY_URN, mockSystemAuthentication(TEST_ACTOR_URN.toString())));

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.argThat(
                new EntityFormsArgumentMatcher(
                    AspectUtils.buildMetadataChangeProposal(
                        TEST_ENTITY_URN,
                        FORMS_ASPECT_NAME,
                        formsAspectMap.get(FORMS_ASPECT_NAME)))),
            Mockito.any(Authentication.class));
  }

  @Test
  private void testVerifyFormForEntityNonVerificationForm() throws Exception {
    EntityClient mockClient = mockEntityClient(null, null);
    FormService formService = new FormService(mockClient, mockSystemAuthentication());

    // form type COMPLETION
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.COMPLETION, new FormActorAssignment());
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_FORM_URN.getEntityType()),
                Mockito.eq(TEST_FORM_URN),
                Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          formService.verifyFormForEntity(
              TEST_FORM_URN, TEST_ENTITY_URN, mockSystemAuthentication(TEST_ACTOR_URN.toString()));
        });

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class), Mockito.any(Authentication.class));
  }

  @Test
  private void testVerifyFormForEntityIncompleteForm() throws Exception {
    EntityClient mockClient = mockEntityClient(null, null);
    FormService formService = new FormService(mockClient, mockSystemAuthentication());

    // form type VERIFICATION
    Map<String, EnvelopedAspect> formInfoAspectMap =
        createFormInfoAspectMap(FormType.VERIFICATION, new FormActorAssignment());
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_FORM_URN.getEntityType()),
                Mockito.eq(TEST_FORM_URN),
                Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formInfoAspectMap)));

    // no completed forms
    Map<String, EnvelopedAspect> formsAspectMap = createFormsAspectMap(new ArrayList<>());
    Mockito.when(
            mockClient.getV2(
                Mockito.eq(TEST_ENTITY_URN.getEntityType()),
                Mockito.eq(TEST_ENTITY_URN),
                Mockito.eq(ImmutableSet.of(FORMS_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap(formsAspectMap)));

    Assert.assertThrows(
        RuntimeException.class,
        () -> {
          formService.verifyFormForEntity(
              TEST_FORM_URN, TEST_ENTITY_URN, mockSystemAuthentication(TEST_ACTOR_URN.toString()));
        });

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class), Mockito.any(Authentication.class));
  }

  private EntityClient mockEntityClient(Forms existingForms, FormInfo form) throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockClient.exists(Mockito.eq(TEST_ENTITY_URN), Mockito.any(Authentication.class)))
        .thenReturn(true);

    Mockito.when(mockClient.exists(Mockito.eq(TEST_FORM_URN), Mockito.any(Authentication.class)))
        .thenReturn(true);

    EntityResponse entityResponse = null;
    if (existingForms != null) {
      entityResponse =
          new EntityResponse()
              .setUrn(TEST_ENTITY_URN)
              .setEntityName(DATASET_ENTITY_NAME)
              .setAspects(
                  new EnvelopedAspectMap(
                      ImmutableMap.of(
                          FORMS_ASPECT_NAME,
                          new EnvelopedAspect().setValue(new Aspect(existingForms.data())))));
    } else {
      entityResponse =
          new EntityResponse()
              .setUrn(TEST_ENTITY_URN)
              .setEntityName(DATASET_ENTITY_NAME)
              .setAspects(new EnvelopedAspectMap());
    }

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(DATASET_ENTITY_NAME),
                Mockito.eq(TEST_ENTITY_URN),
                Mockito.eq(ImmutableSet.of(FORMS_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(entityResponse);

    if (form != null) {
      Mockito.when(
              mockClient.getV2(
                  Mockito.eq(FORM_ENTITY_NAME),
                  Mockito.eq(TEST_FORM_URN),
                  Mockito.eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME)),
                  Mockito.any(Authentication.class)))
          .thenReturn(
              new EntityResponse()
                  .setUrn(TEST_FORM_URN)
                  .setEntityName(FORM_ENTITY_NAME)
                  .setAspects(
                      new EnvelopedAspectMap(
                          ImmutableMap.of(
                              FORM_INFO_ASPECT_NAME,
                              new EnvelopedAspect().setValue(new Aspect(form.data()))))));
    }

    return mockClient;
  }

  private Criterion buildCriterion(String field, String value) {
    return new Criterion()
        .setField(field)
        .setCondition(Condition.EQUAL)
        .setValue(value)
        .setValues(new StringArray(ImmutableList.of(value)))
        .setNegated(false);
  }

  private Authentication mockSystemAuthentication() {
    return mockSystemAuthentication(SYSTEM_ACTOR);
  }

  private Authentication mockSystemAuthentication(String actorUrnStr) {
    Authentication auth = Mockito.mock(Authentication.class);
    Actor actor = Mockito.mock(Actor.class);
    Mockito.when(actor.toUrnStr()).thenReturn(actorUrnStr);
    Mockito.when(auth.getActor()).thenReturn(actor);
    return auth;
  }

  private Map<String, EnvelopedAspect> createOwnershipAspectMap(@Nullable final Urn actorUrn) {
    Ownership ownershipAspect = new Ownership();
    OwnerArray owners = new OwnerArray();
    if (actorUrn != null) {
      Owner owner = new Owner();
      owner.setOwner(TEST_ACTOR_URN);
      owners.add(owner);
    }
    ownershipAspect.setOwners(owners);
    Map<String, EnvelopedAspect> ownershipAspectMap = new HashMap<>();
    ownershipAspectMap.put(
        OWNERSHIP_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(ownershipAspect.data())));

    return ownershipAspectMap;
  }

  private Map<String, EnvelopedAspect> createFormInfoAspectMap(
      @Nonnull final FormType formType, @Nonnull FormActorAssignment actors) {
    FormInfo formInfo = new FormInfo();
    formInfo.setType(formType);
    formInfo.setActors(actors);
    Map<String, EnvelopedAspect> formInfoAspectMap = new HashMap<>();
    formInfoAspectMap.put(
        FORM_INFO_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(formInfo.data())));

    return formInfoAspectMap;
  }

  private Map<String, EnvelopedAspect> createFormsAspectMap(
      @Nonnull final List<FormAssociation> completedForms) {
    Forms forms = new Forms();
    forms.setCompletedForms(new FormAssociationArray(completedForms));
    forms.setVerifications(new FormVerificationAssociationArray());
    Map<String, EnvelopedAspect> formsAspectMap = new HashMap<>();
    formsAspectMap.put(FORMS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(forms.data())));

    return formsAspectMap;
  }
}
