package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FormServiceOwnershipTest {

  private static final Urn FORM_URN = UrnUtils.getUrn("urn:li:form:test");
  private static final Urn ENTITY_URN = UrnUtils.getUrn("urn:li:dataset:(test,test,PROD)");
  private static final Urn USER = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn OTHER_USER = UrnUtils.getUrn("urn:li:corpuser:other");
  private static final Urn GROUP = UrnUtils.getUrn("urn:li:corpGroup:testGroup");
  private static final Urn TECHNICAL_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner");
  private static final Urn BUSINESS_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__business_owner");
  private static final Urn DATA_STEWARD =
      UrnUtils.getUrn("urn:li:ownershipType:__system__data_steward");

  private FormService formService;
  private SystemEntityClient mockClient;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    mockClient = mock(SystemEntityClient.class);
    formService = new FormService(mockClient);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void noOwnershipTypeFilterMatchesAnyOwner() throws Exception {
    mockFormInfo(formAssignedToOwners(/* ownershipTypes */ null));
    mockOwnership(ownershipOf(USER, TECHNICAL_OWNER));

    assertTrue(
        formService.isFormAssignedToUser(
            opContext, FORM_URN, ENTITY_URN, USER, Collections.emptyList()));
  }

  @Test
  public void ownershipTypeFilterRestrictsToMatchingType() throws Exception {
    mockFormInfo(formAssignedToOwners(List.of(TECHNICAL_OWNER)));

    mockOwnership(ownershipOf(USER, TECHNICAL_OWNER));
    assertTrue(
        formService.isFormAssignedToUser(
            opContext, FORM_URN, ENTITY_URN, USER, Collections.emptyList()));

    mockOwnership(ownershipOf(USER, BUSINESS_OWNER));
    assertFalse(
        formService.isFormAssignedToUser(
            opContext, FORM_URN, ENTITY_URN, USER, Collections.emptyList()));
  }

  @Test
  public void multipleOwnershipTypesMatchIfAnySatisfied() throws Exception {
    mockFormInfo(formAssignedToOwners(List.of(TECHNICAL_OWNER, DATA_STEWARD)));
    mockOwnership(ownershipOf(USER, DATA_STEWARD));

    assertTrue(
        formService.isFormAssignedToUser(
            opContext, FORM_URN, ENTITY_URN, USER, Collections.emptyList()));
  }

  @Test
  public void groupOwnershipIsRespected() throws Exception {
    mockFormInfo(formAssignedToOwners(List.of(TECHNICAL_OWNER)));

    mockOwnership(ownershipOf(GROUP, TECHNICAL_OWNER));
    assertTrue(
        formService.isFormAssignedToUser(opContext, FORM_URN, ENTITY_URN, USER, List.of(GROUP)));

    mockOwnership(ownershipOf(GROUP, BUSINESS_OWNER));
    assertFalse(
        formService.isFormAssignedToUser(opContext, FORM_URN, ENTITY_URN, USER, List.of(GROUP)));
  }

  @Test
  public void nonOwnerReturnsFalse() throws Exception {
    mockFormInfo(formAssignedToOwners(List.of(TECHNICAL_OWNER)));
    mockOwnership(ownershipOf(OTHER_USER, TECHNICAL_OWNER));

    assertFalse(
        formService.isFormAssignedToUser(
            opContext, FORM_URN, ENTITY_URN, USER, Collections.emptyList()));
  }

  @Test
  public void enumOnlyOwnerIsMatchedByDerivedSystemTypeUrn() throws Exception {
    mockFormInfo(formAssignedToOwners(List.of(TECHNICAL_OWNER)));
    mockOwnership(enumOnlyOwnershipOf(USER, OwnershipType.TECHNICAL_OWNER));

    assertTrue(
        formService.isFormAssignedToUser(
            opContext, FORM_URN, ENTITY_URN, USER, Collections.emptyList()));
  }

  @Test
  public void ownerAssignmentDisabledReturnsFalse() throws Exception {
    FormActorAssignment actors = new FormActorAssignment();
    actors.setOwners(false);
    UrnArray types = new UrnArray();
    types.add(TECHNICAL_OWNER);
    actors.setOwnershipTypes(types);

    FormInfo formInfo = new FormInfo();
    formInfo.setActors(actors);

    mockFormInfo(formInfo);
    mockOwnership(ownershipOf(USER, TECHNICAL_OWNER));

    assertFalse(
        formService.isFormAssignedToUser(
            opContext, FORM_URN, ENTITY_URN, USER, Collections.emptyList()));
  }

  private static FormInfo formAssignedToOwners(@Nullable List<Urn> ownershipTypes) {
    FormActorAssignment actors = new FormActorAssignment();
    actors.setOwners(true);
    if (ownershipTypes != null && !ownershipTypes.isEmpty()) {
      UrnArray array = new UrnArray();
      array.addAll(ownershipTypes);
      actors.setOwnershipTypes(array);
    }
    FormInfo formInfo = new FormInfo();
    formInfo.setActors(actors);
    return formInfo;
  }

  private static Ownership ownershipOf(Urn ownerUrn, Urn ownershipTypeUrn) {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn);
    owner.setTypeUrn(ownershipTypeUrn);
    OwnerArray owners = new OwnerArray();
    owners.add(owner);
    Ownership ownership = new Ownership();
    ownership.setOwners(owners);
    return ownership;
  }

  private static Ownership enumOnlyOwnershipOf(Urn ownerUrn, OwnershipType type) {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn);
    owner.setType(type);
    OwnerArray owners = new OwnerArray();
    owners.add(owner);
    Ownership ownership = new Ownership();
    ownership.setOwners(owners);
    return ownership;
  }

  private void mockFormInfo(FormInfo formInfo) throws Exception {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(Constants.FORM_INFO_ASPECT_NAME);
    envelopedAspect.setValue(new Aspect(formInfo.data()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(Constants.FORM_INFO_ASPECT_NAME, envelopedAspect);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(FORM_URN);
    entityResponse.setEntityName(Constants.FORM_ENTITY_NAME);
    entityResponse.setAspects(aspectMap);

    when(mockClient.getV2(
            eq(opContext),
            eq(Constants.FORM_ENTITY_NAME),
            eq(FORM_URN),
            eq(Collections.singleton(Constants.FORM_INFO_ASPECT_NAME))))
        .thenReturn(entityResponse);
  }

  private void mockOwnership(Ownership ownership) throws Exception {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(Constants.OWNERSHIP_ASPECT_NAME);
    envelopedAspect.setValue(new Aspect(ownership.data()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(Constants.OWNERSHIP_ASPECT_NAME, envelopedAspect);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(ENTITY_URN);
    entityResponse.setAspects(aspectMap);

    when(mockClient.getV2(
            eq(opContext),
            eq(ENTITY_URN.getEntityType()),
            eq(ENTITY_URN),
            eq(Collections.singleton(Constants.OWNERSHIP_ASPECT_NAME))))
        .thenReturn(entityResponse);
  }
}
