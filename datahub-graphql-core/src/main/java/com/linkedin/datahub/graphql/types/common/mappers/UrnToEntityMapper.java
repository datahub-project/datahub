package com.linkedin.datahub.graphql.types.common.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataHubPolicy;
import com.linkedin.datahub.graphql.generated.DataHubRole;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.ERModelRelationship;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.Notebook;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.datahub.graphql.generated.Post;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.datahub.graphql.generated.Role;
import com.linkedin.datahub.graphql.generated.SchemaFieldEntity;
import com.linkedin.datahub.graphql.generated.StructuredPropertyEntity;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.generated.VersionSet;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UrnToEntityMapper implements ModelMapper<com.linkedin.common.urn.Urn, Entity> {
  public static final UrnToEntityMapper INSTANCE = new UrnToEntityMapper();

  public static Entity map(
      @Nullable QueryContext context, @Nonnull final com.linkedin.common.urn.Urn urn) {
    return INSTANCE.apply(context, urn);
  }

  @Override
  public Entity apply(@Nullable QueryContext context, Urn input) {
    Entity partialEntity = null;
    if (input.getEntityType().equals("dataset")) {
      partialEntity = new Dataset();
      ((Dataset) partialEntity).setUrn(input.toString());
      ((Dataset) partialEntity).setType(EntityType.DATASET);
    }
    if (input.getEntityType().equals("role")) {
      partialEntity = new Role();
      ((Role) partialEntity).setUrn(input.toString());
      ((Role) partialEntity).setType(EntityType.ROLE);
    }
    if (input.getEntityType().equals("glossaryTerm")) {
      partialEntity = new GlossaryTerm();
      ((GlossaryTerm) partialEntity).setUrn(input.toString());
      ((GlossaryTerm) partialEntity).setType(EntityType.GLOSSARY_TERM);
    }
    if (input.getEntityType().equals("glossaryNode")) {
      partialEntity = new GlossaryNode();
      ((GlossaryNode) partialEntity).setUrn(input.toString());
      ((GlossaryNode) partialEntity).setType(EntityType.GLOSSARY_NODE);
    }
    if (input.getEntityType().equals("chart")) {
      partialEntity = new Chart();
      ((Chart) partialEntity).setUrn(input.toString());
      ((Chart) partialEntity).setType(EntityType.CHART);
    }
    if (input.getEntityType().equals("dashboard")) {
      partialEntity = new Dashboard();
      ((Dashboard) partialEntity).setUrn(input.toString());
      ((Dashboard) partialEntity).setType(EntityType.DASHBOARD);
    }
    if (input.getEntityType().equals("notebook")) {
      partialEntity = new Notebook();
      ((Notebook) partialEntity).setUrn(input.toString());
      ((Notebook) partialEntity).setType(EntityType.NOTEBOOK);
    }
    if (input.getEntityType().equals("dataJob")) {
      partialEntity = new DataJob();
      ((DataJob) partialEntity).setUrn(input.toString());
      ((DataJob) partialEntity).setType(EntityType.DATA_JOB);
    }
    if (input.getEntityType().equals("dataFlow")) {
      partialEntity = new DataFlow();
      ((DataFlow) partialEntity).setUrn(input.toString());
      ((DataFlow) partialEntity).setType(EntityType.DATA_FLOW);
    }
    if (input.getEntityType().equals("tag")) {
      partialEntity = new Tag();
      ((Tag) partialEntity).setUrn(input.toString());
      ((Tag) partialEntity).setType(EntityType.TAG);
    }
    if (input.getEntityType().equals("corpuser")) {
      partialEntity = new CorpUser();
      ((CorpUser) partialEntity).setUrn(input.toString());
      ((CorpUser) partialEntity).setType(EntityType.CORP_USER);
    }
    if (input.getEntityType().equals("corpGroup")) {
      partialEntity = new CorpGroup();
      ((CorpGroup) partialEntity).setUrn(input.toString());
      ((CorpGroup) partialEntity).setType(EntityType.CORP_GROUP);
    }
    if (input.getEntityType().equals("mlFeature")) {
      partialEntity = new MLFeature();
      ((MLFeature) partialEntity).setUrn(input.toString());
      ((MLFeature) partialEntity).setType(EntityType.MLFEATURE);
    }
    if (input.getEntityType().equals("mlFeatureTable")) {
      partialEntity = new MLFeatureTable();
      ((MLFeatureTable) partialEntity).setUrn(input.toString());
      ((MLFeatureTable) partialEntity).setType(EntityType.MLFEATURE_TABLE);
    }
    if (input.getEntityType().equals("mlPrimaryKey")) {
      partialEntity = new MLPrimaryKey();
      ((MLPrimaryKey) partialEntity).setUrn(input.toString());
      ((MLPrimaryKey) partialEntity).setType(EntityType.MLPRIMARY_KEY);
    }
    if (input.getEntityType().equals("mlModel")) {
      partialEntity = new MLModel();
      ((MLModel) partialEntity).setUrn(input.toString());
      ((MLModel) partialEntity).setType(EntityType.MLMODEL);
    }
    if (input.getEntityType().equals("mlModelGroup")) {
      partialEntity = new MLModelGroup();
      ((MLModelGroup) partialEntity).setUrn(input.toString());
      ((MLModelGroup) partialEntity).setType(EntityType.MLMODEL_GROUP);
    }
    if (input.getEntityType().equals("dataPlatform")) {
      partialEntity = new DataPlatform();
      ((DataPlatform) partialEntity).setUrn(input.toString());
      ((DataPlatform) partialEntity).setType(EntityType.DATA_PLATFORM);
    }
    if (input.getEntityType().equals("dataPlatformInstance")) {
      partialEntity = new DataPlatformInstance();
      ((DataPlatformInstance) partialEntity).setUrn(input.toString());
      ((DataPlatformInstance) partialEntity).setType(EntityType.DATA_PLATFORM_INSTANCE);
    }
    if (input.getEntityType().equals("container")) {
      partialEntity = new Container();
      ((Container) partialEntity).setUrn(input.toString());
      ((Container) partialEntity).setType(EntityType.CONTAINER);
    }
    if (input.getEntityType().equals("domain")) {
      partialEntity = new Domain();
      ((Domain) partialEntity).setUrn(input.toString());
      ((Domain) partialEntity).setType(EntityType.DOMAIN);
    }
    if (input.getEntityType().equals("erModelRelationship")) {
      partialEntity = new ERModelRelationship();
      ((ERModelRelationship) partialEntity).setUrn(input.toString());
      ((ERModelRelationship) partialEntity).setType(EntityType.ER_MODEL_RELATIONSHIP);
    }
    if (input.getEntityType().equals("assertion")) {
      partialEntity = new Assertion();
      ((Assertion) partialEntity).setUrn(input.toString());
      ((Assertion) partialEntity).setType(EntityType.ASSERTION);
    }
    if (input.getEntityType().equals("test")) {
      partialEntity = new Test();
      ((Test) partialEntity).setUrn(input.toString());
      ((Test) partialEntity).setType(EntityType.TEST);
    }
    if (input.getEntityType().equals(DATAHUB_ROLE_ENTITY_NAME)) {
      partialEntity = new DataHubRole();
      ((DataHubRole) partialEntity).setUrn(input.toString());
      ((DataHubRole) partialEntity).setType(EntityType.DATAHUB_ROLE);
    }
    if (input.getEntityType().equals(POLICY_ENTITY_NAME)) {
      partialEntity = new DataHubPolicy();
      ((DataHubPolicy) partialEntity).setUrn(input.toString());
      ((DataHubPolicy) partialEntity).setType(EntityType.DATAHUB_POLICY);
    }
    if (input.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME)) {
      partialEntity = new SchemaFieldEntity();
      ((SchemaFieldEntity) partialEntity).setUrn(input.toString());
      ((SchemaFieldEntity) partialEntity).setType(EntityType.SCHEMA_FIELD);
    }
    if (input.getEntityType().equals(DATAHUB_VIEW_ENTITY_NAME)) {
      partialEntity = new DataHubView();
      ((DataHubView) partialEntity).setUrn(input.toString());
      ((DataHubView) partialEntity).setType(EntityType.DATAHUB_VIEW);
    }
    if (input.getEntityType().equals(DATA_PRODUCT_ENTITY_NAME)) {
      partialEntity = new DataProduct();
      ((DataProduct) partialEntity).setUrn(input.toString());
      ((DataProduct) partialEntity).setType(EntityType.DATA_PRODUCT);
    }
    if (input.getEntityType().equals(OWNERSHIP_TYPE_ENTITY_NAME)) {
      partialEntity = new OwnershipTypeEntity();
      ((OwnershipTypeEntity) partialEntity).setUrn(input.toString());
      ((OwnershipTypeEntity) partialEntity).setType(EntityType.CUSTOM_OWNERSHIP_TYPE);
    }
    if (input.getEntityType().equals(STRUCTURED_PROPERTY_ENTITY_NAME)) {
      partialEntity = new StructuredPropertyEntity();
      ((StructuredPropertyEntity) partialEntity).setUrn(input.toString());
      ((StructuredPropertyEntity) partialEntity).setType(EntityType.STRUCTURED_PROPERTY);
    }
    if (input.getEntityType().equals(QUERY_ENTITY_NAME)) {
      partialEntity = new QueryEntity();
      ((QueryEntity) partialEntity).setUrn(input.toString());
      ((QueryEntity) partialEntity).setType(EntityType.QUERY);
    }
    if (input.getEntityType().equals(RESTRICTED_ENTITY_NAME)) {
      partialEntity = new Restricted();
      ((Restricted) partialEntity).setUrn(input.toString());
      ((Restricted) partialEntity).setType(EntityType.RESTRICTED);
    }
    if (input.getEntityType().equals(BUSINESS_ATTRIBUTE_ENTITY_NAME)) {
      partialEntity = new BusinessAttribute();
      ((BusinessAttribute) partialEntity).setUrn(input.toString());
      ((BusinessAttribute) partialEntity).setType(EntityType.BUSINESS_ATTRIBUTE);
    }
    if (input.getEntityType().equals(FORM_ENTITY_NAME)) {
      partialEntity = new Form();
      ((Form) partialEntity).setUrn(input.toString());
      ((Form) partialEntity).setType(EntityType.FORM);
    }
    if (input.getEntityType().equals(POST_ENTITY_NAME)) {
      partialEntity = new Post();
      ((Post) partialEntity).setUrn(input.toString());
      ((Post) partialEntity).setType(EntityType.POST);
    }
    if (input.getEntityType().equals(DATA_PROCESS_INSTANCE_ENTITY_NAME)) {
      partialEntity = new DataProcessInstance();
      ((DataProcessInstance) partialEntity).setUrn(input.toString());
      ((DataProcessInstance) partialEntity).setType(EntityType.DATA_PROCESS_INSTANCE);
    }
    if (input.getEntityType().equals(VERSION_SET_ENTITY_NAME)) {
      partialEntity = new VersionSet();
      ((VersionSet) partialEntity).setUrn(input.toString());
      ((VersionSet) partialEntity).setType(EntityType.VERSION_SET);
    }
    return partialEntity;
  }
}
