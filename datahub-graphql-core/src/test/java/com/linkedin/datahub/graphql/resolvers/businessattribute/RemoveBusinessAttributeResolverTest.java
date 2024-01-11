package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.datahub.authentication.Authentication;
import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddBusinessAttributeInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockEntityService;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class RemoveBusinessAttributeResolverTest {
    private static final String BUSINESS_ATTRIBUTE_URN = "urn:li:businessAttribute:7d0c4283-de02-4043-aaf2-698b04274658";
    private static final String RESOURCE_URN = "urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.auth,PROD)";
    private static final String SUB_RESOURCE = "name";
    private EntityClient mockClient;
    private EntityService mockService;
    private QueryContext mockContext;
    private DataFetchingEnvironment mockEnv;
    private Authentication mockAuthentication;
    private void init() {
        mockClient = Mockito.mock(EntityClient.class);
        mockService = getMockEntityService();
        mockEnv = Mockito.mock(DataFetchingEnvironment.class);
        mockAuthentication = Mockito.mock(Authentication.class);
    }
    private void setupAllowContext() {
        mockContext = getMockAllowContext();
        Mockito.when(mockContext.getAuthentication()).thenReturn(mockAuthentication);
        Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    }
    @Test
    public void testSuccess() throws Exception {
        init();
        setupAllowContext();
        Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(addBusinessAttributeInput());
        Mockito.when(mockClient.exists(Urn.createFromString((BUSINESS_ATTRIBUTE_URN)), mockAuthentication)).thenReturn(true);
        Mockito.when(mockService.exists(Urn.createFromString(RESOURCE_URN))).thenReturn(true);
        Mockito.when(mockService.getAspect(Urn.createFromString(RESOURCE_URN), Constants.SCHEMA_METADATA_ASPECT_NAME, 0))
                .thenReturn(schemaMetadata());
        Mockito.when(EntityUtils.getAspectFromEntity(
                RESOURCE_URN,
                Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                mockService, null)
        ).thenReturn(editableSchemaMetadata());

        RemoveBusinessAttributeResolver resolver = new RemoveBusinessAttributeResolver(mockClient, mockService);
        resolver.get(mockEnv).get();

        Mockito.verify(mockClient, Mockito.times(1))
                .ingestProposal(Mockito.any(MetadataChangeProposal.class),
                        Mockito.eq(mockAuthentication));
    }

    @Test
    public void testBusinessAttributeNotExists() throws Exception {
        init();
        setupAllowContext();
        Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(addBusinessAttributeInput());
        Mockito.when(mockClient.exists(Urn.createFromString((BUSINESS_ATTRIBUTE_URN)), mockAuthentication)).thenReturn(false);
        Mockito.when(mockService.exists(Urn.createFromString(RESOURCE_URN))).thenReturn(true);
        Mockito.when(mockService.getAspect(Urn.createFromString(RESOURCE_URN), Constants.SCHEMA_METADATA_ASPECT_NAME, 0))
                .thenReturn(schemaMetadata());
        RemoveBusinessAttributeResolver resolver = new RemoveBusinessAttributeResolver(mockClient, mockService);
        RuntimeException exception = expectThrows(RuntimeException.class, () -> resolver.get(mockEnv).get());
        assertTrue(exception.getMessage().equals(
                String.format("This urn does not exist: %s", BUSINESS_ATTRIBUTE_URN)));
        Mockito.verify(mockClient, Mockito.times(0))
                .ingestProposal(Mockito.any(MetadataChangeProposal.class),
                        Mockito.eq(mockAuthentication));
    }

    @Test
    public void testBusinessAttributeNotAdded() throws Exception {
        init();
        setupAllowContext();
        Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(addBusinessAttributeInput());
        Mockito.when(mockClient.exists(Urn.createFromString((BUSINESS_ATTRIBUTE_URN)), mockAuthentication)).thenReturn(true);
        Mockito.when(mockService.exists(Urn.createFromString(RESOURCE_URN))).thenReturn(true);
        Mockito.when(mockService.getAspect(Urn.createFromString(RESOURCE_URN), Constants.SCHEMA_METADATA_ASPECT_NAME, 0))
                .thenReturn(schemaMetadata());

        RemoveBusinessAttributeResolver resolver = new RemoveBusinessAttributeResolver(mockClient, mockService);
        ExecutionException actualException = expectThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
        assertTrue(actualException.getCause().getMessage().equals(String.format("Failed to remove Business Attribute with urn %s to dataset with urn %s",
                BUSINESS_ATTRIBUTE_URN, RESOURCE_URN)));

        Mockito.verify(mockClient, Mockito.times(0))
                .ingestProposal(Mockito.any(MetadataChangeProposal.class),
                        Mockito.eq(mockAuthentication));

    }

    @Test
    public void testResourceNotExists() throws Exception {
        init();
        setupAllowContext();
        Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(addBusinessAttributeInput());
        Mockito.when(mockClient.exists(Urn.createFromString((BUSINESS_ATTRIBUTE_URN)), mockAuthentication)).thenReturn(true);
        Mockito.when(mockService.exists(Urn.createFromString(RESOURCE_URN))).thenReturn(false);
        Mockito.when(mockService.getAspect(Urn.createFromString(RESOURCE_URN), Constants.SCHEMA_METADATA_ASPECT_NAME, 0))
                .thenReturn(schemaMetadata());

        RemoveBusinessAttributeResolver resolver = new RemoveBusinessAttributeResolver(mockClient, mockService);
        ExecutionException exception = expectThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
        assertTrue(exception.getCause().getMessage().equals(
                String.format("Failed to remove Business Attribute with urn %s to dataset with urn %s",
                        BUSINESS_ATTRIBUTE_URN, RESOURCE_URN
                )));

        Mockito.verify(mockClient, Mockito.times(0))
                .ingestProposal(Mockito.any(MetadataChangeProposal.class),
                        Mockito.eq(mockAuthentication));
    }
    public AddBusinessAttributeInput addBusinessAttributeInput() {
        AddBusinessAttributeInput addBusinessAttributeInput = new AddBusinessAttributeInput();
        addBusinessAttributeInput.setBusinessAttributeUrn(BUSINESS_ATTRIBUTE_URN);
        addBusinessAttributeInput.setResourceUrn(resourceRefInput());
        return addBusinessAttributeInput;
    }
    private ResourceRefInput resourceRefInput() {
        ResourceRefInput resourceRefInput = new ResourceRefInput();
        resourceRefInput.setResourceUrn(RESOURCE_URN);
        resourceRefInput.setSubResource(SUB_RESOURCE);
        resourceRefInput.setSubResourceType(SubResourceType.DATASET_FIELD);
        return resourceRefInput;
    }

    private SchemaMetadata schemaMetadata() {
        SchemaMetadata schemaMetadata = new SchemaMetadata();
        SchemaFieldArray schemaFields = new SchemaFieldArray();
        SchemaField schemaField = new SchemaField();
        schemaField.setFieldPath(SUB_RESOURCE);
        schemaFields.add(schemaField);
        schemaMetadata.setFields(schemaFields);
        return schemaMetadata;
    }

    private EditableSchemaMetadata editableSchemaMetadata() {
        EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
        EditableSchemaFieldInfoArray editableSchemaFieldInfos = new EditableSchemaFieldInfoArray();
        EditableSchemaFieldInfo editableSchemaFieldInfo = new EditableSchemaFieldInfo();
        editableSchemaFieldInfo.setBusinessAttribute(new BusinessAttributeAssociation());
        editableSchemaFieldInfo.setFieldPath(SUB_RESOURCE);
        editableSchemaFieldInfos.add(editableSchemaFieldInfo);
        editableSchemaMetadata.setEditableSchemaFieldInfo(editableSchemaFieldInfos);
        return editableSchemaMetadata;
    }
}
