package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateUserSettingInput;
import com.linkedin.datahub.graphql.generated.UserSetting;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateUserSettingResolverTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:test";

  @Test
  public void testWriteCorpUserSettings() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_USER_URN)), eq(true)))
        .thenReturn(true);

    UpdateUserSettingResolver resolver = new UpdateUserSettingResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    UpdateUserSettingInput input = new UpdateUserSettingInput();
    input.setName(UserSetting.SHOW_SIMPLIFIED_HOMEPAGE);
    input.setValue(true);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    CorpUserSettings newSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true));
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_USER_URN), CORP_USER_SETTINGS_ASPECT_NAME, newSettings);

    verifySingleIngestProposal(mockService, 1, proposal);
  }
}
