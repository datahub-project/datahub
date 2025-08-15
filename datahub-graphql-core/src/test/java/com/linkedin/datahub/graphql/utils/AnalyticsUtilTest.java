package com.linkedin.datahub.graphql.utils;

import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsUtil;
import com.linkedin.datahub.graphql.generated.Cell;
import com.linkedin.datahub.graphql.generated.Row;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AnalyticsUtilTest {

  @Mock private OperationContext mockOpContext;

  @Mock private EntityClient mockEntityClient;

  final String TEST_CORP_USER_INFO_TEST_USER = "Corp User";
  final String TEST_CORP_USER_EDITABLE_INFO_TEST_TITLE = "Editable Info Title";
  final String TEST_CORP_USER_EDITABLE_INFO_TEST_EMAIL = "Editable Info Email";

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testConvertToUserInfoRows() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row(null, Arrays.asList(new Cell("urn:li:corpuser:testuser", null, null))));

    // create a CorpUserInfo with only display name set
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setActive(true);
    corpUserInfo.setDisplayName(TEST_CORP_USER_INFO_TEST_USER);

    // create an editableInfo with the email and title set
    CorpUserEditableInfo corpUserEditableInfo = new CorpUserEditableInfo();
    corpUserEditableInfo.setEmail(TEST_CORP_USER_EDITABLE_INFO_TEST_EMAIL); // Overriding email
    corpUserEditableInfo.setTitle(TEST_CORP_USER_EDITABLE_INFO_TEST_TITLE); // Overriding title

    DataMap corpUserInfoDataMap = new DataMap();
    corpUserInfoDataMap.put("name", Constants.CORP_USER_INFO_ASPECT_NAME);
    corpUserInfoDataMap.put("type", "VERSIONED");
    corpUserInfoDataMap.put("value", corpUserInfo.data());

    DataMap corpUserEditableInfoDataMap = new DataMap();
    corpUserEditableInfoDataMap.put("name", Constants.CORP_USER_EDITABLE_INFO_ASPECT_NAME);
    corpUserEditableInfoDataMap.put("type", "VERSIONED");
    corpUserEditableInfoDataMap.put("value", corpUserEditableInfo.data());

    EnvelopedAspect corpUserInfoEnvelopedAspect = new EnvelopedAspect(corpUserInfoDataMap);
    EnvelopedAspect corpUserEditableInfoEnvelopedAspect =
        new EnvelopedAspect(corpUserEditableInfoDataMap);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(Constants.CORP_USER_INFO_ASPECT_NAME, corpUserInfoEnvelopedAspect);
    aspectMap.put(
        Constants.CORP_USER_EDITABLE_INFO_ASPECT_NAME, corpUserEditableInfoEnvelopedAspect);

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(aspectMap);

    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    Urn userUrn = UrnUtils.getUrn("urn:li:corpuser:testuser");
    entityResponseMap.put(userUrn, entityResponse);

    // method of the entity client we need to mock to retrieve the response map
    when(mockEntityClient.batchGetV2(
            eq(mockOpContext), eq(Constants.CORP_USER_ENTITY_NAME), anySet(), anySet()))
        .thenReturn(entityResponseMap);

    // function we are testing
    AnalyticsUtil.convertToUserInfoRows(mockOpContext, mockEntityClient, rows);

    Row updatedRow = rows.get(0);
    List<Cell> updatedCells = updatedRow.getCells();

    // asserting that the display user is from CorpUserInfo and email, title are from EditableInfo
    assertEquals(updatedCells.get(0).getValue(), TEST_CORP_USER_INFO_TEST_USER);
    assertEquals(
        updatedCells.get(1).getValue(),
        TEST_CORP_USER_EDITABLE_INFO_TEST_TITLE); // Overriding title
    assertEquals(
        updatedCells.get(2).getValue(),
        TEST_CORP_USER_EDITABLE_INFO_TEST_EMAIL); // Overriding email
  }
}
