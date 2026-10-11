package com.linkedin.datahub.upgrade.system.criterion;

import static com.linkedin.metadata.Constants.DATAHUB_VIEW_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class CriterionFilterAspectsBlockingStepPrepareTest {

  @Test
  public void prepare_mergesLegacyValueOntoCopy() throws Exception {
    SystemAspect sa = mock(SystemAspect.class);
    when(sa.getAspectName()).thenReturn(DATAHUB_VIEW_INFO_ASPECT_NAME);

    DataMap criterion = new DataMap();
    criterion.put("field", "entityType");
    criterion.put("value", "dataset");
    DataList criteria = new DataList();
    criteria.add(criterion);
    DataMap filterMap = new DataMap();
    filterMap.put("criteria", criteria);
    DataMap definition = new DataMap();
    definition.put("entityTypes", new DataList());
    definition.put("filter", filterMap);

    DataHubViewInfo view = new DataHubViewInfo();
    view.data().put("definition", definition);
    view.setName("n");
    view.setType(DataHubViewType.PERSONAL);
    view.setCreated(audit());
    view.setLastModified(audit());

    when(sa.getRecordTemplate()).thenReturn(view);

    SystemAspect copy = mock(SystemAspect.class);
    when(sa.copy()).thenReturn(copy);
    when(copy.setRecordTemplate(any())).thenAnswer(inv -> copy);

    SystemAspect out = CriterionFilterAspectsBlockingStep.prepareSystemAspectForBlockingIngest(sa);
    assertSame(out, copy);

    ArgumentCaptor<RecordTemplate> captor = ArgumentCaptor.forClass(RecordTemplate.class);
    verify(copy).setRecordTemplate(captor.capture());
    RecordTemplate migrated = captor.getValue();
    assertFalse(
        migrated
            .data()
            .getDataMap("definition")
            .getDataMap("filter")
            .getDataList("criteria")
            .getDataMap(0)
            .containsKey("value"));
  }

  @Test
  public void prepare_unsupportedAspect_returnsOriginal() {
    SystemAspect sa = mock(SystemAspect.class);
    when(sa.getAspectName()).thenReturn("ownership");
    assertSame(CriterionFilterAspectsBlockingStep.prepareSystemAspectForBlockingIngest(sa), sa);
  }

  private static AuditStamp audit() {
    AuditStamp a = new AuditStamp();
    a.setTime(1L);
    a.setActor(UrnUtils.getUrn("urn:li:corpuser:tester"));
    return a;
  }
}
