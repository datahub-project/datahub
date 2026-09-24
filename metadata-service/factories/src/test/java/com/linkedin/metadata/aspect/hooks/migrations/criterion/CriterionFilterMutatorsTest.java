package com.linkedin.metadata.aspect.hooks.migrations.criterion;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CriterionFilterMutatorsTest {

  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setUp() {
    EntityRegistry entityRegistry = mock(EntityRegistry.class);
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
  }

  @Test
  public void dataHubViewInfoTransform_stripsValueUnderDefinitionFilter() throws Exception {
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

    DataHubViewInfo info = new DataHubViewInfo();
    info.data().put("definition", definition);
    info.setName("n");
    info.setType(DataHubViewType.PERSONAL);
    info.setCreated(newAudit());
    info.setLastModified(newAudit());

    ViewInfoCriterionMutator mutator = new ViewInfoCriterionMutator();
    RecordTemplate out = mutator.applyTransformForTest(info, retrieverContext);
    assertNotNull(out);
    assertFalse(
        out.data()
            .getDataMap("definition")
            .getDataMap("filter")
            .getDataList("criteria")
            .getDataMap(0)
            .containsKey("value"));
  }

  @Test
  public void dataHubViewInfoTransform_noOpWhenNoValue() throws Exception {
    DataMap criterion = new DataMap();
    criterion.put("field", "entityType");
    DataList values = new DataList();
    values.add("dataset");
    criterion.put("values", values);

    DataList criteria = new DataList();
    criteria.add(criterion);

    DataMap filterMap = new DataMap();
    filterMap.put("criteria", criteria);

    DataMap definition = new DataMap();
    definition.put("entityTypes", new DataList());
    definition.put("filter", filterMap);

    DataHubViewInfo info = new DataHubViewInfo();
    info.data().put("definition", definition);
    info.setName("n");
    info.setType(DataHubViewType.PERSONAL);
    info.setCreated(newAudit());
    info.setLastModified(newAudit());

    ViewInfoCriterionMutator mutator = new ViewInfoCriterionMutator();
    assertNull(mutator.applyTransformForTest(info, retrieverContext));
  }

  @Test
  public void mutatorsReportV1ToV2SchemaMigration() {
    // The migration contract requires every concrete mutator to advertise v1 → v2.
    ViewInfoCriterionMutator view = new ViewInfoCriterionMutator();
    DynamicFormCriterionMutator form = new DynamicFormCriterionMutator();

    org.testng.Assert.assertEquals(view.getSourceVersion(), 1L);
    org.testng.Assert.assertEquals(view.getTargetVersion(), 2L);
    org.testng.Assert.assertEquals(form.getSourceVersion(), 1L);
    org.testng.Assert.assertEquals(form.getTargetVersion(), 2L);
  }

  @Test
  public void dynamicFormAssignmentTransform_stripsValueUnderFilter() throws Exception {
    DataMap criterion = new DataMap();
    criterion.put("field", "platform");
    criterion.put("value", "dbt");

    DataList criteria = new DataList();
    criteria.add(criterion);

    DataMap filterMap = new DataMap();
    filterMap.put("criteria", criteria);

    DynamicFormAssignment assignment = new DynamicFormAssignment();
    assignment.data().put("filter", filterMap);

    DynamicFormCriterionMutator mutator = new DynamicFormCriterionMutator();
    RecordTemplate out = mutator.applyTransformForTest(assignment, retrieverContext);
    assertNotNull(out);
    assertFalse(
        out.data().getDataMap("filter").getDataList("criteria").getDataMap(0).containsKey("value"));
  }

  private static AuditStamp newAudit() {
    AuditStamp a = new AuditStamp();
    a.setTime(1L);
    a.setActor(UrnUtils.getUrn("urn:li:corpuser:tester"));
    return a;
  }
}
