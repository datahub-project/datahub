package com.linkedin.metadata.aspect.patch.template.chart;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.chart.ChartDataSourceTypeArray;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class ChartInfoTemplate implements ArrayMergingTemplate<ChartInfo> {

  private static final String INPUT_EDGES_FIELD_NAME = "inputEdges";
  private static final String INPUTS_FIELD_NAME = "inputs";
  private static final String DESTINATION_URN_FIELD_NAME = "destinationUrn";

  @Override
  public ChartInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof ChartInfo) {
      return (ChartInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataJobInputOutput");
  }

  @Override
  public Class<ChartInfo> getTemplateType() {
    return ChartInfo.class;
  }

  @Nonnull
  @Override
  public ChartInfo getDefault() {
    ChartInfo chartInfo = new ChartInfo();
    chartInfo.setDescription("");
    chartInfo.setTitle("");
    ChangeAuditStamps changeAuditStamps = new ChangeAuditStamps();
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    changeAuditStamps.setCreated(auditStamp).setLastModified(auditStamp);
    chartInfo.setLastModified(changeAuditStamps);
    chartInfo.setInputEdges(new EdgeArray());

    // Deprecated fields
    chartInfo.setInputs(new ChartDataSourceTypeArray());

    return chartInfo;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode,
            INPUT_EDGES_FIELD_NAME,
            Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    transformedNode = arrayFieldToMap(transformedNode, INPUTS_FIELD_NAME, Collections.emptyList());

    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched, INPUT_EDGES_FIELD_NAME, Collections.singletonList(DESTINATION_URN_FIELD_NAME));

    rebasedNode = transformedMapToArray(rebasedNode, INPUTS_FIELD_NAME, Collections.emptyList());

    return rebasedNode;
  }
}
