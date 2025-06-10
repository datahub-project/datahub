package com.linkedin.datahub.graphql.types.ingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.execution.StructuredExecutionReport;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import java.util.HashMap;

public class TestUtils {
  public static DataHubIngestionSourceSchedule getSchedule(String timezone, String interval) {
    return new DataHubIngestionSourceSchedule().setTimezone(timezone).setInterval(interval);
  }

  public static DataHubIngestionSourceConfig getConfig(
      String version, String recipe, String executorId) {
    return new DataHubIngestionSourceConfig()
        .setVersion(version)
        .setRecipe(recipe)
        .setExecutorId(executorId)
        .setExtraArgs(new StringMap(ImmutableMap.of("key", "value")));
  }

  public static DataHubIngestionSourceInfo getIngestionSourceInfo(
      String name,
      String type,
      DataHubIngestionSourceSchedule schedule,
      DataHubIngestionSourceConfig config) {
    return new DataHubIngestionSourceInfo()
        .setName(name)
        .setType(type)
        .setSchedule(schedule)
        .setConfig(config);
  }

  public static ExecutionRequestSource getExecutionRequestSource(Urn urn, String type) {
    return new ExecutionRequestSource().setIngestionSource(urn).setType(type);
  }

  public static StringMap getExecutionRequestInputArgs(String recipe, String version) {
    return new StringMap(
        ImmutableMap.of(
            "recipe", recipe,
            "version", version));
  }

  public static StructuredExecutionReport getExecutionRequestStructuredReport() {
    return new StructuredExecutionReport()
        .setType("RUN_INGEST")
        .setSerializedValue("{}")
        .setContentType("type");
  }

  public static ExecutionRequestInput getExecutionRequestInput(
      StringMap args,
      String task,
      String executorId,
      Long requestedAt,
      ExecutionRequestSource source,
      Urn actorUrn) {
    ExecutionRequestInput input = new ExecutionRequestInput();
    input.setArgs(args);
    input.setTask(task);
    input.setExecutorId(executorId);
    input.setRequestedAt(requestedAt);
    input.setSource(source);
    input.setActorUrn(actorUrn);
    return input;
  }

  public static ExecutionRequestResult getExecutionRequestResult(
      Long durationMs,
      String report,
      Long startTimeMs,
      String status,
      StructuredExecutionReport structuredReport) {
    return new ExecutionRequestResult()
        .setDurationMs(durationMs)
        .setReport(report)
        .setStartTimeMs(startTimeMs)
        .setStatus(status)
        .setStructuredReport(structuredReport);
  }

  public static EntityResponse getEntityResponse() {
    return new EntityResponse().setAspects(new EnvelopedAspectMap(new HashMap<>()));
  }

  public static void addAspect(
      EntityResponse entityResponse, String aspectName, RecordTemplate aspect) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(aspect.data()));
    entityResponse.getAspects().put(aspectName, envelopedAspect);
  }

  public static void verifyIngestionSourceInfo(
      IngestionSource ingestionSource, DataHubIngestionSourceInfo info) {
    assertEquals(ingestionSource.getName(), info.getName());
    assertEquals(ingestionSource.getType(), info.getType());
    assertEquals(ingestionSource.getConfig().getRecipe(), info.getConfig().getRecipe());
    assertEquals(ingestionSource.getConfig().getExecutorId(), info.getConfig().getExecutorId());
    assertEquals(ingestionSource.getConfig().getVersion(), info.getConfig().getVersion());
    for (StringMapEntry entry : ingestionSource.getConfig().getExtraArgs()) {
      assertTrue(info.getConfig().getExtraArgs().containsKey(entry.getKey()));
      assertEquals(entry.getValue(), info.getConfig().getExtraArgs().get(entry.getKey()));
    }

    assertEquals(ingestionSource.getSchedule().getInterval(), info.getSchedule().getInterval());
    assertEquals(ingestionSource.getSchedule().getTimezone(), info.getSchedule().getTimezone());
  }

  public static void verifyExecutionRequestInput(
      ExecutionRequest executionRequest, ExecutionRequestInput input) {
    assertEquals(executionRequest.getInput().getTask(), input.getTask());
    assertEquals(executionRequest.getInput().getRequestedAt(), input.getRequestedAt());
    assertEquals(executionRequest.getInput().getExecutorId(), input.getExecutorId());
    assertEquals(
        executionRequest.getInput().getSource().getIngestionSource(),
        input.getSource().getIngestionSource().toString());
    assertEquals(executionRequest.getInput().getSource().getType(), input.getSource().getType());
    assertEquals(executionRequest.getInput().getActorUrn(), input.getActorUrn().toString());

    for (StringMapEntry entry : executionRequest.getInput().getArguments()) {
      assertTrue(input.getArgs().containsKey(entry.getKey()));
      assertEquals(entry.getValue(), input.getArgs().get(entry.getKey()));
    }
  }

  public static void verifyExecutionRequestResult(
      ExecutionRequest executionRequest, ExecutionRequestResult result) {
    assertEquals(executionRequest.getResult().getDurationMs(), result.getDurationMs());
    assertEquals(executionRequest.getResult().getReport(), result.getReport());
    assertEquals(executionRequest.getResult().getStartTimeMs(), result.getStartTimeMs());
    assertEquals(executionRequest.getResult().getStatus(), result.getStatus());
  }
}
