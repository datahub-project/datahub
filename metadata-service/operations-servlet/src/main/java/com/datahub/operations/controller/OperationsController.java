package com.datahub.operations.controller;

import com.datahub.operations.utils.ElasticsearchUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.Map;
import org.elasticsearch.client.tasks.GetTaskResponse;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/elastic")
@Tag(name = "ElasticSearchOperations", description = "An API for managing your elasticsearch instance")
public class OperationsController {

  @Autowired
  @Qualifier("elasticSearchSystemMetadataService")
  private SystemMetadataService _systemMetadataService;
  @GetMapping(path = "/getTaskStatus", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getTaskStatus(String task) {
    // auth
    if (!ElasticsearchUtils.isTaskIdValid(task)) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
          String.format("Task ID should be in the form nodeId:taskId e.g. aB1cdEf2GHI-JKLMnoPQr3:123456 (got %s)", task));
    }
    String nodeIdToQuery =  task.split(":")[0];
    long taskIdToQuery = Long.parseLong(task.split(":")[1]);
    java.util.Optional<GetTaskResponse> res = _systemMetadataService.getTaskStatus(nodeIdToQuery, taskIdToQuery);
    if (res.isEmpty()) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(String.format("Could not get task status for %s:%d", nodeIdToQuery, taskIdToQuery));
    }
    GetTaskResponse resp = res.get();
    JSONObject j = new JSONObject();
    j.put("completed", resp.isCompleted());
    j.put("taskId", res.get().getTaskInfo().getTaskId());
    j.put("status", res.get().getTaskInfo().getStatus());
    j.put("runTimeNanos", res.get().getTaskInfo().getRunningTimeNanos());
    return ResponseEntity.ok(j.toString());
  }
}