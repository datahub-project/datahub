package io.datahubproject.openapi.operations.elastic;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import io.datahubproject.openapi.util.ElasticsearchUtils;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.opensearch.client.tasks.GetTaskResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/operations/elasticSearch")
@Slf4j
@Tag(
    name = "ElasticSearchOperations",
    description = "An API for managing your elasticsearch instance")
public class OperationsController {
  private final AuthorizerChain _authorizerChain;

  @Value("${authorization.restApiAuthorization:false}")
  private boolean restApiAuthorizationEnabled;

  @Autowired
  @Qualifier("elasticSearchSystemMetadataService")
  private SystemMetadataService _systemMetadataService;

  public OperationsController(AuthorizerChain authorizerChain) {
    _authorizerChain = authorizerChain;
  }

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @GetMapping(path = "/getTaskStatus", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getTaskStatus(String task) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.GET_ES_TASK_STATUS_PRIVILEGE.getType()))));
    if (restApiAuthorizationEnabled
        && !AuthUtil.isAuthorizedForResources(
            _authorizerChain, actorUrnStr, List.of(java.util.Optional.empty()), orGroup)) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN)
          .body(String.format(actorUrnStr + " is not authorized to get ElasticSearch task status"));
    }
    if (!ElasticsearchUtils.isTaskIdValid(task)) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST)
          .body(
              String.format(
                  "Task ID should be in the form nodeId:taskId e.g. aB1cdEf2GHI-JKLMnoPQr3:123456 (got %s)",
                  task));
    }
    String nodeIdToQuery = task.split(":")[0];
    long taskIdToQuery = Long.parseLong(task.split(":")[1]);
    java.util.Optional<GetTaskResponse> res =
        _systemMetadataService.getTaskStatus(nodeIdToQuery, taskIdToQuery);
    if (res.isEmpty()) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(String.format("Could not get task status for %s:%d", nodeIdToQuery, taskIdToQuery));
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
