package com.linkedin.metadata.search.opensearch;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.datahubusage.DataHubUsageServiceImpl;
import com.linkedin.metadata.datahubusage.ExternalAuditEventsSearchRequest;
import com.linkedin.metadata.datahubusage.event.UsageEventResult;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.opensearch.client.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Runs the audit event search against usage indices shaped like the ones deployments have: created
 * from the template (type as keyword), auto-created before the template existed (type as text), and
 * both behind one alias.
 */
@Import({
  OpenSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class DataHubUsageServiceOpenSearchTest extends AbstractTestNGSpringContextTests {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Autowired private SearchClientShim<?> searchClientShim;

  @Test
  public void testSearchesAnIndexCreatedWithoutItsTemplate() throws IOException {
    String index = "audit_legacy_datahub_usage_event";
    indexEvent(index, "legacy-1", "urn:li:corpuser:legacy");

    assertEquals(searchActors(index), Set.of("urn:li:corpuser:legacy"));
  }

  @Test
  public void testSearchesAnIndexCreatedFromTheTemplate() throws IOException {
    String index = "audit_template_datahub_usage_event";
    createTemplateMappedIndex(index);
    indexEvent(index, "template-1", "urn:li:corpuser:template");

    assertEquals(searchActors(index), Set.of("urn:li:corpuser:template"));
  }

  @Test
  public void testKeepsTheTemplateMappedIndexWhenBothShareAnAlias() throws IOException {
    String legacy = "audit_mixed_legacy";
    String current = "audit_mixed_current";
    String alias = "audit_mixed_datahub_usage_event";
    indexEvent(legacy, "mixed-legacy-1", "urn:li:corpuser:legacy");
    createTemplateMappedIndex(current);
    indexEvent(current, "mixed-current-1", "urn:li:corpuser:current");
    request(
        "POST",
        "/_aliases",
        String.format(
            "{\"actions\":[{\"add\":{\"index\":\"%s\",\"alias\":\"%s\"}},"
                + "{\"add\":{\"index\":\"%s\",\"alias\":\"%s\"}}]}",
            legacy, alias, current, alias));

    assertEquals(searchActors(alias), Set.of("urn:li:corpuser:current"));
  }

  private Set<String> searchActors(String usageIndexName) {
    IndexConvention indexConvention = Mockito.mock(IndexConvention.class);
    Mockito.when(
            indexConvention.getIndexName(
                OP_CONTEXT, SearchComponent.USAGE, DATAHUB_USAGE_EVENT_INDEX))
        .thenReturn(usageIndexName);
    long now = System.currentTimeMillis();
    List<UsageEventResult> events =
        new DataHubUsageServiceImpl(searchClientShim, indexConvention)
            .externalAuditEventsSearch(
                OP_CONTEXT,
                ExternalAuditEventsSearchRequest.builder()
                    .size(10)
                    .eventTypes(List.of("LogInEvent"))
                    .startTime(now - 3_600_000L)
                    .endTime(now + 60_000L)
                    .build())
            .getUsageEvents();
    return events.stream().map(UsageEventResult::getActorUrn).collect(Collectors.toSet());
  }

  private void createTemplateMappedIndex(String index) throws IOException {
    request(
        "PUT",
        "/" + index,
        "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},"
            + "\"type\":{\"type\":\"keyword\"},\"timestamp\":{\"type\":\"date\"}}}}");
  }

  private void indexEvent(String index, String id, String actorUrn) throws IOException {
    long now = System.currentTimeMillis();
    request(
        "PUT",
        "/" + index + "/_doc/" + id + "?refresh=true",
        String.format(
            "{\"type\":\"LogInEvent\",\"timestamp\":%d,\"@timestamp\":%d,\"actorUrn\":\"%s\","
                + "\"usageSource\":\"backend\",\"loginSource\":\"PASSWORD_LOGIN\","
                + "\"eventSource\":\"GRAPHQL\"}",
            now, now, actorUrn));
  }

  private void request(String method, String endpoint, @Nullable String body) throws IOException {
    Request request = new Request(method, endpoint);
    if (body != null) {
      request.setJsonEntity(body);
    }
    searchClientShim.performLowLevelRequest(OP_CONTEXT, request);
  }
}
