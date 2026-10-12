package com.datahub.gms.servlet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.micrometer.core.instrument.Clock;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.mockito.Answers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.context.WebApplicationContext;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = {ConfigServletTestContext.class, ConfigSearchExportTest.QueryFilterConfig.class},
    properties = {
      "spring.main.allow-bean-definition-overriding=true",
      "elasticsearch.entityIndex.v3.enabled=true",
      "elasticsearch.entityIndex.v3.keywordReadEnabled=true"
    })
public class ConfigSearchExportTest extends AbstractTestNGSpringContextTests {

  @Configuration
  static class QueryFilterConfig {
    @Bean
    public QueryFilterRewriteChain queryFilterRewriteChain() {
      return QueryFilterRewriteChain.EMPTY;
    }
  }

  @Autowired private WebApplicationContext webApplicationContext;

  @MockitoBean public Clock clock;
  @MockitoBean public SystemTelemetryContext systemTelemetryContext;

  @MockitoBean(name = "searchClientShim", answers = Answers.RETURNS_MOCKS)
  SearchClientShim<?> searchClientShim;

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @Test
  public void testCsvExportWithSearchV3KeywordRead() throws Exception {
    ServletContext servletContext = mock(ServletContext.class);
    when(servletContext.getAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE))
        .thenReturn(webApplicationContext);
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter("format")).thenReturn("csv");
    when(request.getServletContext()).thenReturn(servletContext);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter body = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(body));

    new ConfigSearchExport().doGet(request, response);

    verify(response).setStatus(HttpServletResponse.SC_OK);
    String csv = body.toString();
    // Deliberate V3 change: the Stage 1 query reads the shared _search fields, so the export lists
    // their subfields and no V2 per-field subfields, analyzers or word-gram phrases
    assertTrue(csv.contains("_search.entityName.text"), csv);
    assertTrue(csv.contains("_search.description.stemmed"), csv);
    assertTrue(csv.contains("prefix_match"), csv);
    // Stage 1 clauses under the dis_max root: constant-score exact names on the root keywords
    assertTrue(csv.contains("exact_match,ConstantScoreQueryBuilder,name.keyword"), csv);
    assertFalse(csv.contains(".delimited"), csv);
    assertFalse(csv.contains("query_word_delimited"), csv);
    assertFalse(csv.contains("phrase_match"), csv);
  }
}
