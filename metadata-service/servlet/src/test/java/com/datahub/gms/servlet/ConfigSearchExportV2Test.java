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

// A Spring context is per test class, so V2 is enabled here rather than in
// ConfigSearchExportDefaultTest. V2 stays a supported configuration when V3 is off.
@SpringBootTest(
    classes = {ConfigServletTestContext.class, ConfigSearchExportV2Test.QueryFilterConfig.class},
    properties = {
      "spring.main.allow-bean-definition-overriding=true",
      "elasticsearch.entityIndex.v2.enabled=true",
      "elasticsearch.entityIndex.v3.enabled=false"
    })
public class ConfigSearchExportV2Test extends AbstractTestNGSpringContextTests {

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
  public void testV2ExportListsWordGramPhraseClauses() throws Exception {
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
    // The V2 query matches word gram subfields with phrase clauses and has no V3 Stage 1 clause
    assertTrue(csv.contains("phrase_match"), csv);
    assertFalse(csv.contains("v3_text_search"), csv);
  }
}
