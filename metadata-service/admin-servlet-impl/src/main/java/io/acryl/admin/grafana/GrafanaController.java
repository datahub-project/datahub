package io.acryl.admin.grafana;

import static io.acryl.admin.grafana.GrafanaConfiguration.GRAFANA_SERVLET_NAME;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Collections;
import java.util.Enumeration;
import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@Controller
public class GrafanaController {
  @Autowired private GrafanaServlet grafanaServlet;

  @Autowired private ServletContext servletContext;

  @PostConstruct
  private void postConstruct() throws ServletException {
    grafanaServlet.init(
        new ServletConfig() {
          @Override
          public String getServletName() {
            return GRAFANA_SERVLET_NAME;
          }

          @Override
          public ServletContext getServletContext() {
            return servletContext;
          }

          @Override
          public String getInitParameter(String s) {
            return null;
          }

          @Override
          public Enumeration<String> getInitParameterNames() {
            return Collections.emptyEnumeration();
          }
        });
  }

  @RequestMapping(
      value = "/admin/dashboard/**",
      method = {RequestMethod.GET, RequestMethod.POST, RequestMethod.HEAD, RequestMethod.OPTIONS})
  protected @ResponseBody void handleRequest(
      @Nonnull HttpServletRequest request, @Nonnull HttpServletResponse response) throws Exception {

    Assert.state(this.grafanaServlet != null, "No Servlet instance");
    this.grafanaServlet.service(request, response);
  }
}
