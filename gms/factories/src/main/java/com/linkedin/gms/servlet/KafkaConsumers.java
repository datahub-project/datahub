package com.linkedin.gms.servlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.WebApplicationContext;


public class KafkaConsumers extends HttpServlet {

  private MetadataAuditEventProcessor

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    ApplicationContext ac = (ApplicationContext) config.getServletContext().getAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE);

    this.someObject = (SomeBean)ac.getBean("someBeanRef");
  }


}
