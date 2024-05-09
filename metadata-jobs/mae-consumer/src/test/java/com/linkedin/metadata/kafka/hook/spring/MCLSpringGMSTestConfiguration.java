package com.linkedin.metadata.kafka.hook.spring;

import com.linkedin.metadata.entity.EntityService;
import org.springframework.boot.test.mock.mockito.MockBean;

public class MCLSpringGMSTestConfiguration {
  @MockBean EntityService<?> entityService;
}
