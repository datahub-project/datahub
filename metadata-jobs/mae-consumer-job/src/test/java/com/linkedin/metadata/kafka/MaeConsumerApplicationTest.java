package com.linkedin.metadata.kafka;

import static org.testng.AssertJUnit.*;

import com.linkedin.metadata.entity.EntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {MaeConsumerApplication.class, MaeConsumerApplicationTestConfiguration.class})
public class MaeConsumerApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired private EntityService _mockEntityService;

  @Test
  public void testMaeConsumerAutoWiring() {
    assertNotNull(_mockEntityService);
  }
}
