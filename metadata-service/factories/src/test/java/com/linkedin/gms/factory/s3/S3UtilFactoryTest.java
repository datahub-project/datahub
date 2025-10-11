package com.linkedin.gms.factory.s3;

import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.graphql.util.S3Util;
import com.linkedin.entity.client.EntityClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = {S3UtilFactory.class})
@TestPropertySource(properties = {"datahub.s3.roleArn="})
public class S3UtilFactoryTest extends AbstractTestNGSpringContextTests {

  // Set AWS region before any Spring context initialization
  static {
    System.setProperty("aws.region", "us-east-1");
  }

  @MockitoBean
  @Qualifier("entityClient")
  private EntityClient entityClient;

  @Autowired
  @Qualifier("s3Util")
  private S3Util s3Util;

  @Test
  public void testS3UtilCreationWithoutRoleArn() {
    // When/Then
    assertNotNull(s3Util, "S3Util bean should be created successfully");
    assertNotNull(entityClient, "EntityClient should be injected");
  }

  @Test
  public void testS3UtilBeanName() {
    // Verify the bean is registered with the correct name
    assertNotNull(s3Util, "S3Util bean should be available with name 's3Util'");
  }
}

/** Test class for S3UtilFactory with STS role ARN configuration */
@SpringBootTest(classes = {S3UtilFactory.class})
@TestPropertySource(properties = {"datahub.s3.roleArn=arn:aws:iam::123456789012:role/test-role"})
class S3UtilFactoryWithRoleArnTest extends AbstractTestNGSpringContextTests {

  @MockitoBean
  @Qualifier("entityClient")
  private EntityClient entityClient;

  @Autowired
  @Qualifier("s3Util")
  private S3Util s3Util;

  @Test
  public void testS3UtilCreationWithRoleArn() {
    // When/Then
    assertNotNull(s3Util, "S3Util bean should be created successfully with STS role ARN");
    assertNotNull(entityClient, "EntityClient should be injected");
  }
}

/** Test class for S3UtilFactory with empty/whitespace role ARN configuration */
@SpringBootTest(classes = {S3UtilFactory.class})
@TestPropertySource(properties = {"datahub.s3.roleArn=   "})
class S3UtilFactoryWithEmptyRoleArnTest extends AbstractTestNGSpringContextTests {

  @MockitoBean
  @Qualifier("entityClient")
  private EntityClient entityClient;

  @Autowired
  @Qualifier("s3Util")
  private S3Util s3Util;

  @Test
  public void testS3UtilCreationWithEmptyRoleArn() {
    // When/Then - should fall back to default credentials
    assertNotNull(s3Util, "S3Util bean should be created successfully with empty role ARN");
    assertNotNull(entityClient, "EntityClient should be injected");
  }
}
