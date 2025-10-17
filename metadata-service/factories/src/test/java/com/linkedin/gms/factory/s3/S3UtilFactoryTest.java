package com.linkedin.gms.factory.s3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.graphql.util.S3Util;
import com.linkedin.entity.client.EntityClient;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@Configuration
class TestStsClientConfiguration {

  @Bean
  @Primary
  public StsClient mockStsClient() {
    StsClient mockClient = org.mockito.Mockito.mock(StsClient.class);

    // Mock STS assumeRole to return fake credentials with future expiry
    Credentials mockCredentials =
        Credentials.builder()
            .accessKeyId("test-access-key")
            .secretAccessKey("test-secret-key")
            .sessionToken("test-session-token")
            .expiration(Instant.now().plusSeconds(3600)) // 1 hour from now
            .build();

    AssumeRoleResponse mockResponse =
        AssumeRoleResponse.builder().credentials(mockCredentials).build();

    when(mockClient.assumeRole(any(AssumeRoleRequest.class))).thenReturn(mockResponse);

    return mockClient;
  }
}

@SpringBootTest(classes = {S3UtilFactory.class, TestStsClientConfiguration.class})
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
@SpringBootTest(classes = {S3UtilFactory.class, TestStsClientConfiguration.class})
@TestPropertySource(properties = {"datahub.s3.roleArn=arn:aws:iam::123456789012:role/test-role"})
class S3UtilFactoryWithRoleArnTest extends AbstractTestNGSpringContextTests {

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
  public void testS3UtilCreationWithRoleArn() {
    // When/Then
    assertNotNull(s3Util, "S3Util bean should be created successfully with STS role ARN");
    assertNotNull(entityClient, "EntityClient should be injected");
  }
}
