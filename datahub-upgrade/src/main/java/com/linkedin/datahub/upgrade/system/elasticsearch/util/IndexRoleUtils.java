package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

/**
 * Utility class for creating and managing Elasticsearch/OpenSearch users and roles for DataHub
 * authentication. This class provides methods to set up security roles and users required for
 * DataHub to access Elasticsearch/OpenSearch clusters.
 *
 * <p>The class handles both standard Elasticsearch and AWS OpenSearch scenarios, using appropriate
 * APIs and configurations for each environment.
 */
@Slf4j
public class IndexRoleUtils {

  /** Extracts the response body from a Response or RawResponse for error logging. */
  private static String extractResponseBody(Object response) {
    try {
      HttpEntity entity = null;
      if (response instanceof Response) {
        entity = ((Response) response).getEntity();
      } else if (response instanceof RawResponse) {
        entity = ((RawResponse) response).getEntity();
      }

      if (entity != null) {
        return new String(entity.getContent().readAllBytes(), StandardCharsets.UTF_8);
      }
    } catch (Exception e) {
      log.debug("Failed to read response body", e);
    }
    return "No response body";
  }

  /**
   * Creates a user and role for Elasticsearch Cloud environments.
   *
   * <p>This method creates both a security role with appropriate permissions and a user associated
   * with that role for Elasticsearch Cloud environments. The role is configured with cluster
   * monitoring permissions and full access to indices matching the prefix pattern.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param roleName the name of the role to create (e.g., "prod_access")
   * @param username the username to create (e.g., "datahub")
   * @param password the password for the user
   * @param prefix the index prefix to apply to role permissions (e.g., "prod_")
   * @throws IOException if there's an error reading the role/user templates or making requests
   */
  public static void createElasticsearchCloudUser(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String roleName,
      String username,
      String password,
      String prefix)
      throws IOException {

    // Create role first
    createElasticsearchCloudRole(esComponents, roleName, prefix);

    // Then create user
    createElasticsearchCloudUserInternal(esComponents, username, password, roleName);
  }

  /**
   * Creates a security role for Elasticsearch Cloud environments.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param roleName the name of the role to create
   * @param prefix the index prefix to apply to role permissions
   * @throws IOException if there's an error creating the role
   */
  public static void createElasticsearchCloudRole(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String roleName,
      String prefix)
      throws IOException {
    try {
      String roleJson =
          IndexUtils.loadResourceAsString("/index/user/access_policy_data_es_cloud.json")
              .replace("PREFIX", prefix);

      String endpoint = "/_security/role/" + roleName;

      // Use retry logic for role creation
      boolean success =
          IndexUtils.retryWithBackoff(
              5,
              2000,
              () -> {
                try {
                  RawResponse response =
                      IndexUtils.performPutRequest(esComponents, endpoint, roleJson);

                  int statusCode = response.getStatusLine().getStatusCode();
                  if (statusCode == 200 || statusCode == 201) {
                    log.info("Successfully created Elasticsearch Cloud role: {}", roleName);
                    return true;
                  } else if (statusCode == 409) {
                    log.info("Elasticsearch Cloud role {} already exists", roleName);
                    return true; // Consider this a success since role exists
                  } else {
                    log.warn("Elasticsearch Cloud role creation returned status: {}", statusCode);
                    throw new RuntimeException("Retryable error: " + statusCode);
                  }
                } catch (ResponseException e) {
                  if (e.getResponse().getStatusLine().getStatusCode() == 409) {
                    log.info("Elasticsearch Cloud role {} already exists", roleName);
                    return true;
                  } else {
                    throw new RuntimeException(
                        "Retryable error: " + e.getResponse().getStatusLine().getStatusCode());
                  }
                }
              });

      if (!success) {
        throw new IOException(
            "Failed to create Elasticsearch Cloud role after retries: " + roleName);
      }

    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("Elasticsearch Cloud role {} already exists", roleName);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates a user for Elasticsearch Cloud environments.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param username the username to create
   * @param password the password for the user
   * @param roleName the role to assign to the user
   * @throws IOException if there's an error creating the user
   */
  private static void createElasticsearchCloudUserInternal(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String username,
      String password,
      String roleName)
      throws IOException {
    try {
      String userJson =
          IndexUtils.loadResourceAsString("/index/user/user_data_es_cloud.json")
              .replace("ELASTICSEARCH_PASSWORD", password)
              .replace("PREFIX", roleName);

      String endpoint = "/_security/user/" + username;

      // Use retry logic for user creation
      boolean success =
          IndexUtils.retryWithBackoff(
              5,
              2000,
              () -> {
                try {
                  Request request = new Request("PUT", endpoint);
                  request.setJsonEntity(userJson);

                  RawResponse response =
                      esComponents.getSearchClient().performLowLevelRequest(request);

                  int statusCode = response.getStatusLine().getStatusCode();
                  if (statusCode == 200 || statusCode == 201) {
                    log.info("Successfully created Elasticsearch Cloud user: {}", username);
                    return true;
                  } else if (statusCode == 409) {
                    log.info("Elasticsearch Cloud user {} already exists", username);
                    return true; // Consider this a success since user exists
                  } else {
                    log.warn("Elasticsearch Cloud user creation returned status: {}", statusCode);
                    throw new RuntimeException("Retryable error: " + statusCode);
                  }
                } catch (ResponseException e) {
                  if (e.getResponse().getStatusLine().getStatusCode() == 409) {
                    log.info("Elasticsearch Cloud user {} already exists", username);
                    return true;
                  } else {
                    throw new RuntimeException(
                        "Retryable error: " + e.getResponse().getStatusLine().getStatusCode());
                  }
                }
              });

      if (!success) {
        throw new IOException(
            "Failed to create Elasticsearch Cloud user after retries: " + username);
      }

    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("Elasticsearch Cloud user {} already exists", username);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates a security role for AWS OpenSearch environments.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param roleName the name of the role to create
   * @param prefix the index prefix to apply to role permissions
   * @throws IOException if there's an error creating the role
   */
  public static void createAwsOpenSearchRole(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String roleName,
      String prefix)
      throws IOException {
    try {
      String roleJson =
          IndexUtils.loadResourceAsString("/index/user/aws_role.json").replace("PREFIX", prefix);

      String endpoint = "/_opendistro/_security/api/roles/" + roleName;

      // Use retry logic for role creation
      boolean success =
          IndexUtils.retryWithBackoff(
              5,
              2000,
              () -> {
                try {
                  RawResponse response =
                      IndexUtils.performPutRequest(esComponents, endpoint, roleJson);

                  int statusCode = response.getStatusLine().getStatusCode();
                  if (statusCode == 200 || statusCode == 201) {
                    log.info("Successfully created AWS OpenSearch role: {}", roleName);
                    return true;
                  } else if (statusCode == 409) {
                    log.info("AWS OpenSearch role {} already exists", roleName);
                    return true; // Consider this a success since role exists
                  } else {
                    log.warn("AWS OpenSearch role creation returned status: {}", statusCode);
                    throw new RuntimeException("Retryable error: " + statusCode);
                  }
                } catch (ResponseException e) {
                  if (e.getResponse().getStatusLine().getStatusCode() == 409) {
                    log.info("AWS OpenSearch role {} already exists", roleName);
                    return true;
                  } else {
                    throw new RuntimeException(
                        "Retryable error: " + e.getResponse().getStatusLine().getStatusCode());
                  }
                }
              });

      if (!success) {
        throw new IOException("Failed to create AWS OpenSearch role after retries: " + roleName);
      }

    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("AWS OpenSearch role {} already exists", roleName);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates a user for AWS OpenSearch environments.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param username the username to create
   * @param password the password for the user
   * @param roleName the role to assign to the user
   * @param operationContext the operation context for JSON parsing
   * @throws IOException if there's an error creating the user
   */
  public static void createAwsOpenSearchUser(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String username,
      String password,
      String roleName,
      String iamRoleArn,
      OperationContext operationContext)
      throws IOException {
    try {
      ObjectMapper mapper = operationContext.getObjectMapper();

      // Build the user JSON
      ObjectNode userNode = mapper.createObjectNode();

      // Add password or hash for IAM-only auth
      if (password != null && !password.isEmpty()) {
        userNode.put("password", password);
      } else {
        userNode.put("hash", "");
        log.info("Using IAM-only authentication, setting empty hash");
      }

      // Add security role
      ArrayNode securityRoles = mapper.createArrayNode();
      securityRoles.add(roleName);
      userNode.set("opendistro_security_roles", securityRoles);

      // Add backend_roles for IAM if provided
      if (iamRoleArn != null && !iamRoleArn.isEmpty()) {
        log.info("Setting backend_roles to IAM role ARN: {}", iamRoleArn);
        ArrayNode backendRoles = mapper.createArrayNode();
        backendRoles.add(iamRoleArn);
        userNode.set("backend_roles", backendRoles);
      }

      String userJson = mapper.writeValueAsString(userNode);

      String endpoint = "/_opendistro/_security/api/internalusers/" + username;

      // Use retry logic for user creation
      boolean success =
          IndexUtils.retryWithBackoff(
              5,
              2000,
              () -> {
                try {
                  RawResponse response =
                      IndexUtils.performPutRequest(esComponents, endpoint, userJson);

                  int statusCode = response.getStatusLine().getStatusCode();
                  if (statusCode == 200 || statusCode == 201) {
                    log.info("Successfully created AWS OpenSearch user: {}", username);
                    return true;
                  } else if (statusCode == 409) {
                    log.info("AWS OpenSearch user {} already exists", username);
                    return true; // Consider this a success since user exists
                  } else {
                    // Log error response body
                    String responseBody = extractResponseBody(response);
                    log.error(
                        "AWS OpenSearch user creation returned status: {}. Response: {}",
                        statusCode,
                        responseBody);
                    throw new RuntimeException(
                        "Retryable error: " + statusCode + " - " + responseBody);
                  }
                } catch (ResponseException e) {
                  int exStatusCode = e.getResponse().getStatusLine().getStatusCode();
                  if (exStatusCode == 409) {
                    log.info("AWS OpenSearch user {} already exists", username);
                    return true;
                  } else {
                    // Log error response body
                    String responseBody = extractResponseBody(e.getResponse());
                    log.error(
                        "AWS OpenSearch user PUT exception. Status: {}. Response: {}",
                        exStatusCode,
                        responseBody);
                    throw new RuntimeException(
                        "Retryable error: " + exStatusCode + " - " + responseBody);
                  }
                } catch (Exception e) {
                  log.error("Unexpected exception during PUT request", e);
                  throw e;
                }
              });

      if (!success) {
        throw new IOException(
            "Failed to create AWS OpenSearch user after retries: "
                + username
                + ". Check logs for error details from OpenSearch.");
      }

    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("AWS OpenSearch user {} already exists", username);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates a role mapping for AWS OpenSearch IAM authentication.
   *
   * <p>This method creates a role mapping that maps an IAM role ARN to an OpenSearch security role.
   * This is used for IAM authentication where no internal user is needed.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param roleName the name of the role to map to (e.g., "prod_access")
   * @param iamRoleArn the IAM role ARN to map (e.g., "arn:aws:iam::123456789012:role/datahub")
   * @throws IOException if there's an error creating the role mapping
   */
  public static void createAwsOpenSearchRoleMapping(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String roleName,
      String iamRoleArn)
      throws IOException {
    try {
      // Load the role mapping template
      String mappingJson =
          IndexUtils.loadResourceAsString("/index/user/aws_roles_mapping.json")
              .replace("IAM_ROLE_ARN", iamRoleArn);

      String endpoint = "/_plugins/_security/api/rolesmapping/" + roleName;

      log.info(
          "Creating role mapping from IAM role {} to OpenSearch role {}", iamRoleArn, roleName);

      // Use retry logic for role mapping creation
      boolean success =
          IndexUtils.retryWithBackoff(
              5,
              2000,
              () -> {
                try {
                  RawResponse response =
                      IndexUtils.performPutRequest(esComponents, endpoint, mappingJson);

                  int statusCode = response.getStatusLine().getStatusCode();
                  if (statusCode == 200 || statusCode == 201) {
                    log.info(
                        "Successfully created AWS OpenSearch role mapping: {} -> {}",
                        iamRoleArn,
                        roleName);
                    return true;
                  } else if (statusCode == 409) {
                    log.info("AWS OpenSearch role mapping {} already exists", roleName);
                    return true; // Consider this a success since mapping exists
                  } else {
                    String responseBody = extractResponseBody(response);
                    log.warn(
                        "AWS OpenSearch role mapping creation returned status: {}. Response: {}",
                        statusCode,
                        responseBody);
                    throw new RuntimeException(
                        "Retryable error: " + statusCode + " - " + responseBody);
                  }
                } catch (ResponseException e) {
                  int exStatusCode = e.getResponse().getStatusLine().getStatusCode();
                  if (exStatusCode == 409) {
                    log.info("AWS OpenSearch role mapping {} already exists", roleName);
                    return true;
                  } else {
                    String responseBody = extractResponseBody(e.getResponse());
                    log.error(
                        "AWS OpenSearch role mapping PUT exception. Status: {}. Response: {}",
                        exStatusCode,
                        responseBody);
                    throw new RuntimeException(
                        "Retryable error: " + exStatusCode + " - " + responseBody);
                  }
                }
              });

      if (!success) {
        throw new IOException(
            "Failed to create AWS OpenSearch role mapping after retries: "
                + roleName
                + ". Check logs for error details from OpenSearch.");
      }

    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("AWS OpenSearch role mapping {} already exists", roleName);
      } else {
        throw e;
      }
    }
  }
}
