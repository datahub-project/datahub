package com.linkedin.metadata.billing.metronome;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.billing.BillingException;
import com.linkedin.metadata.billing.BillingProvider;
import com.linkedin.metadata.billing.contract.ContractSpec;
import com.linkedin.metadata.billing.contract.RecurringCreditSpec;
import com.linkedin.metadata.config.BillingConfiguration;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * This client communicates with Metronome's REST API to manage customers and contracts.
 *
 * <p>Metronome API Endpoints: - POST /v1/customers - Create a new customer - POST
 * /v1/contracts/create - Create a contract with recurring credits
 *
 * @see <a href="https://docs.metronome.com/guides/get-started/api-quickstart">Metronome API
 *     Quickstart</a>
 */
@Slf4j
public class MetronomeClient implements BillingProvider {

  private final CloseableHttpClient httpClient;
  private final BillingConfiguration.MetronomeConfiguration config;
  private final ObjectMapper objectMapper;

  private static final String CUSTOMERS_ENDPOINT = "/v1/customers";
  private static final String CONTRACTS_ENDPOINT = "/v1/contracts/create";
  private static final String INGEST_ENDPOINT = "/v1/ingest";
  private static final String BALANCES_ENDPOINT = "/v1/contracts/customerBalances/list";

  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String BEARER_PREFIX = "Bearer ";

  private static final DateTimeFormatter ISO_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

  /**
   * Construct a MetronomeClient.
   *
   * @param httpClient Apache HTTP client for making requests
   * @param config Metronome-specific configuration (API key, base URL, plan ID, etc.)
   */
  public MetronomeClient(
      @Nonnull CloseableHttpClient httpClient,
      @Nonnull BillingConfiguration.MetronomeConfiguration config) {
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.objectMapper = new ObjectMapper();
  }

  @Override
  @Nonnull
  public String provisionCustomer(
      @Nonnull String customerName, @Nonnull List<ContractSpec> contracts) throws BillingException {
    Objects.requireNonNull(customerName, "customerName must not be null");
    Objects.requireNonNull(contracts, "contracts must not be null");

    if (contracts.isEmpty()) {
      throw new IllegalArgumentException("At least one contract must be provided");
    }

    log.info(
        "Provisioning Metronome customer '{}' with {} contract(s)", customerName, contracts.size());

    // Step 1: Check if customer already exists, create if not
    String metronomeCustomerId = getCustomerByIngestAlias(customerName);

    if (metronomeCustomerId != null) {
      log.info(
          "Customer '{}' already exists with ID: {}, adding contracts",
          customerName,
          metronomeCustomerId);
    } else {
      // Customer doesn't exist, create it
      log.info("Customer '{}' does not exist, creating new customer", customerName);
      metronomeCustomerId = createCustomer(customerName);
    }

    // Step 2: Create each contract for the customer (whether just created or already existed)
    for (int i = 0; i < contracts.size(); i++) {
      ContractSpec contractSpec = contracts.get(i);
      log.info("Creating contract {}/{}: {}", i + 1, contracts.size(), contractSpec.getName());
      createContract(metronomeCustomerId, contractSpec);
    }

    return metronomeCustomerId;
  }

  /**
   * Get Metronome customer ID by ingest alias.
   *
   * <p>Queries the Metronome API to find a customer by their ingest alias. This allows us to
   * retrieve the customer ID without storing it.
   *
   * @param ingestAlias The ingest alias (typically the hostname)
   * @return Metronome's internal customer ID, or null if not found
   * @throws BillingException if the API call fails
   */
  @javax.annotation.Nullable
  public String getCustomerByIngestAlias(@Nonnull String ingestAlias) throws BillingException {
    Objects.requireNonNull(ingestAlias, "ingestAlias must not be null");

    log.debug("Querying Metronome for customer with ingest alias: {}", ingestAlias);

    try {
      Map<String, String> queryParams = new HashMap<>();
      queryParams.put("ingest_alias", ingestAlias);

      String responseBody = get(CUSTOMERS_ENDPOINT, queryParams);
      JsonNode result = objectMapper.readTree(responseBody);
      JsonNode data = result.get("data");

      if (data != null && data.isArray() && data.size() > 0) {
        // Get the first customer's ID
        JsonNode firstCustomer = data.get(0);
        if (firstCustomer.has("id")) {
          String customerId = firstCustomer.get("id").asText();
          log.debug("Found Metronome customer with ingest alias '{}': {}", ingestAlias, customerId);
          return customerId;
        }
      }

      log.debug("No customer found with ingest alias: {}", ingestAlias);
      return null;
    } catch (BillingException e) {
      throw e;
    } catch (Exception e) {
      throw new BillingException("Failed to get customer by ingest alias: " + ingestAlias, e);
    }
  }

  /**
   * Create a customer in Metronome.
   *
   * <p>The customerName (hostname) is used as both the customer name and the ingest alias. This
   * allows us to query the customer later by ingest alias without storing the customer ID.
   *
   * @param customerName The customer name (hostname) to use in Metronome, also used as ingest alias
   * @return Metronome's internal customer ID from the response
   * @throws BillingException if customer creation fails
   * @see <a href="https://docs.metronome.com/api-reference/customers/create-a-customer">Metronome
   *     API - Create a Customer</a>
   */
  private String createCustomer(String customerName) throws BillingException {
    Map<String, Object> body = new HashMap<>();
    body.put("name", customerName);

    // Use customerName (hostname) as ingest alias so we can query the customer later
    List<String> ingestAliases = new ArrayList<>();
    ingestAliases.add(customerName);
    body.put("ingest_aliases", ingestAliases);

    try {
      String response = post(CUSTOMERS_ENDPOINT, body);
      JsonNode result = objectMapper.readTree(response);

      // Extract Metronome's internal customer ID from response
      JsonNode data = result.get("data");
      if (data != null && data.has("id")) {
        String metronomeCustomerId = data.get("id").asText();
        log.info(
            "Created Metronome customer '{}' with ingest alias '{}' (Metronome ID: {})",
            customerName,
            customerName,
            metronomeCustomerId);
        return metronomeCustomerId;
      }

      throw new BillingException("Metronome customer creation response missing 'id' field");
    } catch (BillingException e) {
      throw e;
    } catch (Exception e) {
      throw new BillingException("Failed to create customer '" + customerName + "'", e);
    }
  }

  /**
   * Create a contract for a customer based on contract specification.
   *
   * <p>This method creates a contract in Metronome with recurring credits based on the provided
   * specification. The contract can be for free trials, standard subscriptions, or custom
   * agreements.
   *
   * @param metronomeCustomerId Metronome's internal customer identifier
   * @param contractSpec Contract specification with rate card, credits, and dates
   * @throws BillingException if contract creation fails
   * @see <a href="https://docs.metronome.com/api-reference/contracts/create-a-contract">Metronome
   *     API - Create a Contract</a>
   */
  private void createContract(String metronomeCustomerId, ContractSpec contractSpec)
      throws BillingException {
    Objects.requireNonNull(metronomeCustomerId, "Customer ID must not be null");
    Objects.requireNonNull(contractSpec, "Contract spec must not be null");

    // Validate contract spec
    contractSpec.validate();

    // Convert LocalDate to Metronome's ISO format timestamp
    String startingAt = formatDateForMetronome(contractSpec.getStartDate());

    // Build contract request body
    Map<String, Object> body = new HashMap<>();
    body.put("customer_id", metronomeCustomerId);
    body.put("starting_at", startingAt);
    body.put("name", contractSpec.getName());
    body.put("rate_card_id", contractSpec.getRateCardId());

    // Build list of recurring credits from contract spec
    List<Map<String, Object>> recurringCreditsList = new ArrayList<>();
    for (RecurringCreditSpec creditSpec : contractSpec.getRecurringCredits()) {
      Map<String, Object> recurringCredit = new HashMap<>();
      recurringCredit.put("product_id", creditSpec.getProductId());
      recurringCredit.put("priority", creditSpec.getPriority());
      recurringCredit.put("starting_at", startingAt);
      recurringCredit.put("recurrence_frequency", "monthly");

      // Define credit allocation
      Map<String, Object> accessAmount = new HashMap<>();
      accessAmount.put("unit_price", 1);
      accessAmount.put("credit_type_id", creditSpec.getCreditTypeId());
      accessAmount.put("quantity", creditSpec.getMonthlyCredits());
      recurringCredit.put("access_amount", accessAmount);

      // Set commit duration: 1 period = 1 month
      Map<String, Object> commitDuration = new HashMap<>();
      commitDuration.put("value", 1);
      commitDuration.put("unit", "periods");
      recurringCredit.put("commit_duration", commitDuration);

      recurringCreditsList.add(recurringCredit);
    }

    body.put("recurring_credits", recurringCreditsList);

    try {
      post(CONTRACTS_ENDPOINT, body);
      log.info(
          "Created contract '{}' for customer: {} with {} product(s) (start: {})",
          contractSpec.getName(),
          metronomeCustomerId,
          recurringCreditsList.size(),
          startingAt);
    } catch (Exception e) {
      throw new BillingException(
          "Failed to create contract '"
              + contractSpec.getName()
              + "' for customer: "
              + metronomeCustomerId,
          e);
    }
  }

  /**
   * Format a LocalDate to Metronome's ISO timestamp format.
   *
   * @param date The date to format
   * @return ISO formatted timestamp string (e.g., "2024-01-01T00:00:00.000Z")
   */
  private String formatDateForMetronome(LocalDate date) {
    ZonedDateTime zonedDateTime = date.atStartOfDay(ZoneId.of("UTC"));
    return ISO_FORMATTER.format(zonedDateTime.toInstant());
  }

  /**
   * Execute a POST request to Metronome API.
   *
   * @param endpoint API endpoint (e.g., "/v1/customers")
   * @param body Request body (will be serialized to JSON)
   * @return Response body as string
   * @throws IOException if request fails
   */
  private String post(String endpoint, Object body) throws IOException {
    try {
      String url = config.getBaseUrl() + endpoint;
      HttpPost request = new HttpPost(url);

      // Set headers
      request.setHeader("Content-Type", CONTENT_TYPE_JSON);
      request.setHeader(AUTHORIZATION_HEADER, BEARER_PREFIX + config.getApiKey());

      // Serialize body to JSON
      String jsonBody = objectMapper.writeValueAsString(body);
      request.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

      return sendRequest(request);
    } catch (IOException e) {
      throw e; // Re-throw IOException as-is
    } catch (Exception e) {
      throw new IOException("Failed to execute POST request", e);
    }
  }

  /**
   * Execute a GET request to Metronome API with query parameters.
   *
   * @param endpoint API endpoint (e.g., "/v1/customers")
   * @param queryParams Query parameters to append to the URL
   * @return Response body as string
   * @throws IOException if request fails
   */
  private String get(String endpoint, Map<String, String> queryParams) throws IOException {
    try {
      // Build URL with query parameters
      URIBuilder uriBuilder = new URIBuilder(config.getBaseUrl() + endpoint);
      if (queryParams != null) {
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
          uriBuilder.addParameter(entry.getKey(), entry.getValue());
        }
      }

      String url = uriBuilder.build().toString();
      HttpGet request = new HttpGet(url);

      // Set headers
      request.setHeader(AUTHORIZATION_HEADER, BEARER_PREFIX + config.getApiKey());

      return sendRequest(request);
    } catch (IOException e) {
      throw e; // Re-throw IOException as-is
    } catch (java.net.URISyntaxException e) {
      throw new IOException("Failed to build request URL", e);
    } catch (Exception e) {
      throw new IOException("Failed to execute GET request", e);
    }
  }

  /**
   * Send an HTTP request to Metronome API and handle the response.
   *
   * @param request The HTTP request to send (GET, POST, etc.)
   * @return Response body as string
   * @throws IOException if request fails or returns non-2xx status code
   */
  private String sendRequest(org.apache.http.client.methods.HttpUriRequest request)
      throws IOException {
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      HttpEntity entity = response.getEntity();
      String responseBody =
          entity != null ? EntityUtils.toString(entity, StandardCharsets.UTF_8) : "";

      if (statusCode >= HttpStatus.SC_OK && statusCode < HttpStatus.SC_MULTIPLE_CHOICES) {
        return responseBody;
      } else {
        log.error(
            "Metronome API error: {} {} - Status: {} - Response: {}",
            request.getMethod(),
            request.getURI(),
            statusCode,
            responseBody);
        throw new IOException(
            String.format("Metronome API returned HTTP %d: %s", statusCode, responseBody));
      }
    }
  }

  @Override
  public boolean hasRemainingCredits(@Nonnull String customerId, @Nonnull String productId)
      throws BillingException {
    Objects.requireNonNull(customerId, "customerId must not be null");
    Objects.requireNonNull(productId, "productId must not be null");

    int remaining = getRemainingBalance(customerId, productId);
    boolean hasCredits = remaining > 0;
    return hasCredits;
  }

  /**
   * Get remaining balance for a customer for a specific product.
   *
   * <p>Returns the number of credits remaining in the customer's account for the specified product.
   * Filters the balance entries by product_id.
   *
   * @param customerId The billing provider's internal customer ID
   * @param productId The product ID to check balance for
   * @return Number of remaining credits (0 if exhausted)
   * @throws BillingException if balance retrieval fails
   */
  @Override
  public int getRemainingBalance(@Nonnull String customerId, @Nonnull String productId)
      throws BillingException {
    Objects.requireNonNull(customerId, "customerId must not be null");
    Objects.requireNonNull(productId, "productId must not be null");

    Map<String, Object> body = new HashMap<>();
    body.put("customer_id", customerId);
    body.put("include_ledgers", false);
    body.put("include_balance", true);
    body.put("limit", 25);
    body.put("covering_date", ISO_FORMATTER.format(Instant.now()));

    try {
      String response = post(BALANCES_ENDPOINT, body);
      JsonNode result = objectMapper.readTree(response);

      JsonNode data = result.get("data");
      if (data != null && data.isArray() && data.size() > 0) {
        // Iterate through balance entries to find the one matching the product_id
        for (int i = 0; i < data.size(); i++) {
          JsonNode entry = data.get(i);

          // Check if entry has nested product object with id
          if (entry.has("product") && entry.has("balance")) {
            JsonNode product = entry.get("product");
            if (product != null && product.has("id")) {
              String entryProductId = product.get("id").asText();

              if (productId.equals(entryProductId)) {
                int remaining = entry.get("balance").asInt();
                return remaining;
              }
            }
          }
        }

        // Product not found in balance entries
        log.warn("No balance data found for customer: {} and product: {}", customerId, productId);
        return 1;
      }

      log.error("No balance data found for customer: {}", customerId);
      throw new BillingException("Unable to retrieve balance data for customer: " + customerId);
    } catch (BillingException e) {
      throw e;
    } catch (Exception e) {
      throw new BillingException(
          "Failed to get balance for customer: " + customerId + ", product: " + productId, e);
    }
  }

  /**
   * Report usage to Metronome.
   *
   * <p>Reports usage to Metronome. This should be called after a successful AI chat response.
   *
   * @param customerId The billing provider's internal customer ID
   * @param eventType The type of event being reported (e.g., "ai_message", "data_export", etc.)
   * @param transactionId Unique identifier for this usage event (for idempotency)
   * @param quantity Number of credits to deduct (typically 1 per AI chat answer)
   * @param additionalProperties Additional properties to include with the usage event (e.g.,
   *     conversation_id, user_id)
   * @throws BillingException if usage reporting fails
   */
  @Override
  public void reportUsage(
      @Nonnull String customerId,
      @Nonnull String eventType,
      @Nonnull String transactionId,
      int quantity,
      @Nonnull Map<String, Object> additionalProperties)
      throws BillingException {
    Objects.requireNonNull(customerId, "customerId must not be null");
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(transactionId, "transactionId must not be null");
    Objects.requireNonNull(additionalProperties, "additionalProperties must not be null");

    Map<String, Object> event = new HashMap<>();
    event.put("customer_id", customerId);
    event.put("transaction_id", transactionId);
    event.put("event_type", eventType);
    event.put("timestamp", ISO_FORMATTER.format(Instant.now()));

    // Build properties with additional context
    Map<String, Object> properties = new HashMap<>();
    properties.putAll(additionalProperties); // Add conversation_id, user_id, etc.

    event.put("properties", properties);

    List<Map<String, Object>> events = new ArrayList<>();
    events.add(event);

    try {
      post(INGEST_ENDPOINT, events);
      log.debug(
          "Successfully reported usage to Metronome for customer: {} (event_type: {}, transaction: {})",
          customerId,
          eventType,
          transactionId);
    } catch (Exception e) {
      log.error("Failed to report usage to Metronome", e);
      throw new BillingException(
          "Failed to report usage for customer: " + customerId + ", transaction: " + transactionId,
          e);
    }
  }
}
