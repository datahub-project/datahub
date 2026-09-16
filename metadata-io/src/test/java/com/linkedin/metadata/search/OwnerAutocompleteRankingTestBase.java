package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearch2SearchClientShim;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import java.net.URISyntaxException;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies autocomplete RANKING (not just query shape) for the owner/people typeahead fix in {@link
 * com.linkedin.metadata.search.elasticsearch.query.request.AutocompleteRequestHandler}.
 *
 * <p>Uses the real {@code corpuser} EntitySpec (not the synthetic {@code testEntity} schema from
 * {@link SearchServiceTestBase}) so the corpuser/corpgroup-scoped per-token prefix behavior
 * actually applies, and indexes ad-hoc documents directly rather than relying on the checked-in
 * gzip sample-data fixture, so the specific name collision below is guaranteed to exist.
 */
public abstract class OwnerAutocompleteRankingTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract SearchClientShim<?> getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  @Nonnull
  protected abstract String getElasticSearchImplementation();

  @Nonnull
  protected abstract SearchConfiguration getSearchConfiguration();

  protected OperationContext operationContext;
  protected ElasticSearchService elasticSearchService;
  private V2LegacySettingsBuilder settingsBuilder;

  @BeforeClass
  public void setup() throws RemoteInvocationException, URISyntaxException {
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
            new ConfiguredIndexPrefixResolver("owner_autocomplete_ranking_test"),
            SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

    OperationContext testOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            TestOperationContexts.defaultEntityRegistry());

    MappingsBuilder mappingsBuilder =
        createDelegatingMappingsBuilder(SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);
    SearchContext searchContext =
        SearchContext.builder()
            .indexConvention(indexConvention)
            .searchableFieldTypes(
                ESUtils.buildSearchableFieldTypes(
                    testOpContext.getEntityRegistry(), mappingsBuilder))
            .searchableFieldPaths(
                ESUtils.buildSearchableFieldPaths(testOpContext.getEntityRegistry()))
            .build();

    operationContext =
        testOpContext.toBuilder()
            .searchContext(searchContext)
            .build(testOpContext.getSessionAuthentication(), true)
            .asSession(RequestContext.TEST, Authorizer.EMPTY, TestOperationContexts.TEST_USER_AUTH);

    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();
    IndexConvention mockIndexConvention = mock(IndexConvention.class);
    when(mockIndexConvention.isV2EntityIndexType(anyString())).thenReturn(true);
    settingsBuilder = new V2LegacySettingsBuilder(indexConfiguration, mockIndexConvention);

    ElasticSearchConfiguration esConfig =
        TEST_OS_SEARCH_CONFIG.toBuilder().search(getSearchConfiguration()).build();
    ESSearchDAO searchDAO =
        new ESSearchDAO(
            getSearchClient(),
            esConfig.getSearch().isPointInTimeCreationEnabled(),
            esConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    ESBrowseDAO browseDAO =
        new ESBrowseDAO(
            getSearchClient(),
            esConfig,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
    ESWriteDAO writeDAO = new ESWriteDAO(esConfig, getSearchClient(), getBulkProcessor());
    elasticSearchService =
        new ElasticSearchService(
            getIndexBuilder(),
            TEST_SEARCH_SERVICE_CONFIG,
            TEST_ES_SEARCH_CONFIG,
            new V2MappingsBuilder(
                TEST_ES_SEARCH_CONFIG.getEntityIndex(),
                OpenSearch2SearchClientShim.PARTIAL_NGRAM_CONFIG),
            settingsBuilder,
            searchDAO,
            browseDAO,
            writeDAO);
    elasticSearchService.reindexAll(operationContext, Collections.emptySet());
  }

  @BeforeMethod
  public void wipe() throws Exception {
    syncAfterWrite(getBulkProcessor());
    elasticSearchService.clear(operationContext);
    syncAfterWrite(getBulkProcessor());
  }

  @Test
  public void testMultiTokenPrefixMatchRanksAboveSingleTokenMatch() throws Exception {
    // "Johnathan Killroy" prefix-matches BOTH query tokens ("John" -> "Johnathan", "K" ->
    // "Killroy"), while "John Fitzgerald" only prefix-matches the first token ("K" does not
    // match "Fitzgerald"). Per RFE (/tmp/rfe.md item 2), AutocompleteRequestHandler requires every
    // token to prefix-match (one MUST per token) for corpuser/corpgroup, so "John K" returns
    // "Johnathan Killroy" and not "John Fitzgerald". Without that, the OR-only bool_prefix query
    // returns both, and on any real-sized index the whole-term "john" match (high IDF) outranks
    // the constant-scored "k" prefix match, putting the wrong person first.
    Urn fitzgeraldUrn = UrnUtils.getUrn("urn:li:corpuser:jfitzgerald");
    ObjectNode fitzgeraldDoc = JsonNodeFactory.instance.objectNode();
    fitzgeraldDoc.set("urn", JsonNodeFactory.instance.textNode(fitzgeraldUrn.toString()));
    fitzgeraldDoc.set("fullName", JsonNodeFactory.instance.textNode("John Fitzgerald"));
    fitzgeraldDoc.set("displayName", JsonNodeFactory.instance.textNode("John Fitzgerald"));
    fitzgeraldDoc.set("ldap", JsonNodeFactory.instance.textNode("jfitzgerald"));
    elasticSearchService.upsertDocument(
        operationContext,
        CORP_USER_ENTITY_NAME,
        fitzgeraldDoc.toString(),
        fitzgeraldUrn.toString());

    Urn killroyUrn = UrnUtils.getUrn("urn:li:corpuser:jkillroy");
    ObjectNode killroyDoc = JsonNodeFactory.instance.objectNode();
    killroyDoc.set("urn", JsonNodeFactory.instance.textNode(killroyUrn.toString()));
    killroyDoc.set("fullName", JsonNodeFactory.instance.textNode("Johnathan Killroy"));
    killroyDoc.set("displayName", JsonNodeFactory.instance.textNode("Johnathan Killroy"));
    killroyDoc.set("ldap", JsonNodeFactory.instance.textNode("jkillroy"));
    elasticSearchService.upsertDocument(
        operationContext, CORP_USER_ENTITY_NAME, killroyDoc.toString(), killroyUrn.toString());

    syncAfterWrite(getBulkProcessor());

    AutoCompleteResult result =
        elasticSearchService.autoComplete(
            operationContext, CORP_USER_ENTITY_NAME, "John K", null, null, 10);

    assertEquals(
        result.getEntities().size(),
        1,
        "Expected only \"Johnathan Killroy\" (matches both tokens \"John\" and \"K\") for query "
            + "\"John K\"; \"John Fitzgerald\" matches only \"John\" and must not be returned");
    assertEquals(result.getEntities().get(0).getUrn(), killroyUrn);

    // Single-token behaviour is unchanged: "John" still finds both, the exact whole-term first.
    AutoCompleteResult single =
        elasticSearchService.autoComplete(
            operationContext, CORP_USER_ENTITY_NAME, "John", null, null, 10);
    assertEquals(single.getEntities().size(), 2);
    assertEquals(single.getEntities().get(0).getUrn(), fitzgeraldUrn);

    // Strictness must not cost recall. The .ngram analyzer already absorbs small typos ("Jon K"
    // still finds Killroy through the "jo" gram), so use a token no gram can match: under the
    // every-token rule "Qx K" matches nobody, ESSearchDAO retries with the ranking-only query,
    // and the "K" prefix still suggests Killroy instead of an empty list.
    AutoCompleteResult unmatchable =
        elasticSearchService.autoComplete(
            operationContext, CORP_USER_ENTITY_NAME, "Qx K", null, null, 10);
    assertEquals(
        unmatchable.getEntities().size(),
        1,
        "zero-result strict query must fall back to the ranking-only query");
    assertEquals(unmatchable.getEntities().get(0).getUrn(), killroyUrn);

    // Punctuation in the typed name is normalized before the per-token clauses are built:
    // "Killroy, John" -> [Killroy, John] -> only Killroy prefix-matches both.
    AutoCompleteResult punctuated =
        elasticSearchService.autoComplete(
            operationContext, CORP_USER_ENTITY_NAME, "Killroy, John", null, null, 10);
    assertEquals(punctuated.getEntities().size(), 1);
    assertEquals(punctuated.getEntities().get(0).getUrn(), killroyUrn);
  }
}
