package com.linkedin.metadata.restli;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SortCriterion;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.testing.EntityValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class SearchableClientTest {

  public static class TestSearchableClient implements SearchableClient<EntityValue> {

    @Override
    @Nonnull
    public CollectionResponse<EntityValue> search(@Nonnull String input, @Nullable Map<String, String> requestFilters,
        @Nullable SortCriterion sortCriterion, int start, int count) throws RemoteInvocationException {

      return new CollectionResponse<>(EntityValue.class);
    }

    @Override
    @Nonnull
    public AutoCompleteResult autocomplete(@Nonnull String query, @Nullable String field,
        @Nullable Map<String, String> requestFilters, int limit) throws RemoteInvocationException {

      return new AutoCompleteResult().setQuery(query).setSuggestions(new StringArray(
          Arrays.asList("res1", "res2", "res3")));
    }
  }

  @Test
  public void testClient() throws RemoteInvocationException {
    TestSearchableClient testSearchableClient = new TestSearchableClient();
    assertEquals(testSearchableClient.autocomplete("test", "field", new HashMap<>(), 1).getQuery(), "test");
    assertEquals(testSearchableClient.autocomplete("test", "field", null, 1).getSuggestions(),
        new StringArray(Arrays.asList("res1", "res2", "res3")));
  }

}