package com.linkedin.metadata.search.api;

import static org.testng.Assert.assertEquals;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.SearchFlags;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class SearchDocFieldFetchConfigTest {

  @Test
  public void resolveReturnsDefaultsWhenFlagsAbsent() {
    assertEquals(
        SearchDocFieldFetchConfig.resolve(
            SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SCROLL, null),
        Set.of("urn"));
  }

  @Test
  public void resolveUnionsRequestedFieldsPreservingOrder() {
    SearchFlags flags =
        new SearchFlags()
            .setFetchExtraFields(new StringArray(List.of("parentDomain", "urn", "name")));
    assertEquals(
        List.copyOf(
            SearchDocFieldFetchConfig.resolve(
                SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SCROLL, flags)),
        List.of("urn", "parentDomain", "name"));
  }

  @Test
  public void resolveIgnoresBlankRequestedFields() {
    SearchFlags flags = new SearchFlags().setFetchExtraFields(new StringArray(List.of(" ", "")));
    assertEquals(
        SearchDocFieldFetchConfig.resolve(
            SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SCROLL, flags),
        Set.of("urn"));
  }
}
