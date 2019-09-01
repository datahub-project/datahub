package com.linkedin.metadata.dao.search;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ESSearchDAOTest {
  @Test
  public void testDecoupleArrayToGetSubstringMatch() throws Exception {
    // Test empty fieldVal
    List<String> fieldValList = Collections.emptyList();
    String searchInput = "searchInput";
    List<String> searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 0);

    // Test non-list fieldVal
    String fieldValString = "fieldVal";
    searchInput = "searchInput";
    searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValString, searchInput);
    assertEquals(searchResult.size(), 1);

    // Test list fieldVal with no match
    fieldValList = Arrays.asList("fieldVal1", "fieldVal2", "fieldVal3");
    searchInput = "searchInput";
    searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 0);

    // Test list fieldVal with single match
    searchInput = "val1";
    searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 1);
    assertEquals(searchResult.get(0), "fieldVal1");

    // Test list fieldVal with multiple match
    searchInput = "val";
    searchResult = ESAutoCompleteQueryForHighCardinalityFields
        .decoupleArrayToGetSubstringMatch(fieldValList, searchInput);
    assertEquals(searchResult.size(), 3);
    assertTrue(searchResult.equals(fieldValList));
  }
}
