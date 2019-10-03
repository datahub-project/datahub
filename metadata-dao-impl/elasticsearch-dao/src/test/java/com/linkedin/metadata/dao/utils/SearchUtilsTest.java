package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.Filter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class SearchUtilsTest {
  @Test
  public void testGetFilter() {
    final Map requestMap = Collections.unmodifiableMap(new HashMap() {
      {
        put("key1", "value1");
        put("key2", "value2");
      }
    });
    final Filter filter = SearchUtils.getFilter(requestMap);
    assertEquals(filter.getCriteria().stream().collect(Collectors.toMap(Criterion::getField, Criterion::getValue)),
        requestMap);
  }
}
