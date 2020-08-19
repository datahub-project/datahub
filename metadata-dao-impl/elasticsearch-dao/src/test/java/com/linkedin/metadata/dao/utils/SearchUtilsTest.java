package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static com.linkedin.metadata.dao.utils.SearchUtils.*;
import static org.testng.Assert.*;

public class SearchUtilsTest {
  @Test
  public void testGetRequestMap() {
    // Empty filter
    final Filter filter1 = QueryUtils.newFilter(null);
    final Map<String, String> actual1 = getRequestMap(filter1);
    assertTrue(actual1.isEmpty());

    // Filter with criteria with default condition
    final Map requestParams = Collections.unmodifiableMap(new HashMap() {
      {
        put("key1", "value1");
        put("key2", "value2");
      }
    });
    final Filter filter2 = QueryUtils.newFilter(requestParams);
    final Map<String, String> actual2 = getRequestMap(filter2);
    assertEquals(actual2, requestParams);

    // Filter with unsupported condition
    final Filter filter3 = new Filter().setCriteria(new CriterionArray(
        new Criterion().setField("key").setValue("value").setCondition(Condition.CONTAIN)
    ));
    assertThrows(UnsupportedOperationException.class, () -> getRequestMap(filter3));
  }
}
