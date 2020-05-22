package com.linkedin.metadata.dao.utils;

import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.Filter;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class QueryUtilTest {

  @Test
  public void testNewCriterion() {
    Criterion criterion = QueryUtils.newCriterion("foo", "bar");
    assertEquals(criterion, new Criterion().setField("foo").setValue("bar").setCondition(Condition.EQUAL));

    criterion = QueryUtils.newCriterion("f", "v", Condition.CONTAIN);
    assertEquals(criterion, new Criterion().setField("f").setValue("v").setCondition(Condition.CONTAIN));
  }

  @Test
  public void testNewFilter() {
    Map<String, String> params = Collections.singletonMap("foo", "bar");

    Filter filter = QueryUtils.newFilter(params);
    assertEquals(filter.getCriteria().size(), 1);
    assertEquals(filter.getCriteria().get(0), QueryUtils.newCriterion("foo", "bar"));

    // test null values
    Map<String, String> paramsWithNulls = Collections.singletonMap("foo", null);
    filter = QueryUtils.newFilter(paramsWithNulls);
    assertEquals(filter.getCriteria().size(), 0);
  }

  private boolean hasAspectVersion(Set<AspectVersion> aspectVersions, String aspectName, long version) {
    return aspectVersions.stream()
        .filter(av -> av.getAspect().equals(aspectName) && av.getVersion().equals(version))
        .count() == 1;
  }

  @Test
  public void testGetTotalPageCount() {

    int totalPageCount = QueryUtils.getTotalPageCount(23, 10);
    assertEquals(totalPageCount, 3);

    totalPageCount = QueryUtils.getTotalPageCount(20, 10);
    assertEquals(totalPageCount, 2);

    totalPageCount = QueryUtils.getTotalPageCount(19, 10);
    assertEquals(totalPageCount, 2);

    totalPageCount = QueryUtils.getTotalPageCount(9, 0);
    assertEquals(totalPageCount, 0);
  }

  @Test
  public void testIsHavingMore() {

    int totalCount = 23;
    int totalPageCount = QueryUtils.getTotalPageCount(totalCount, 10); // 3

    boolean havingMore = QueryUtils.isHavingMore(0, 10, totalPageCount);
    assertTrue(havingMore);

    havingMore = QueryUtils.isHavingMore(10, 10, totalPageCount);
    assertTrue(havingMore);

    havingMore = QueryUtils.isHavingMore(20, 10, totalPageCount);
    assertFalse(havingMore);

    havingMore = QueryUtils.isHavingMore(30, 10, totalPageCount);
    assertFalse(havingMore);

    havingMore = QueryUtils.isHavingMore(30, 0, totalPageCount);
    assertFalse(havingMore);
  }

  private AspectVersion makeAspectVersion(String aspectName, long version) {
    return new AspectVersion().setAspect(aspectName).setVersion(version);
  }
}