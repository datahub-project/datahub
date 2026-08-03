package com.linkedin.metadata.graph.postgres;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PostgresGraphFilterSqlTest {

  @Test
  public void emptyFilterIsTrue() {
    List<Object> params = new ArrayList<>();
    String sql = PostgresGraphFilterSql.vertexFilterSql(new Filter(), "v.urn", params);
    Assert.assertEquals(sql, "TRUE");
    Assert.assertTrue(params.isEmpty());
  }

  @Test
  public void singleUrnEquals() {
    List<Object> params = new ArrayList<>();
    Criterion c =
        new Criterion()
            .setField("urn")
            .setCondition(Condition.EQUAL)
            .setValues(new StringArray(ImmutableList.of("urn:li:dataset:(a,b,c)")));
    Filter f =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(c)))));
    String sql = PostgresGraphFilterSql.vertexFilterSql(f, "vs.urn", params);
    Assert.assertEquals(sql, "((vs.urn = ?))");
    Assert.assertEquals(params.size(), 1);
  }
}
