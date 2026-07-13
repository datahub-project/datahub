package com.linkedin.datahub.graphql.types.metric.mappers;

import com.linkedin.datahub.graphql.generated.Dialect;
import com.linkedin.datahub.graphql.generated.DialectExpression;
import com.linkedin.datahub.graphql.generated.MetricExpression;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps {@link com.linkedin.metric.MetricExpression} Pegasus records to the generated GraphQL {@link
 * MetricExpression}.
 */
@Slf4j
public class MetricExpressionMapper {

  private MetricExpressionMapper() {}

  @Nullable
  public static MetricExpression map(@Nullable com.linkedin.metric.MetricExpression pdl) {
    if (pdl == null) {
      return null;
    }

    List<DialectExpression> dialects;
    if (pdl.hasDialects() && pdl.getDialects() != null) {
      dialects =
          pdl.getDialects().stream()
              .map(MetricExpressionMapper::mapDialectExpression)
              .collect(Collectors.toList());
    } else {
      dialects = Collections.emptyList();
    }

    MetricExpression result = new MetricExpression();
    result.setDialects(dialects);
    return result;
  }

  @Nonnull
  private static DialectExpression mapDialectExpression(
      @Nonnull com.linkedin.metric.DialectExpression pdl) {
    DialectExpression result = new DialectExpression();
    result.setExpression(pdl.getExpression());

    Dialect dialect;
    try {
      dialect = Dialect.valueOf(pdl.getDialect().name());
    } catch (IllegalArgumentException e) {
      log.warn(
          "MetricExpressionMapper: unknown Dialect value '{}', falling back to OTHER",
          pdl.getDialect().name());
      dialect = Dialect.OTHER;
    }
    result.setDialect(dialect);

    return result;
  }
}
