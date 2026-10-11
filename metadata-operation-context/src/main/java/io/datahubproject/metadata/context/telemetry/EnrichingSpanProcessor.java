package io.datahubproject.metadata.context.telemetry;

import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EnrichingSpanProcessor implements SpanProcessor {
  private static final ContextKey<OperationContext> OPERATION_CONTEXT_KEY =
      ContextKey.named("datahub-operation-context");
  private final List<SpanContextEnricher> enrichers;

  public EnrichingSpanProcessor(@Nonnull List<SpanContextEnricher> enrichers) {
    this.enrichers = enrichers;
  }

  @Nonnull
  public static Scope attach(@Nonnull OperationContext operationContext) {
    return Context.current().with(OPERATION_CONTEXT_KEY, operationContext).makeCurrent();
  }

  @Override
  public void onStart(@Nonnull Context parentContext, @Nonnull ReadWriteSpan span) {
    OperationContext operationContext = parentContext.get(OPERATION_CONTEXT_KEY);
    if (operationContext == null) {
      return;
    }
    for (SpanContextEnricher enricher : enrichers) {
      try {
        enricher.enrich(span, operationContext);
      } catch (Exception e) {
        log.error(
            "SpanContextEnricher {} failed; continuing chain",
            enricher.getClass().getSimpleName(),
            e);
      }
    }
  }

  @Override
  public boolean isStartRequired() {
    return true;
  }

  @Override
  public void onEnd(@Nonnull ReadableSpan span) {}

  @Override
  public boolean isEndRequired() {
    return false;
  }
}
