package com.linkedin.metadata.kafka.listener;

import com.linkedin.metadata.utils.HookExecutionContext;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Supplier;

/**
 * Wraps a client interface so that every call made while a hook is executing (per {@link
 * HookExecutionContext}) increments {@link MetricUtils#DATAHUB_HOOK_EXTERNAL_READS}, tagged by hook
 * and client type. Calls made outside a hook invocation are not counted, which is what keeps the
 * count correct when the consumer runs embedded in GMS.
 *
 * <p>This is the mechanism behind RFC-0's per-hook external-read signal; wiring the wrapped beans
 * in is gated by configuration so it stays dark until enabled.
 */
public final class InstrumentedClientProxy {

  public static final String CLIENT_TAG = "client";

  private InstrumentedClientProxy() {}

  /**
   * @param metricUtilsSupplier resolves the {@link MetricUtils} at call time (may return null);
   *     supplied lazily so this can wrap beans created before the metrics context is ready.
   */
  @SuppressWarnings("unchecked")
  public static <T> T wrap(Class<T> iface, T delegate, Supplier<MetricUtils> metricUtilsSupplier) {
    return (T)
        Proxy.newProxyInstance(
            iface.getClassLoader(),
            new Class<?>[] {iface},
            new CountingHandler(iface.getSimpleName(), delegate, metricUtilsSupplier));
  }

  private static final class CountingHandler implements InvocationHandler {
    private final String clientName;
    private final Object delegate;
    private final Supplier<MetricUtils> metricUtilsSupplier;

    CountingHandler(String clientName, Object delegate, Supplier<MetricUtils> metricUtilsSupplier) {
      this.clientName = clientName;
      this.delegate = delegate;
      this.metricUtilsSupplier = metricUtilsSupplier;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      // Don't count Object methods (toString/hashCode/equals) — they are not external reads.
      if (method.getDeclaringClass() != Object.class) {
        String hook = HookExecutionContext.getCurrentOrNull();
        if (hook != null) {
          MetricUtils metricUtils = metricUtilsSupplier.get();
          if (metricUtils != null) {
            metricUtils.incrementMicrometer(
                MetricUtils.DATAHUB_HOOK_EXTERNAL_READS,
                1,
                MetricUtils.HOOK_TAG,
                hook,
                CLIENT_TAG,
                clientName);
          }
        }
      }
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}
