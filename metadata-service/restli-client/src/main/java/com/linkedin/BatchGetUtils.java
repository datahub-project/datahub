package com.linkedin;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.BatchGetEntityRequest;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.base.BatchGetEntityRequestBuilderBase;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public final class BatchGetUtils {
  private BatchGetUtils() {
    // not called
  }

  private static int batchSize = 25;

  public static <
          U extends Urn,
          T extends RecordTemplate,
          CRK extends ComplexResourceKey<K, EmptyRecord>,
          RB extends BatchGetEntityRequestBuilderBase<CRK, T, RB>,
          K extends RecordTemplate>
      Map<U, T> batchGet(
          @Nonnull Set<U> urns,
          Function<Void, BatchGetEntityRequestBuilderBase<CRK, T, RB>> requestBuilders,
          Function<U, CRK> getKeyFromUrn,
          Function<CRK, U> getUrnFromKey,
          Client client)
          throws RemoteInvocationException {
    AtomicInteger index = new AtomicInteger(0);

    final Collection<List<U>> entityUrnBatches =
        urns.stream()
            .collect(Collectors.groupingBy(x -> index.getAndIncrement() / batchSize))
            .values();

    final Map<U, T> response = new HashMap<>();

    for (List<U> urnsInBatch : entityUrnBatches) {
      BatchGetEntityRequest<CRK, T> batchGetRequest =
          requestBuilders
              .apply(null)
              .ids(urnsInBatch.stream().map(getKeyFromUrn).collect(Collectors.toSet()))
              .build();
      final Map<U, T> batchResponse =
          client.sendRequest(batchGetRequest).getResponseEntity().getResults().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> getUrnFromKey.apply(entry.getKey()),
                      entry -> entry.getValue().getEntity()));
      response.putAll(batchResponse);
    }

    return response;
  }
}
