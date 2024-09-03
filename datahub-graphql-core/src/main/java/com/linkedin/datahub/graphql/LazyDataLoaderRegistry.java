package com.linkedin.datahub.graphql;

import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class LazyDataLoaderManager {
    private final Map<String, Function<QueryContext, DataLoader<?, ?>>> dataLoaderSuppliers = new ConcurrentHashMap<>();
    private final DataLoaderRegistry registry;

    public LazyDataLoaderManager(Map<String, Function<QueryContext, DataLoader<?, ?>>> dataLoaderSuppliers) {
        this.dataLoaderSuppliers.putAll(dataLoaderSuppliers);
        this.registry = registry;
    }

    public <K, V> DataLoader<K, V> getDataLoader(String key) {
        if (!registry.getKeys().contains(key)) {
            Function<QueryContext, DataLoader<?, ?>> supplier = dataLoaderSuppliers.get(key);
            if (supplier == null) {
                throw new IllegalArgumentException("No DataLoader registered for key: " + key);
            }
            registry.register(key, supplier.get());
        }
        return registry.getDataLoader(key);
    }

    public void clearAll() {
        registry.getKeys().forEach(registry::unregister);
    }
}
