package io.datahubproject.iceberg.catalog;

import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;

@Slf4j
public class DataHubViewOps extends BaseViewOperations {

  private final TableOrViewOpsDelegate<ViewMetadata> delegate;

  public DataHubViewOps(
      DataHubIcebergWarehouse warehouse,
      TableIdentifier tableIdentifier,
      EntityService entityService,
      OperationContext operationContext,
      FileIOFactory fileIOFactory) {
    this.delegate =
        new ViewOpsDelegate(
            warehouse, tableIdentifier, entityService, operationContext, fileIOFactory);
  }

  @Override
  public ViewMetadata refresh() {
    return delegate.refresh();
  }

  @Override
  public ViewMetadata current() {
    return delegate.current();
  }

  @Override
  protected void doRefresh() {
    throw new UnsupportedOperationException();
  }

  @SneakyThrows
  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {
    delegate.doCommit(
        base == null ? null : new MetadataWrapper<>(base),
        new MetadataWrapper<>(metadata),
        () -> writeNewMetadataIfRequired(metadata));
  }

  @Override
  protected String viewName() {
    return delegate.name();
  }

  @Override
  public FileIO io() {
    return delegate.io();
  }
}
