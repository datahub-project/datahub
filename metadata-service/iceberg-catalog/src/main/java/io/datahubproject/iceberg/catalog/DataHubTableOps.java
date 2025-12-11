/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.iceberg.catalog;

import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import lombok.SneakyThrows;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

public class DataHubTableOps extends BaseMetastoreTableOperations {

  private final TableOrViewOpsDelegate<TableMetadata> delegate;

  public DataHubTableOps(
      DataHubIcebergWarehouse warehouse,
      TableIdentifier tableIdentifier,
      EntityService entityService,
      OperationContext operationContext,
      FileIOFactory fileIOFactory) {
    this.delegate =
        new TableOpsDelegate(
            warehouse, tableIdentifier, entityService, operationContext, fileIOFactory);
  }

  @Override
  public TableMetadata refresh() {
    return delegate.refresh();
  }

  @Override
  public TableMetadata current() {
    return delegate.current();
  }

  @SneakyThrows
  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    delegate.doCommit(
        base == null ? null : new MetadataWrapper<>(base),
        new MetadataWrapper<>(metadata),
        () -> writeNewMetadataIfRequired(base == null, metadata));
  }

  @Override
  protected String tableName() {
    return delegate.name();
  }

  @Override
  public FileIO io() {
    return delegate.io();
  }
}
