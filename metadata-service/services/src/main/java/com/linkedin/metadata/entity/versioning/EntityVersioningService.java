package com.linkedin.metadata.entity.versioning;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.RollbackResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

public interface EntityVersioningService {

  /**
   * Generates a new set of VersionProperties for the latest version and links it to the specified
   * version set. If the specified version set does not yet exist, will create it. Order of
   * operations here is important: 1. Create initial Version Set if necessary, do not generate
   * Version Set Properties 2. Create Version Properties for specified entity. 3. Generate version
   * properties with the properly set latest version Will eventually want to add in the scheme here
   * as a parameter
   *
   * @return ingestResult -> the results of the ingested linked version
   */
  List<IngestResult> linkLatestVersion(
      OperationContext opContext,
      Urn versionSet,
      Urn newLatestVersion,
      VersionPropertiesInput inputProperties);

  /**
   * Unlinks the latest version from a version set. Will attempt to set up the previous version as
   * the new latest. This fully removes the version properties and unversions the specified entity.
   *
   * @param opContext operational context containing various information about the current execution
   * @param currentLatest the currently linked latest versioned entity urn
   * @return the deletion result
   */
  List<RollbackResult> unlinkVersion(OperationContext opContext, Urn versionSet, Urn currentLatest);
}
