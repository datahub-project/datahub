package com.linkedin.metadata.dao.scsi;

import com.linkedin.common.urn.Urn;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Given an URN, extracts a map containing parts of the urn to values to index in the secondary index.
 *
 * <p>This should map all urn parts and values you may wish to query in the secondary index. Nested values can also
 * be extracted.
 *
 * <p>For example, if dataset URN is like {@code urn:li:dataset:(urn:li:platform:%PLATFORM_NAME%),%NAME%,%ORIGIN%)} then
 * an implementation that parses datset URNs may return a map like:
 *
 * <pre>
 *   /platform -> urn:li:platform:%PLATFORM_NAME%
 *   /platform/platformName -> %PLATFORM_NAME%
 *   /datasetName -> %NAME%
 *   /origin -> %ORIGIN%
 * </pre>
 *
 * @param <URN> the concrete URN type this can extract paths from
 */
public interface UrnPathExtractor<URN extends Urn> {
  @Nonnull
  Map<String, Object> extractPaths(@Nonnull URN urn);
}
