/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package datahub.client;

import javax.annotation.Nullable;

public interface Callback {

  /**
   * Called when the client request has completed. Completion does not imply success. Inspect the
   * response object to understand if this was a successfully processed request or not.
   *
   * @param response
   */
  void onCompletion(@Nullable MetadataWriteResponse response);

  /**
   * Called when the client request has thrown an exception before completion.
   *
   * @param exception
   */
  void onFailure(Throwable exception);
}
