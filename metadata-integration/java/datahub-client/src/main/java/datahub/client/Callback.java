package datahub.client;

import javax.annotation.Nullable;


public interface Callback<T> {

  /**
   * Called when the client request has completed.
   * Completion does not imply success. Inspect the response object to understand if
   * this was a successfully processed request or not.
   * @param response
   */
  void onCompletion(@Nullable T response);

  /**
   * Called when the client request has thrown an exception before completion.
   * @param exception
   */
  void onFailure(Throwable exception);

}
