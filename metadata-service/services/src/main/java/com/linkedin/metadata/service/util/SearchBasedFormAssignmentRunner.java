/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.service.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.form.DynamicFormAssignment;
import io.datahubproject.metadata.context.OperationContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchBasedFormAssignmentRunner {

  public static void assign(
      OperationContext opContext,
      DynamicFormAssignment formFilters,
      Urn formUrn,
      int batchFormEntityCount,
      SystemEntityClient entityClient) {
    Runnable runnable =
        new Runnable() {
          @Override
          public void run() {
            try {
              SearchBasedFormAssignmentManager.apply(
                  opContext, formFilters, formUrn, batchFormEntityCount, entityClient);
            } catch (Exception e) {
              log.error(
                  "SearchBasedFormAssignmentRunner failed to run. "
                      + "Options: formFilters: {}, "
                      + "formUrn: {}, "
                      + "batchFormCount: {}, "
                      + "entityClient: {}, ",
                  formFilters,
                  formUrn,
                  batchFormEntityCount,
                  entityClient);
              throw new RuntimeException("Form assignment runner error.", e);
            }
          }
        };

    new Thread(runnable).start();
  }

  private SearchBasedFormAssignmentRunner() {}
}
