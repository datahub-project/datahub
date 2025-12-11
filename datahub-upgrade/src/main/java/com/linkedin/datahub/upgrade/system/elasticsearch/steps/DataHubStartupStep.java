/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.mxe.DataHubUpgradeHistoryEvent;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class DataHubStartupStep implements UpgradeStep {
  private final KafkaEventProducer _kafkaEventProducer;
  private final String _version;

  @Override
  public String id() {
    return "DataHubStartupStep";
  }

  @Override
  public int retryCount() {
    return 3;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        DataHubUpgradeHistoryEvent dataHubUpgradeHistoryEvent =
            new DataHubUpgradeHistoryEvent().setVersion(_version);
        _kafkaEventProducer.produceDataHubUpgradeHistoryEvent(dataHubUpgradeHistoryEvent);
        log.info("System Update finished for version: {}", _version);
      } catch (Exception e) {
        log.error("DataHubStartupStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }
}
