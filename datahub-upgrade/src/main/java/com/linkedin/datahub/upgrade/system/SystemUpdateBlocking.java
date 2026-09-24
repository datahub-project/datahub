package com.linkedin.datahub.upgrade.system;

import com.linkedin.datahub.upgrade.system.bootstrapmcps.BootstrapMCP;
import java.util.List;
import lombok.NonNull;

public class SystemUpdateBlocking extends SystemUpdate {

  public SystemUpdateBlocking(
      @NonNull List<BlockingSystemUpgrade> blockingSystemUpgrades,
      @NonNull final BootstrapMCP bootstrapMCPBlocking) {
    super(blockingSystemUpgrades, List.of(), bootstrapMCPBlocking, null);
  }
}
