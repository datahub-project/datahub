package com.linkedin.datahub.upgrade;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.datahub.upgrade.nocodecleanup.NoCodeCleanupUpgrade;
import com.linkedin.datahub.upgrade.removeunknownaspects.RemoveUnknownAspects;
import com.linkedin.datahub.upgrade.restorebackup.RestoreBackup;
import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.datahub.upgrade.system.SystemUpdate;
import com.linkedin.datahub.upgrade.system.SystemUpdateBlocking;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

@Slf4j
@Component
public class UpgradeCli implements CommandLineRunner {

  private static final class Args {
    @CommandLine.Option(names = {"-u", "--upgrade-id"})
    String upgradeId;

    @CommandLine.Option(names = {"-a", "--arg"})
    List<String> args;
  }

  private final UpgradeManager _upgradeManager = new DefaultUpgradeManager();

  @Inject
  @Named("noCodeUpgrade")
  private NoCodeUpgrade noCodeUpgrade;

  @Inject
  @Named("noCodeCleanup")
  private NoCodeCleanupUpgrade noCodeCleanup;

  @Inject
  @Named("restoreIndices")
  private RestoreIndices restoreIndices;

  @Inject
  @Named("restoreBackup")
  private RestoreBackup restoreBackup;

  @Inject
  @Named("removeUnknownAspects")
  private RemoveUnknownAspects removeUnknownAspects;

  @Autowired(required = false)
  @Named("systemUpdate")
  private SystemUpdate systemUpdate;

  @Autowired(required = false)
  @Named("systemUpdateBlocking")
  private SystemUpdateBlocking systemUpdateBlocking;

  @Autowired(required = false)
  @Named("systemUpdateNonBlocking")
  private SystemUpdateNonBlocking systemUpdateNonBlocking;

  @Autowired
  @Named("systemOperationContext")
  private OperationContext systemOperationContext;

  @Override
  public void run(String... cmdLineArgs) {
    _upgradeManager.register(noCodeUpgrade);
    _upgradeManager.register(noCodeCleanup);
    _upgradeManager.register(restoreIndices);
    _upgradeManager.register(restoreBackup);
    _upgradeManager.register(removeUnknownAspects);
    if (systemUpdate != null) {
      _upgradeManager.register(systemUpdate);
    }
    if (systemUpdateBlocking != null) {
      _upgradeManager.register(systemUpdateBlocking);
    }
    if (systemUpdateNonBlocking != null) {
      _upgradeManager.register(systemUpdateNonBlocking);
    }

    final Args args = new Args();
    new CommandLine(args).setCaseInsensitiveEnumValuesAllowed(true).parseArgs(cmdLineArgs);
    UpgradeResult result =
        _upgradeManager.execute(systemOperationContext, args.upgradeId.trim(), args.args);

    if (DataHubUpgradeState.FAILED.equals(result.result())) {
      System.exit(1);
    } else {
      System.exit(0);
    }
  }
}
