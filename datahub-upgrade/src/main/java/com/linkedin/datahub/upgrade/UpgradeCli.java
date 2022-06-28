package com.linkedin.datahub.upgrade;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.datahub.upgrade.nocodecleanup.NoCodeCleanupUpgrade;
import com.linkedin.datahub.upgrade.removeunknownaspects.RemoveUnknownAspects;
import com.linkedin.datahub.upgrade.restorebackup.RestoreBackup;
import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
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

  @Override
  public void run(String... cmdLineArgs) {
    _upgradeManager.register(noCodeUpgrade);
    _upgradeManager.register(noCodeCleanup);
    _upgradeManager.register(restoreIndices);
    _upgradeManager.register(restoreBackup);
    _upgradeManager.register(removeUnknownAspects);

    final Args args = new Args();
    new CommandLine(args).setCaseInsensitiveEnumValuesAllowed(true).parseArgs(cmdLineArgs);
    UpgradeResult result = _upgradeManager.execute(args.upgradeId, args.args);

    if (UpgradeResult.Result.FAILED.equals(result.result())) {
      System.exit(1);
    } else {
      System.exit(0);
    }
  }
}
