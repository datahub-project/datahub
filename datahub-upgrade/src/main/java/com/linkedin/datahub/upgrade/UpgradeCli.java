package com.linkedin.datahub.upgrade;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.datahub.upgrade.loadindices.LoadIndices;
import com.linkedin.datahub.upgrade.removeunknownaspects.RemoveUnknownAspects;
import com.linkedin.datahub.upgrade.restorebackup.RestoreBackup;
import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetup;
import com.linkedin.datahub.upgrade.system.SystemUpdate;
import com.linkedin.datahub.upgrade.system.SystemUpdateBlocking;
import com.linkedin.datahub.upgrade.system.SystemUpdateNonBlocking;
import com.linkedin.datahub.upgrade.system.cron.SystemUpdateCron;
import com.linkedin.datahub.upgrade.system.elasticsearch.ReindexDebug;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
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

    @CommandLine.Option(
        names = {"-n", "--nonblocking-classname"},
        description =
            "Run only the specified non-blocking upgrade(s). Supports comma-delimited list "
                + "(e.g., BackfillBrowsePathsV2,BackfillPolicyFields). Only applicable with SystemUpdateNonBlocking.")
    String nonBlockingClassnames;
  }

  private final UpgradeManager _upgradeManager = new DefaultUpgradeManager();

  @Autowired(required = false)
  @Named("sqlSetup")
  private SqlSetup sqlSetup;

  @Autowired(required = false)
  @Named("loadIndices")
  private LoadIndices loadIndices;

  @Autowired(required = false)
  @Named("restoreIndices")
  private RestoreIndices restoreIndices;

  @Autowired(required = false)
  @Named("restoreBackup")
  private RestoreBackup restoreBackup;

  @Autowired(required = false)
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

  @Autowired(required = false)
  @Named("systemUpdateCron")
  private SystemUpdateCron systemUpdateCron;

  @Autowired(required = false)
  @Named("reindexDebug")
  private ReindexDebug reindexDebug;

  @Override
  public void run(String... cmdLineArgs) {
    // Register upgrades with null checks and warnings
    if (restoreIndices != null) {
      _upgradeManager.register(restoreIndices);
    } else {
      log.info("RestoreIndices upgrade not available - bean not found");
    }

    if (restoreBackup != null) {
      _upgradeManager.register(restoreBackup);
    } else {
      log.info("RestoreBackup upgrade not available - bean not found");
    }

    if (removeUnknownAspects != null) {
      _upgradeManager.register(removeUnknownAspects);
    } else {
      log.info("RemoveUnknownAspects upgrade not available - bean not found");
    }

    if (sqlSetup != null) {
      _upgradeManager.register(sqlSetup);
    } else {
      log.info("SqlSetup upgrade not available - bean not found");
    }

    if (loadIndices != null) {
      _upgradeManager.register(loadIndices);
    } else {
      log.info("LoadIndices upgrade not available - bean not found");
    }

    if (systemUpdate != null) {
      _upgradeManager.register(systemUpdate);
    } else {
      log.info("SystemUpdate upgrade not available - bean not found");
    }

    if (systemUpdateBlocking != null) {
      _upgradeManager.register(systemUpdateBlocking);
    } else {
      log.info("SystemUpdateBlocking upgrade not available - bean not found");
    }

    if (systemUpdateNonBlocking != null) {
      _upgradeManager.register(systemUpdateNonBlocking);
    } else {
      log.info("SystemUpdateNonBlocking upgrade not available - bean not found");
    }

    if (systemUpdateCron != null) {
      _upgradeManager.register(systemUpdateCron);
    } else {
      log.info("SystemUpdateCron upgrade not available - bean not found");
    }

    if (reindexDebug != null) {
      _upgradeManager.register(reindexDebug);
    } else {
      log.info("ReindexDebug upgrade not available - bean not found");
    }

    final Args args = new Args();
    new CommandLine(args).setCaseInsensitiveEnumValuesAllowed(true).parseArgs(cmdLineArgs);

    List<String> execArgs =
        args.args != null ? new java.util.ArrayList<>(args.args) : new java.util.ArrayList<>();
    if (args.nonBlockingClassnames != null && !args.nonBlockingClassnames.isBlank()) {
      execArgs.add("nonBlockingClassnames=" + args.nonBlockingClassnames.trim());
    }

    UpgradeResult result =
        _upgradeManager.execute(systemOperationContext, args.upgradeId.trim(), execArgs);

    if (DataHubUpgradeState.FAILED.equals(result.result())) {
      System.exit(1);
    } else {
      System.exit(0);
    }
  }
}
