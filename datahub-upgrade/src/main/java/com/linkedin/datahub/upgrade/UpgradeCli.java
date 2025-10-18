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
      log.warn("RestoreIndices upgrade not available - bean not found");
    }

    if (restoreBackup != null) {
      _upgradeManager.register(restoreBackup);
    } else {
      log.warn("RestoreBackup upgrade not available - bean not found");
    }

    if (removeUnknownAspects != null) {
      _upgradeManager.register(removeUnknownAspects);
    } else {
      log.warn("RemoveUnknownAspects upgrade not available - bean not found");
    }

    if (sqlSetup != null) {
      _upgradeManager.register(sqlSetup);
    } else {
      log.warn("SqlSetup upgrade not available - bean not found");
    }

    if (loadIndices != null) {
      _upgradeManager.register(loadIndices);
    } else {
      log.warn("LoadIndices upgrade not available - bean not found");
    }

    if (systemUpdate != null) {
      _upgradeManager.register(systemUpdate);
    } else {
      log.warn("SystemUpdate upgrade not available - bean not found");
    }

    if (systemUpdateBlocking != null) {
      _upgradeManager.register(systemUpdateBlocking);
    } else {
      log.warn("SystemUpdateBlocking upgrade not available - bean not found");
    }

    if (systemUpdateNonBlocking != null) {
      _upgradeManager.register(systemUpdateNonBlocking);
    } else {
      log.warn("SystemUpdateNonBlocking upgrade not available - bean not found");
    }

    if (systemUpdateCron != null) {
      _upgradeManager.register(systemUpdateCron);
    } else {
      log.warn("SystemUpdateCron upgrade not available - bean not found");
    }

    if (reindexDebug != null) {
      _upgradeManager.register(reindexDebug);
    } else {
      log.warn("ReindexDebug upgrade not available - bean not found");
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
