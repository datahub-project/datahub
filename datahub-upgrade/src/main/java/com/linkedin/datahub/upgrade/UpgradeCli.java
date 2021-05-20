package com.linkedin.datahub.upgrade;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
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
  }

  private final UpgradeManager _upgradeManager = new DefaultUpgradeManager();

  @Inject
  @Named("noCodeUpgrade")
  private Upgrade _noCodeUpgrade;

  UpgradeCli() {
    _upgradeManager.register(_noCodeUpgrade);
  }

  @Override
  public void run(String... cmdLineArgs) {
    final Args args = new Args();
    new CommandLine(args).setCaseInsensitiveEnumValuesAllowed(true).parseArgs(cmdLineArgs);
    _upgradeManager.execute(args.upgradeId);
  }
}