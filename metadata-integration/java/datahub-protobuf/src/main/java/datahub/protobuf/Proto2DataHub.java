package datahub.protobuf;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import datahub.client.Emitter;
import datahub.client.file.FileEmitter;
import datahub.client.file.FileEmitterConfig;
import datahub.client.rest.RestEmitter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/** Rudimentary application */
public class Proto2DataHub {

  private static final Option OPTION_DATAHUB_PLATFORM =
      Option.builder()
          .longOpt("platform")
          .hasArg()
          .desc(
              "[Optional] The data platform to produce schemas for. e.g. kafka, snowflake, etc. (defaults to kafka)")
          .build();

  private static final Option OPTION_DATAHUB_API =
      Option.builder()
          .longOpt("datahub_api")
          .hasArg()
          .desc("[Optional] The API endpoint for DataHub GMS. (defaults to https://localhost:8080)")
          .build();

  private static final Option OPTION_DATAHUB_TOKEN =
      Option.builder()
          .longOpt("datahub_token")
          .hasArg()
          .desc("[Optional] The authentication token for DataHub API access. (defaults to empty)")
          .build();

  private static final Option OPTION_DESCRIPTOR =
      Option.builder()
          .longOpt("descriptor")
          .hasArg()
          .desc(
              "[Required] The generated protobuf descriptor file. "
                  + "Typically a single .dsc file for the repo or a .protoc file (1:1 with each src file)")
          .required()
          .build();

  private static final Option OPTION_FILE =
      Option.builder()
          .longOpt("file")
          .hasArg()
          .desc(
              "[Optional if using --directory] The protobuf source file. Typically a .proto file.")
          .build();

  private static final Option OPTION_DIR =
      Option.builder()
          .longOpt("directory")
          .hasArg()
          .desc("[Optional if using --file] The root directory containing protobuf source files.")
          .build();

  private static final Option OPTION_EXCLUDE_PATTERN =
      Option.builder()
          .longOpt("exclude")
          .valueSeparator(',')
          .hasArgs()
          .desc(
              "[Optional] Exclude patterns to avoid processing all source files, separated by ,. Typically used with --directory option. "
                  + "Follows glob patterns: e.g. --exclude \"build/**,generated/**\" will exclude all files in the build "
                  + "and generated directories under the rootDirectory given by the --directory option")
          .build();

  private static final Option OPTION_DATAHUB_USER =
      Option.builder()
          .longOpt("datahub_user")
          .hasArg()
          .desc("[Optional] The datahub user to attribute this ingestion to. (defaults to ..)")
          .build();

  private static final Option OPTION_ENV =
      Option.builder()
          .longOpt("env")
          .hasArg()
          .desc(
              "[Optional] The environment to attach all entities to. Typically, DEV, PROD etc. (defaults to DEV)")
          .build();

  private static final Option OPTION_GITHUB_ORG =
      Option.builder()
          .longOpt("github_org")
          .hasArg()
          .desc(
              "[Optional] The GitHub organization that this schema repository belongs to. "
                  + "We will translate comments in your protoc files like @datahub-project/data-team "
                  + "to GitHub team urls like: https://github.com/orgs/datahub-project/teams/data-team")
          .build();

  private static final Option OPTION_SLACK_ID =
      Option.builder()
          .longOpt("slack_id")
          .hasArg()
          .desc(
              "[Optional] The Slack team id if your protobuf files contain comments with references to channel names. "
                  + "We will translate comments like #data-eng in your protobuf file to slack urls like: "
                  + "https://slack.com/app_redirect?channel=data-eng&team=T1234 following the "
                  + "documentation at (https://api.slack.com/reference/deep-linking#deep-linking-into-your-slack-app__opening-a-channel-by-name-or-id) "
                  + "The easiest way to find your Slack team id is to open your workspace in your browser. It should look "
                  + "something like: https://app.slack.com/client/TUMKD5EGJ/...  In this case, the team-id is TUMKD5EGJ.")
          .build();

  private static final Option OPTION_TRANSPORT =
      Option.builder()
          .longOpt("transport")
          .hasArg()
          .desc(
              "[Optional] What transport to use to communicate with DataHub. Options are: rest (default), kafka and file.")
          .build();

  private static final Option OPTION_FILENAME =
      Option.builder()
          .longOpt("filename")
          .hasArg()
          .desc("[Required if using transport file] Filename to write output to.")
          .build();

  private static final Option OPTION_HELP =
      Option.builder().longOpt("help").desc("Print this help message").build();

  private static final Option OPTION_SUBTYPE =
      Option.builder()
          .longOpt("subtype")
          .desc(
              "[Optional] A custom subtype to attach to all entities produced. e.g. event, schema, topic etc."
                  + "(Default is schema)")
          .build();

  enum TransportOptions {
    REST,
    KAFKA,
    FILE
  }

  static class AppConfig {

    private final String datahubUser;
    private final FabricType fabricType;
    private final String datahubAPI;
    private final String datahubToken;
    private final String githubOrg;
    private final String slackId;
    private final String dataPlatform;
    private final String protoc;
    private final String inputFile;
    private final String inputDir;
    private final TransportOptions transport;
    private final String filename;
    private final String subType;
    private final String[] excludePatterns;

    AppConfig(CommandLine cli) {
      Map<String, String> env = System.getenv();
      datahubAPI =
          cli.getOptionValue(
              OPTION_DATAHUB_API, env.getOrDefault("DATAHUB_API", "http://localhost:8080"));
      datahubToken =
          cli.getOptionValue(OPTION_DATAHUB_TOKEN, env.getOrDefault("DATAHUB_TOKEN", ""));
      datahubUser =
          cli.getOptionValue(OPTION_DATAHUB_USER, env.getOrDefault("DATAHUB_USER", "datahub"));
      fabricType =
          FabricType.valueOf(
              cli.getOptionValue(OPTION_ENV, env.getOrDefault("DATAHUB_ENV", "DEV"))
                  .toUpperCase(Locale.ROOT));
      githubOrg =
          cli.getOptionValue(OPTION_GITHUB_ORG, env.getOrDefault("DATAHUB_GITHUBORG", null));
      slackId = cli.getOptionValue(OPTION_SLACK_ID, env.getOrDefault("DATAHUB_SLACKID", null));
      dataPlatform = cli.getOptionValue(OPTION_DATAHUB_PLATFORM, "kafka").toLowerCase(Locale.ROOT);
      protoc = cli.getOptionValue(OPTION_DESCRIPTOR);
      inputFile = cli.getOptionValue(OPTION_FILE, null);
      transport =
          TransportOptions.valueOf(
              cli.getOptionValue(OPTION_TRANSPORT, "rest").toUpperCase(Locale.ROOT));
      filename = cli.getOptionValue(OPTION_FILENAME, null);
      subType = cli.getOptionValue(OPTION_SUBTYPE, "schema").toLowerCase(Locale.ROOT);
      inputDir = cli.getOptionValue(OPTION_DIR, null);
      excludePatterns = cli.getOptionValues(OPTION_EXCLUDE_PATTERN);
    }

    private AppConfig validate() throws Exception {
      switch (transport) {
        case FILE:
          if (filename == null) {
            throw new Exception("Transport file is being used, but a filename was not provided");
          }
          break;
        default:
          // do nothing
      }
      if (this.protoc != null) {
        Path path = Path.of(this.protoc);
        if (!Files.exists(path)) {
          throw new Exception(
              String.format("Proto-descriptor file %s does not exist", this.protoc));
        }
        if (!Files.isRegularFile(path)) {
          throw new Exception(
              String.format("Proto-descriptor file %s is not a regular file", this.protoc));
        }
      }
      if ((this.inputFile == null) && (this.inputDir == null)) {
        throw new Exception("Must provide either an input file or an input directory to read from");
      }
      if (this.slackId != null) {
        if (!this.slackId.startsWith("T")) {
          throw new Exception(
              String.format(
                  "Slack team id %s should start with the letter T. "
                      + "The easiest way to find your Slack team id is to open your workspace in your browser. "
                      + "It should look something like: https://app.slack.com/client/TUMKD5EGJ/...  "
                      + "In this case, the team-id is TUMKD5EGJ.",
                  this.slackId));
        }
      }
      return this;
    }
  }

  private Proto2DataHub() {}

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options
        .addOption(OPTION_DATAHUB_PLATFORM)
        .addOption(OPTION_DATAHUB_API)
        .addOption(OPTION_DATAHUB_TOKEN)
        .addOption(OPTION_DESCRIPTOR)
        .addOption(OPTION_FILE)
        .addOption(OPTION_DIR)
        .addOption(OPTION_EXCLUDE_PATTERN)
        .addOption(OPTION_DATAHUB_USER)
        .addOption(OPTION_GITHUB_ORG)
        .addOption(OPTION_ENV)
        .addOption(OPTION_SLACK_ID)
        .addOption(OPTION_TRANSPORT)
        .addOption(OPTION_FILENAME)
        .addOption(OPTION_SUBTYPE)
        .addOption(OPTION_HELP);

    Options firstPassOptions = new Options().addOption(OPTION_HELP);

    // create the parser
    CommandLineParser parser = new DefaultParser();
    CommandLine cli = null;
    cli = parser.parse(firstPassOptions, args, true);
    if (cli.hasOption(OPTION_HELP)) {
      printUsageAndExit(options, 0);
    }

    try {
      // parse the real command line arguments
      cli = parser.parse(options, args);
    } catch (Exception exp) {
      // oops, something went wrong
      // we try old-style format before giving up
      try {
        String[] translatedArgs = convertOldStyleArgsIfPossible(args);
        if (translatedArgs != null) {
          cli = parser.parse(options, translatedArgs);
        } else {
          System.err.println("Parsing failed.  Reason: " + exp.getMessage());
          printUsageAndExit(options, 1);
        }
      } catch (Exception secondExp) {
        System.err.println("Parsing failed.  Reason: " + secondExp.getMessage());
        printUsageAndExit(options, 1);
      }
    }

    AppConfig config = new AppConfig(cli).validate();
    Emitter emitter = null;
    AtomicInteger totalEvents = new AtomicInteger();

    switch (config.transport) {
      case REST:
        {
          emitter = RestEmitter.create(b -> b.server(config.datahubAPI).token(config.datahubToken));
        }
        break;
      case KAFKA:
        {
          throw new UnsupportedOperationException("Kafka transport is not supported yet.");
        }
      case FILE:
        {
          emitter = new FileEmitter(FileEmitterConfig.builder().fileName(config.filename).build());
        }
        break;
      default:
        {
          throw new UnsupportedOperationException(
              String.format("%s transport is not supported yet.", config.transport));
        }
    }

    AuditStamp auditStamp =
        new AuditStamp()
            .setTime(System.currentTimeMillis())
            .setActor(new CorpuserUrn(config.datahubUser));

    InputStream protocStream = new FileInputStream(config.protoc);

    Stream<Path> filePathStream = Stream.empty();
    if (config.inputFile != null) {
      filePathStream = Stream.of(Path.of(config.inputFile));
    } else {
      DirectoryWalker walker = new DirectoryWalker(config.inputDir, config.excludePatterns);
      filePathStream = walker.walkFiles();
    }

    Emitter finalEmitter = emitter;
    AtomicInteger exitCode = new AtomicInteger(0);
    AtomicInteger totalFiles = new AtomicInteger(0);

    try {
      filePathStream.forEach(
          filePath -> {
            totalFiles.incrementAndGet();
            try {
              String textSchema = Files.readString(filePath);

              ProtobufDataset dataset =
                  ProtobufDataset.builder()
                      .setDataPlatformUrn(new DataPlatformUrn(config.dataPlatform))
                      .setProtocIn(new FileInputStream(config.protoc))
                      .setFilename(filePath.toString())
                      .setSchema(textSchema)
                      .setAuditStamp(auditStamp)
                      .setFabricType(config.fabricType)
                      .setGithubOrganization(config.githubOrg)
                      .setSlackTeamId(config.slackId)
                      .setSubType(config.subType)
                      .build();

              dataset
                  .getAllMetadataChangeProposals()
                  .flatMap(Collection::stream)
                  .forEach(
                      mcpw -> {
                        try {
                          finalEmitter.emit(mcpw, null).get();
                          totalEvents.getAndIncrement();
                        } catch (Exception e) {
                          throw new RuntimeException(e);
                        }
                      });
            } catch (Exception e) {
              if (e.getMessage() != null
                  && e.getMessage().equals("Cannot autodetect protobuf Message.")) {
                System.err.printf(
                    "WARN: Top-level schema not found in %s, no dataset emitted%n", args[1]);
              } else {
                e.printStackTrace();
                System.err.println(
                    String.format(
                        "‼️ Failed to emit to DataHub over %s. Num events emitted so far %d",
                        config.transport, totalEvents.get()));
                exitCode.set(1);
              }
            }
          });
    } finally {
      if (emitter != null) {
        emitter.close();
      }
    }
    if (exitCode.get() == 0) {
      System.out.println(
          String.format(
              "✅ Successfully emitted %d events for %d files to DataHub %s",
              totalEvents.get(), totalFiles.get(), config.transport));
    } else {
      System.out.println(
          String.format(
              "‼️ Emitted %d events for %d files to DataHub %s",
              totalEvents.get(), totalFiles.get(), config.transport));
    }
    System.exit(exitCode.get());
  }

  private static String[] convertOldStyleArgsIfPossible(String[] args) {
    if (args.length == 2) {
      String[] translatedArgs = {"--descriptor", args[0], "--file", args[1]};
      return translatedArgs;
    } else {
      return null;
    }
  }

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(Proto2DataHub.class.getSimpleName(), options);
    System.exit(exitCode);
  }
}
