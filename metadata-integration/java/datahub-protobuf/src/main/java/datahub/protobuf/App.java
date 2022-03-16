package datahub.protobuf;

import com.linkedin.common.FabricType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import datahub.client.rest.RestEmitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

/**
 * Rudimentary application
 */
public class App {
    private App() {

    }

    public static void main(String[] args) throws IOException {
        Map<String, String> env = System.getenv();

        if (!env.containsKey("DATAHUB_API")) {
            System.err.println("Missing environmental variable DATAHUB_API.");
            System.exit(1);
        }

        if (!env.containsKey("DATAHUB_TOKEN")) {
            System.err.println("Missing environmental variable DATAHUB_TOKEN.");
            System.exit(1);
        }

        if (args.length != 2) {
            System.err.println("Expected 2 arguments, the protobuf descriptor filename and the schema filename.");
            System.exit(1);
        }

        String datahubUser = env.getOrDefault("DATAHUB_USER", "datahub");
        FabricType fabricType = FabricType.valueOf(env.getOrDefault("DATAHUB_ENV", "DEV"));
        String githubOrg = env.getOrDefault("DATAHUB_GITHUBORG", null);
        String slackId = env.getOrDefault("DATAHUB_SLACKID", null);

        try {
            RestEmitter emitter = RestEmitter
                    .create(b -> b.server(env.get("DATAHUB_API")).token(env.get("DATAHUB_TOKEN")));

            AuditStamp auditStamp = new AuditStamp()
                    .setTime(System.currentTimeMillis())
                    .setActor(new CorpuserUrn(datahubUser));

            InputStream protocStream = new FileInputStream(args[0]);
            String textSchema = Files.readString(Path.of(args[1]));

            ProtobufDataset dataset = ProtobufDataset.builder()
                    .setDataPlatformUrn(new DataPlatformUrn("kafka"))
                    .setProtocIn(protocStream)
                    .setFilename(args[1])
                    .setSchema(textSchema)
                    .setAuditStamp(auditStamp)
                    .setFabricType(fabricType)
                    .setGithubOrganization(githubOrg)
                    .setSlackTeamId(slackId)
                    .build();

            dataset.getAllMetadataChangeProposals().flatMap(Collection::stream)
                    .forEach(mcpw -> {
                        try {
                            emitter.emit(mcpw, null).get();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

        } catch (Exception e) {
            if (e.getMessage().equals("Cannot autodetect protobuf Message.")) {
                System.err.printf("WARN: Top-level schema not found in %s, no dataset emitted%n", args[1]);
            } else {
                e.printStackTrace();
                System.exit(1);
            }
        }

        System.exit(0);
    }
}
