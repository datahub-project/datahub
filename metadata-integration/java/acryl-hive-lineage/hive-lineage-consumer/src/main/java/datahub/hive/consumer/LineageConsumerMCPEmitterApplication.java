package datahub.hive.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main application class for the Lineage Consumer service.
 * This service consumes Hive lineage data from Kafka and emits Metadata Change Proposals (MCPs) to DataHub.
 */
@SpringBootApplication
public class LineageConsumerMCPEmitterApplication {

    public static void main(String[] args) {
        SpringApplication.run(LineageConsumerMCPEmitterApplication.class, args);
    }
}
