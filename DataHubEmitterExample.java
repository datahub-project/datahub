
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetPropertiesAspect;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.event.MetadataChangeProposalWrapper;
import io.datahubproject.metadata.client.JavaEmitterClient;

public class DataHubEmitterExample {
    
        public static void main(String[] args) throws Exception {
            // Set DataHub REST endpoint
            String datahubHost = "http://localhost:8080"; // your DataHub instance
            String token = "your-access-token"; // optional authentication
    
            // Initialize the DataHub client
            JavaEmitterClient client = new JavaEmitterClient(datahubHost, token);
    
            // Define a dataset URN (Uniform Resource Name)
            DatasetUrn datasetUrn = new DatasetUrn("mysql", "sampledb.users", "PROD");
    
            // Create metadata (e.g., description, owner, tags)
            DatasetPropertiesAspect datasetProps = new DatasetPropertiesAspect();
            datasetProps.setDescription("User information table for analytics.");
            datasetProps.setCustomProperties(Map.of(
                "domain", "analytics",
                "retention", "90 days"
            ));
    
            // Wrap metadata in a change proposal
            MetadataChangeProposalWrapper proposal = MetadataChangeProposalWrapper.builder()
                    .entityUrn(datasetUrn)
                    .aspect(datasetProps)
                    .build();
    
            // Emit metadata to the DataHub server
            client.emit(proposal);
    
            // Close connection
            client.close();
            System.out.println("Metadata successfully emitted to DataHub!");
        }
    }
import java.util.Map;
