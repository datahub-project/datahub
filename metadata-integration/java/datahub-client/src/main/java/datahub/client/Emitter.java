package datahub.client;

import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public interface Emitter extends Closeable {

  Future<MetadataWriteResponse> emit(MetadataChangeProposalWrapper mcpw, Callback<MetadataWriteResponse> callback) throws IOException;

  Future<MetadataWriteResponse> emit(MetadataChangeProposal mcp, Callback<MetadataWriteResponse> callback) throws IOException;

  boolean testConnection() throws IOException, ExecutionException, InterruptedException;

}
