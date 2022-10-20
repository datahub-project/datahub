package datahub.client.file;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.mxe.MetadataChangeProposal;

import datahub.client.Callback;
import datahub.client.Emitter;
import datahub.client.MetadataWriteResponse;
import datahub.event.EventFormatter;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.event.UpsertAspectRequest;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileEmitter implements Emitter {

  private final EventFormatter eventFormatter;
  private final FileEmitterConfig config;
  private final ObjectMapper objectMapper = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  private final JacksonDataTemplateCodec dataTemplateCodec = new JacksonDataTemplateCodec(objectMapper.getFactory());

  private final BufferedWriter writer;
  private final Future<MetadataWriteResponse> cachedSuccessFuture;
  private final AtomicBoolean closed;
  private boolean wroteSomething;
  private static final String INDENT_4 = "    ";

  /**
   * The default constructor
   * 
   * @param config
   */
  public FileEmitter(FileEmitterConfig config) {

    this.config = config;
    this.eventFormatter = this.config.getEventFormatter();

    DefaultPrettyPrinter pp = new DefaultPrettyPrinter()
        .withObjectIndenter(new DefaultIndenter(FileEmitter.INDENT_4, DefaultIndenter.SYS_LF))
        .withArrayIndenter(new DefaultIndenter(FileEmitter.INDENT_4, DefaultIndenter.SYS_LF));
    this.dataTemplateCodec.setPrettyPrinter(pp);

    try {
      FileWriter fw = new FileWriter(config.getFileName(), false);
      this.writer = new BufferedWriter(fw);
      this.writer.append("[");
      this.writer.newLine();
      this.closed = new AtomicBoolean(false);
    } catch (IOException e) {
      throw new RuntimeException("Error while creating file", e);
    }
    this.wroteSomething = false;
    log.debug("Emitter created successfully for " + this.config.getFileName());

    this.cachedSuccessFuture = new Future<MetadataWriteResponse>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public MetadataWriteResponse get() throws InterruptedException, ExecutionException {
        return MetadataWriteResponse.builder().success(true).responseContent("MCP witten to File").build();
      }

      @Override
      public MetadataWriteResponse get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return this.get();
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }
    };
  }

  @Override
  public void close() throws IOException {
    this.writer.newLine();
    this.writer.append("]");
    this.writer.close();
    this.closed.set(true);
    log.debug("Emitter closed for {}", this.config.getFileName());
  }

  @Override
  public Future<MetadataWriteResponse> emit(@SuppressWarnings("rawtypes") MetadataChangeProposalWrapper mcpw,
      Callback callback) throws IOException {
    return emit(this.eventFormatter.convert(mcpw), callback);
  }

  @Override
  public Future<MetadataWriteResponse> emit(MetadataChangeProposal mcp, Callback callback) throws IOException {
    if (this.closed.get()) {
      String errorMsg = "File Emitter is already closed.";
      log.error(errorMsg);
      Future<MetadataWriteResponse> response = createFailureFuture(errorMsg);
      if (callback != null) {
        callback.onFailure(new Exception(errorMsg));
      }
      return response;
    }
    try {
      String serializedMCP = this.dataTemplateCodec.mapToString(mcp.data());
      if (wroteSomething) {
        this.writer.append(",");
        this.writer.newLine();
      }
      this.writer.append(serializedMCP);
      wroteSomething = true;
      log.debug("MCP written successfully: {}", serializedMCP);
      Future<MetadataWriteResponse> response = this.cachedSuccessFuture;
      if (callback != null) {
        try {
          callback.onCompletion(response.get());
        } catch (InterruptedException | ExecutionException e) {
          log.warn("Callback could not be executed.", e);
        }
      }
      return response;
    } catch (Throwable t) {
      Future<MetadataWriteResponse> response = createFailureFuture(t.getMessage());
      if (callback != null) {
        try {
          callback.onFailure(t);
        } catch (Exception e) {
          log.warn("Callback could not be executed.", e);
        }
      }
      return response;
    }
  }

  @Override
  public boolean testConnection() throws IOException, ExecutionException, InterruptedException {
    throw new UnsupportedOperationException("testConnection not relevant for File Emitter");
  }

  @Override
  public Future<MetadataWriteResponse> emit(List<UpsertAspectRequest> request, Callback callback) throws IOException {
    throw new UnsupportedOperationException("UpsertAspectRequest not relevant for File Emitter");
  }

  private Future<MetadataWriteResponse> createFailureFuture(String message) {
    return new Future<MetadataWriteResponse>() {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public MetadataWriteResponse get() throws InterruptedException, ExecutionException {
        return MetadataWriteResponse.builder().success(false).responseContent(message).build();
      }

      @Override
      public MetadataWriteResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
          TimeoutException {
        return this.get();
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

    };
  }

}
